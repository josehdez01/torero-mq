import { randomUUID } from 'node:crypto';
import { createServer, type Socket } from 'node:net';
import { QueueEvents } from 'bullmq';
import { afterEach, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
    ClosedError,
    createQueueClient,
    createWorkers,
    job,
    type PublishOptions,
    type WireInput,
} from '../../src/index.js';

const resources: { close(): Promise<unknown> }[] = [];
const logger = { error() {}, warn() {}, info() {} };
const connection = { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' };
const settings = () => ({ connection, logger, prefix: `torero-adversarial-${randomUUID()}` });
function track<T extends { close(): Promise<unknown> }>(resource: T): T {
    resources.push(resource);
    return resource;
}
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
afterEach(async () => {
    vi.restoreAllMocks();
    for (const resource of resources.splice(0).reverse()) await resource.close();
});

it('closes pending observers and supports explicit retry after repairing a failed job dependency', async () => {
    let repaired = false;
    const task = job('manual-retry').handler(() => {
        if (!repaired) throw new Error('Dependency unavailable');
        return 'recovered';
    });
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const pending = await client.jobs.task.publish();
    const observer = pending.wait();
    const stopped = expect(observer).rejects.toBeInstanceOf(ClosedError);
    await client.close();
    await stopped;

    const restarted = track(createQueueClient({ task }, config));
    const handle = restarted.jobs.task.get(pending.id);
    const workers = track(createWorkers({ task }, config));
    await workers.start();
    await expect(handle.wait()).rejects.toThrow('Dependency unavailable');
    expect(await handle.status()).toBe('failed');
    repaired = true;
    await handle.retry();
    expect(await handle.wait()).toBe('recovered');
    expect(await handle.status()).toBe('completed');
    expect(await handle.remove()).toBe('removed');
});

it('snapshots bulk payloads and nested options before asynchronous schema validation', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
        release = resolve;
    });
    const task = job('snapshot').input(
        z.object({ value: z.number() }).refine(async () => {
            await gate;
            return true;
        }),
    );
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const input = { value: 1 };
    const options: PublishOptions = {
        attempts: 2,
        backoff: { type: 'fixed', delay: 50 },
        removeOnComplete: { count: 10 },
    };
    const published = client.jobs.task.publishBulk([{ input, options }]);
    input.value = 99;
    options.attempts = 8;
    if (typeof options.backoff === 'object') options.backoff.delay = 999;
    if (typeof options.removeOnComplete === 'object') options.removeOnComplete.count = 999;
    release();
    const [handle] = await published;
    if (!handle) throw new Error('Missing bulk result');
    const raw = await client.jobs.task.raw();
    await expect(handle.wait({ timeoutMs: 2_147_483_648 })).rejects.toThrow('at most');
    expect((await raw.getJob(handle.id))?.opts).toMatchObject({
        attempts: 2,
        backoff: { delay: 50 },
        removeOnComplete: { count: 10 },
    });
    const workers = track(
        createWorkers({ task: task.handler(({ input }) => input.value) }, config),
    );
    await workers.start();
    expect(await handle.wait()).toBe(1);
    await expect(
        client.jobs.task.publishBulk([
            { input: { value: 2 } },
            { input: { value: 'bad' as unknown as number } },
        ]),
    ).rejects.toThrow('validation');
    expect((await client.jobs.task.counts()).completed).toBe(1);
});

it('rejects mismatched revisions, invalid decoded input, and lossy progress permanently', async () => {
    let calls = 0;
    const task = job('protocol', { version: 2 }).input(z.number()).options({ attempts: 3 });
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const workers = track(
        createWorkers(
            {
                task: task.handler(async ({ job: current }) => {
                    calls++;
                    await current.updateProgress({ percent: 10 });
                    await current.updateProgress(new Date() as never);
                }),
            },
            config,
        ),
    );
    const raw = await client.jobs.task.raw();
    const valid = await client.jobs.task.publish(1);
    const original = await raw.getJob(valid.id);
    if (!original) throw new Error('Missing job');
    const revision = await raw.add('protocol', { ...original.data, version: 1 }, { attempts: 3 });
    const invalid = await raw.add(
        'protocol',
        {
            ...original.data,
            input: JSON.stringify({ version: 1, payload: { json: 'bad' } }),
        } satisfies WireInput,
        { attempts: 3 },
    );
    await workers.start();
    await expect(valid.wait()).rejects.toMatchObject({ details: { code: 'CODEC_ERROR' } });
    await expect(client.jobs.task.get(revision.id!).wait()).rejects.toMatchObject({
        details: { code: 'PROTOCOL_ERROR' },
    });
    await expect(client.jobs.task.get(invalid.id!).wait()).rejects.toMatchObject({
        details: { code: 'VALIDATION_ERROR' },
    });
    expect(calls).toBe(1);
    for (const id of [valid.id, revision.id!, invalid.id!])
        expect((await raw.getJob(id))?.attemptsMade).toBe(1);
    expect((await raw.getJob(valid.id))?.progress).toEqual({ percent: 10 });
});

it('does not lose immediate unretained scheduler results before the event reader starts', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
        release = resolve;
    });
    const originalRun = QueueEvents.prototype.run;
    vi.spyOn(QueueEvents.prototype, 'run').mockImplementation(async function (this: QueueEvents) {
        await gate;
        return originalRun.call(this);
    });
    const task = job('reader-race').handler(() => 42);
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const workers = track(createWorkers({ task }, config));
    await workers.start();
    const stream = await client.jobs.task.schedules.get('immediate').subscribe();
    try {
        await client.jobs.task.schedules.upsert(
            'immediate',
            { everyMs: 60_000, limit: 1 },
            undefined,
            { removeOnComplete: true },
        );
        const raw = await client.jobs.task.raw();
        const deadline = Date.now() + 2_000;
        while (
            (await raw.getJobCountByTypes('waiting', 'active', 'delayed')) > 0 &&
            Date.now() < deadline
        )
            await sleep(10);
        expect(await raw.getJobCountByTypes('waiting', 'active', 'delayed', 'completed')).toBe(0);
        release();
        expect(await stream.next()).toMatchObject({
            done: false,
            value: { type: 'completed', result: 42 },
        });
    } finally {
        release();
        stream.close();
    }
});

it('closes a TCP-connected Redis handshake that never responds', async () => {
    const sockets = new Set<Socket>();
    const server = createServer((socket) => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing port');
    const task = job('blackhole').handler(() => 1);
    const config = {
        ...settings(),
        connection: { host: '127.0.0.1', port: address.port, retryStrategy: () => null },
        startupTimeoutMs: 75,
    };
    const client = track(createQueueClient({ task }, config));
    const workers = track(createWorkers({ task }, config));
    try {
        const outcomes = await Promise.race([
            Promise.allSettled([client.start(), workers.start()]),
            sleep(1_500).then(() => {
                throw new Error('Startup cleanup hung');
            }),
        ]);
        expect(outcomes.map((result) => result.status)).toEqual(['rejected', 'rejected']);
        await Promise.all([client.close(), workers.close()]);
    } finally {
        for (const socket of sockets) socket.destroy();
        await new Promise<void>((resolve, reject) =>
            server.close((error) => (error ? reject(error) : resolve())),
        );
    }
});
