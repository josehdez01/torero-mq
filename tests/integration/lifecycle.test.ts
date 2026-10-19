import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { Redis } from 'ioredis';
import { z } from 'zod';
import {
    ClosedError,
    createQueueClient,
    createWorkers,
    job,
    StreamOverflowError,
} from '../../src/index.js';

const connection = { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' };
const logger = { error() {}, warn() {}, info() {} };
const resources: { close(): Promise<unknown> }[] = [];
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
function track<T extends { close(): Promise<unknown> }>(resource: T): T {
    resources.push(resource);
    return resource;
}
async function bounded<T>(work: Promise<T>, ms = 2_000): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            work,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(`Operation exceeded ${ms}ms`)), ms);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}
async function eventually(assertion: () => Promise<void>, timeoutMs = 4_000) {
    const end = Date.now() + timeoutMs;
    let failure: unknown;
    do {
        try {
            await assertion();
            return;
        } catch (error) {
            failure = error;
        }
        await sleep(20);
    } while (Date.now() < end);
    throw failure;
}
afterEach(async () => {
    await bounded(
        Promise.all(
            resources
                .splice(0)
                .reverse()
                .map((resource) => resource.close()),
        ),
    );
});

function fixture() {
    const task = job('lifecycle').input(z.string()).output(z.string());
    const settings = { connection, prefix: `torero-lifecycle-${randomUUID()}`, logger };
    const client = track(createQueueClient({ task }, settings));
    const workers = track(createWorkers({ task: task.handler(({ input }) => input) }, settings));
    return { task, settings, client, workers };
}

describe('resource lifecycle', () => {
    it('shares concurrent startup and closes idempotently', async () => {
        const { client, workers } = fixture();
        const clientStart = client.start();
        const workerStart = workers.start();
        expect(client.start()).toBe(clientStart);
        expect(workers.start()).toBe(workerStart);
        await Promise.all([clientStart, workerStart]);
        expect(await (await client.jobs.task.publish('one runtime')).wait()).toBe('one runtime');
        const clientClose = client.close();
        const workerClose = workers.close();
        expect(client.close()).toBe(clientClose);
        expect(workers.close()).toBe(workerClose);
        await Promise.all([clientClose, workerClose]);
        await expect(client.start()).rejects.toBeInstanceOf(ClosedError);
        await expect(workers.start()).rejects.toBeInstanceOf(ClosedError);
        await expect(client.jobs.task.publish('closed')).rejects.toBeInstanceOf(ClosedError);
    });
    it('settles close initiated during startup', async () => {
        const { client, workers } = fixture();
        const starts = [client.start(), workers.start()];
        await bounded(Promise.allSettled([...starts, client.close(), workers.close()]));
        await expect(client.jobs.task.counts()).rejects.toBeInstanceOf(ClosedError);
    });
    it('leaves caller supplied Redis connections alive', async () => {
        const redis = new Redis(connection.url, { maxRetriesPerRequest: null });
        try {
            await redis.ping();
            const task = job('borrowed').handler(() => 'done');
            const settings = {
                connection: redis,
                prefix: `torero-borrowed-${randomUUID()}`,
                logger,
            };
            const client = track(createQueueClient({ task }, settings));
            const workers = track(createWorkers({ task }, settings));
            await workers.start();
            expect(await (await client.jobs.task.publish()).wait()).toBe('done');
            await Promise.all([client.close(), workers.close()]);
            expect(await redis.ping()).toBe('PONG');
        } finally {
            await redis.quit();
        }
    });
    it('bounds unavailable Redis startup and releases connecting resources', async () => {
        const server = createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') throw new Error('Missing test port');
        await new Promise<void>((resolve, reject) =>
            server.close((error) => (error ? reject(error) : resolve())),
        );
        const baseline = process
            .getActiveResourcesInfo()
            .filter((name) => name === 'TCPSocketWrap').length;
        const task = job('unavailable').handler(() => 1);
        const settings = {
            connection: {
                host: '127.0.0.1',
                port: address.port,
                connectTimeout: 50,
                retryStrategy: () => 10,
            },
            prefix: `torero-unavailable-${randomUUID()}`,
            logger,
            startupTimeoutMs: 100,
        };
        const client = track(createQueueClient({ task }, settings));
        const workers = track(createWorkers({ task }, settings));
        const started = Date.now();
        const results = await bounded(Promise.allSettled([client.start(), workers.start()]));
        expect(results.map((result) => result.status)).toEqual(['rejected', 'rejected']);
        expect(Date.now() - started).toBeLessThan(2_000);
        await bounded(Promise.all([client.close(), workers.close()]));
        await eventually(async () => {
            expect(
                process.getActiveResourcesInfo().filter((name) => name === 'TCPSocketWrap').length,
            ).toBeLessThanOrEqual(baseline);
        });
    });
});

describe('scheduler subscriptions', () => {
    it('isolates two schedulers and ordinary jobs while delivering unretained results', async () => {
        const { client, workers } = fixture();
        const one = await client.jobs.task.schedules.get('one').subscribe();
        const two = await client.jobs.task.schedules.get('two').subscribe();
        try {
            await client.jobs.task.schedules.upsert('one', { everyMs: 60_000, limit: 1 }, 'first', {
                removeOnComplete: true,
            });
            await client.jobs.task.schedules.upsert(
                'two',
                { everyMs: 60_000, limit: 1 },
                'second',
                { removeOnComplete: true },
            );
            const ordinary = await client.jobs.task.publish('ordinary');
            await workers.start();
            const [first, second] = await bounded(Promise.all([one.next(), two.next()]));
            expect(first).toMatchObject({
                done: false,
                value: { type: 'completed', result: 'first' },
            });
            expect(second).toMatchObject({
                done: false,
                value: { type: 'completed', result: 'second' },
            });
            expect(await ordinary.wait()).toBe('ordinary');
            if (first.done || second.done) throw new Error('Missing scheduler event');
            expect(await client.jobs.task.get(first.value.jobId).status()).toBe('unknown');
            expect(await client.jobs.task.get(second.value.jobId).status()).toBe('unknown');
        } finally {
            one.close();
            two.close();
        }
    });
    it('closes a stream on for-await break and aborts pending reads', async () => {
        const { client, workers } = fixture();
        const stream = await client.jobs.task.schedules.get('break').subscribe();
        await client.jobs.task.schedules.upsert('break', { everyMs: 60_000, limit: 1 }, 'break');
        await workers.start();
        await bounded(
            (async () => {
                for await (const event of stream) {
                    expect(event.type).toBe('completed');
                    break;
                }
            })(),
        );
        expect(await stream.next()).toEqual({ done: true, value: undefined });
        const controller = new AbortController();
        const aborted = await client.jobs.task.schedules
            .get('abort')
            .subscribe({ signal: controller.signal });
        const pending = aborted.next();
        const reason = new Error('stop subscription');
        controller.abort(reason);
        await expect(pending).rejects.toBe(reason);
        await expect(aborted.next()).rejects.toBe(reason);
    });
    it('fails bounded buffers instead of silently dropping future results', async () => {
        const task = job('overflow').handler(() => 'result');
        let finished = 0;
        const settings = { connection, prefix: `torero-overflow-${randomUUID()}`, logger };
        const client = track(createQueueClient({ task }, settings));
        const workers = track(
            createWorkers(
                {
                    task: job('overflow').handler(() => {
                        finished++;
                        return 'result';
                    }),
                },
                settings,
            ),
        );
        const stream = await client.jobs.task.schedules.get('fast').subscribe({ capacity: 1 });
        try {
            await client.jobs.task.schedules.upsert('fast', { everyMs: 25, limit: 3 }, undefined);
            await workers.start();
            await eventually(async () => {
                expect(finished).toBe(3);
            });
            await sleep(100);
            await expect(stream.next()).rejects.toBeInstanceOf(StreamOverflowError);
        } finally {
            stream.close();
        }
    });
});
