import { randomUUID } from 'node:crypto';
import { Worker } from 'bullmq';
import { afterEach, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
    createQueueClient,
    createWorkers,
    implement,
    job,
    JobCancelledError,
    JobNotFoundError,
    ProtocolError,
} from '../../src/index.js';
import { inputEnvelope } from '../../src/shared.js';

const connection = { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' };
const resources: { close(): Promise<unknown> }[] = [];
function track<T extends { close(): Promise<unknown> }>(resource: T): T {
    resources.push(resource);
    return resource;
}
const settings = () => ({
    connection,
    prefix: `torero-observation-${randomUUID()}`,
    logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
});
afterEach(async () => {
    vi.restoreAllMocks();
    for (const resource of resources.splice(0).reverse()) await resource.close();
});

it('keeps simultaneous waiters independent and reports removal to every remaining observer', async () => {
    const task = job('waiters');
    const client = track(createQueueClient({ task }, settings()));
    const handle = await client.jobs.task.publish();
    const raw = await client.jobs.task.raw();
    const getJob = raw.getJob.bind(raw);
    let count = 0;
    const ready = Promise.withResolvers<void>();
    vi.spyOn(raw, 'getJob').mockImplementation(async (id) => {
        const result = await getJob(id);
        if (++count === 3) ready.resolve();
        return result;
    });
    const controller = new AbortController();
    const reason = new Error('stop only this waiter');
    const aborted = expect(handle.wait({ signal: controller.signal })).rejects.toBe(reason);
    const first = expect(handle.wait()).rejects.toBeInstanceOf(JobCancelledError);
    const second = expect(handle.wait()).rejects.toBeInstanceOf(JobCancelledError);
    await ready.promise;
    controller.abort(reason);
    await aborted;
    expect(await handle.remove()).toBe('removed');
    await Promise.all([first, second]);
    await expect(handle.wait()).rejects.toBeInstanceOf(JobNotFoundError);
});

it('surfaces incompatible worker outputs both to job waiters and all schedule observers', async () => {
    const task = job('mixed-deployment').output(z.number());
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    // A real BullMQ worker models an accidentally deployed incompatible producer/processor pair.
    const worker = track(
        new Worker(
            task.definition.name,
            async () => ({ torero: 1, name: task.definition.name, version: 99, output: 'bad' }),
            { ...config, autorun: false },
        ),
    );
    await worker.waitUntilReady();
    const running = worker.run();
    const schedule = client.jobs.task.schedules.get('shared');
    const first = await schedule.subscribe();
    const second = await schedule.subscribe();
    try {
        await expect((await client.jobs.task.publish()).wait()).rejects.toBeInstanceOf(
            ProtocolError,
        );
        await client.jobs.task.schedules.upsert('shared', { everyMs: 60_000, limit: 1 }, undefined);
        for (const stream of [first, second])
            expect(await stream.next()).toMatchObject({
                done: false,
                value: { type: 'failed', error: { code: 'JOB_FAILED' } },
            });
        first.close();
        const pending = second.next();
        expect(await schedule.remove()).toBe(true);
        expect(await pending).toEqual({ done: true, value: undefined });
        expect(await schedule.remove()).toBe(false);
    } finally {
        first.close();
        second.close();
        await worker.close();
        await running;
    }
});

it('preserves throttle deduplication after completion until its key is explicitly cleared', async () => {
    const task = job('throttle')
        .input(z.number())
        .handler(({ input }) => input);
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const workers = track(createWorkers({ task }, config));
    await workers.start();
    const options = { deduplication: { id: 'recipient', ttl: 60_000 } };
    const first = await client.jobs.task.publish(1, options);
    expect(await first.wait()).toBe(1);
    const duplicate = await client.jobs.task.publish(2, options);
    expect(duplicate.id).toBe(first.id);
    expect(await duplicate.wait()).toBe(1);
    const raw = await client.jobs.task.raw();
    await raw.removeDeduplicationKey('recipient');
    const fresh = await client.jobs.task.publish(3, options);
    expect(fresh.id).not.toBe(first.id);
    expect(await fresh.wait()).toBe(3);
    expect(await client.jobs.task.publishBulk([])).toEqual([]);
});

it('supports dated scheduling, bounded cron windows, and inspection of native schedulers', async () => {
    const task = job('calendar');
    const client = track(createQueueClient({ task }, settings()));
    const future = new Date(Date.now() + 86_400_000);
    const end = new Date(future.getTime() + 86_400_000);
    const delayed = await client.jobs.task.schedule(undefined, { at: future });
    expect(await delayed.status()).toBe('delayed');
    const immediate = await client.jobs.task.schedule(undefined, { at: new Date(0) });
    expect(await immediate.status()).toBe('waiting');
    await client.jobs.task.schedules.upsert(
        'window',
        { cron: '0 * * * *', startDate: future, endDate: end, limit: 2 },
        undefined,
    );
    const raw = await client.jobs.task.raw();
    await raw.upsertJobScheduler(
        'native-id',
        { every: 60_000 },
        { name: task.definition.name, data: inputEnvelope(task, undefined) },
    );
    expect(await client.jobs.task.schedules.list()).toEqual(
        expect.arrayContaining([
            expect.objectContaining({ id: 'window', cron: '0 * * * *' }),
            expect.objectContaining({ id: 'native-id', everyMs: 60_000 }),
        ]),
    );
    const schedulers = await raw.getJobSchedulers();
    expect(schedulers.find((item) => item.pattern === '0 * * * *')).toMatchObject({
        startDate: future.getTime(),
        endDate: end.getTime(),
        limit: 2,
    });
});

it('rejects injected input on no-input jobs and keeps worker logs usable', async () => {
    const task = job('no-input');
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    let executions = 0;
    const workers = track(
        createWorkers(
            {
                task: task.handler(async ({ job: current }) => {
                    executions++;
                    await current.log('started');
                    return 1;
                }),
            },
            config,
        ),
    );
    const raw = await client.jobs.task.raw();
    const poison = await raw.add(task.definition.name, inputEnvelope(task, { injected: true }), {
        attempts: 3,
    });
    await workers.start();
    await expect(client.jobs.task.get(poison.id!).wait()).rejects.toMatchObject({
        details: { code: 'VALIDATION_ERROR' },
    });
    expect(executions).toBe(0);
    expect((await raw.getJob(poison.id!))?.attemptsMade).toBe(1);
    const valid = await client.jobs.task.publish();
    expect(await valid.wait()).toBe(1);
    expect(await raw.getJobLogs(valid.id)).toMatchObject({ logs: ['started'], count: 1 });
});

it('applies the execution deadline while awaiting context and never enters the handler afterward', async () => {
    let executions = 0;
    const task = implement(job('context-deadline'))
        .$context<{ ready: boolean }>()
        .worker({ timeoutMs: 10 })
        .handler(() => {
            executions++;
        });
    const config = settings();
    const client = track(createQueueClient({ task }, config));
    const workers = track(
        createWorkers(
            { task },
            {
                ...config,
                context: async ({ signal }) => {
                    await new Promise<void>((resolve) => {
                        if (signal.aborted) resolve();
                        else signal.addEventListener('abort', () => resolve(), { once: true });
                    });
                    return { ready: true };
                },
            },
        ),
    );
    await workers.start();
    await expect((await client.jobs.task.publish()).wait()).rejects.toMatchObject({
        details: { code: 'JOB_TIMEOUT' },
    });
    expect(executions).toBe(0);
});
