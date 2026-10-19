import { randomUUID } from 'node:crypto';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { createQueueClient, createWorkers, implement, job, JobError } from '../../src/index.js';
import type { WireInput } from '../../src/index.js';

const connection = { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' };
const resources: { close(): Promise<unknown> }[] = [];
const prefix = () => `torero-e2e-${randomUUID()}`;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
function track<T extends { close(): Promise<unknown> }>(resource: T): T {
    resources.push(resource);
    return resource;
}
async function eventually(assertion: () => Promise<void>, timeoutMs = 5_000) {
    const deadline = Date.now() + timeoutMs;
    let failure: unknown;
    do {
        try {
            await assertion();
            return;
        } catch (error) {
            failure = error;
            await sleep(20);
        }
    } while (Date.now() < deadline);
    throw failure;
}
afterEach(async () => {
    for (const resource of resources.splice(0).reverse()) await resource.close();
});

describe('Redis runtime', () => {
    it('isolates identical job names by namespace and supports nested routers', async () => {
        const task = job('namespace').input(z.number()).output(z.number());
        const left = prefix();
        const right = prefix();
        const a = track(createQueueClient({ nested: { task } }, { connection, prefix: left }));
        const b = track(createQueueClient({ nested: { task } }, { connection, prefix: right }));
        const wa = track(
            createWorkers(
                { nested: { task: task.handler(({ input }) => input + 1) } },
                { connection, prefix: left },
            ),
        );
        const wb = track(
            createWorkers(
                { nested: { task: task.handler(({ input }) => input + 100) } },
                { connection, prefix: right },
            ),
        );
        await Promise.all([wa.start(), wb.start()]);
        const [one, two] = await Promise.all([
            a.jobs.nested.task.publish(1, { jobId: 'shared' }),
            b.jobs.nested.task.publish(1, { jobId: 'shared' }),
        ]);
        expect(await one.wait()).toBe(2);
        expect(await two.wait()).toBe(101);
    });

    it('runs async input transforms/defaults exactly once and output transforms', async () => {
        const task = job('transforms')
            .input(
                z
                    .object({ value: z.string().default('4') })
                    .transform(async ({ value }) => ({ amount: Number(value) + 1 })),
            )
            .output(z.number().transform(async (value) => `value:${value}`));
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                { task: task.handler(({ input }) => input.amount * 2) },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        expect(await (await client.jobs.task.publish({})).wait()).toBe('value:10');
    });

    it('preserves rich wire values, null and undefined results', async () => {
        const rich = job('rich').input(z.unknown()).output(z.unknown());
        const nil = job('nil').handler(() => null);
        const absent = job('absent').handler(() => undefined);
        const scope = prefix();
        const client = track(
            createQueueClient({ rich, nil, absent }, { connection, prefix: scope }),
        );
        const workers = track(
            createWorkers(
                { rich: rich.handler(({ input }) => input), nil, absent },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        const value = {
            date: new Date('2026-01-01'),
            bigint: 22n,
            map: new Map([['key', new Set([1, 2])]]),
            absent: undefined,
            nil: null,
        };
        expect(await (await client.jobs.rich.publish(value)).wait()).toEqual(value);
        expect(await (await client.jobs.nil.publish()).wait()).toBeNull();
        expect(await (await client.jobs.absent.publish()).wait()).toBeUndefined();
    });

    it('replays persisted results for late waiters and a restarted client', async () => {
        const task = job('persistent').input(z.number()).output(z.number());
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                { task: task.handler(({ input }) => input * 2) },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        const handle = await client.jobs.task.publish(21);
        expect(await handle.wait()).toBe(42);
        expect(await handle.wait()).toBe(42);
        expect(await handle.result()).toBe(42);
        await client.close();
        const restarted = track(createQueueClient({ task }, { connection, prefix: scope }));
        expect(await restarted.jobs.task.get(handle.id).wait()).toBe(42);
        expect(await restarted.jobs.task.get(handle.id).result()).toBe(42);
        const raw = await restarted.jobs.task.raw();
        expect((await raw.getJob(handle.id))?.opts).toMatchObject({
            removeOnComplete: { age: 86_400, count: 1_000 },
            removeOnFail: { age: 604_800, count: 5_000 },
        });
    });

    it('adds context through middleware and executes cleanup after failures', async () => {
        const events: string[] = [];
        const task = job('context').input(z.boolean()).options({ attempts: 1 });
        const implementation = implement(task)
            .$context<{ value: number }>()
            .use(async ({ context, next }) => {
                events.push('enter');
                try {
                    return await next({ context: { doubled: context.value * 2 } });
                } finally {
                    events.push('exit');
                }
            })
            .handler(({ input, context }) => {
                if (input) throw new Error('intentional');
                return context.doubled;
            });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                { task: implementation },
                { connection, prefix: scope, context: async () => ({ value: 3 }) },
            ),
        );
        await workers.start();
        expect(await (await client.jobs.task.publish(false)).wait()).toBe(6);
        await expect((await client.jobs.task.publish(true)).wait()).rejects.toThrow('intentional');
        expect(events).toEqual(['enter', 'exit', 'enter', 'exit']);
    });

    it('enforces global concurrency across independent worker groups', async () => {
        let active = 0;
        let maximum = 0;
        const task = job('concurrency').input(z.number());
        const implementation = task.handler(async ({ input }) => {
            active++;
            maximum = Math.max(maximum, active);
            await sleep(35);
            active--;
            return input;
        });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        await client.jobs.task.setGlobalConcurrency(1);
        const first = track(createWorkers({ task: implementation }, { connection, prefix: scope }));
        const second = track(
            createWorkers({ task: implementation }, { connection, prefix: scope }),
        );
        await Promise.all([first.start(), second.start()]);
        const handles = await client.jobs.task.publishBulk(
            Array.from({ length: 8 }, (_, input) => ({ input })),
        );
        expect(await Promise.all(handles.map((handle) => handle.wait()))).toEqual([
            0, 1, 2, 3, 4, 5, 6, 7,
        ]);
        expect(maximum).toBe(1);
        await client.jobs.task.removeGlobalConcurrency();
    });

    it('signals deadlines and waits for cleanup before retrying', async () => {
        let active = 0;
        let maximum = 0;
        let starts = 0;
        const task = job('deadline').options({ attempts: 2 });
        const implementation = implement(task)
            .worker({ timeoutMs: 30 })
            .handler(async ({ signal }) => {
                starts++;
                active++;
                maximum = Math.max(maximum, active);
                try {
                    await new Promise<void>((resolve) => {
                        if (signal.aborted) resolve();
                        else signal.addEventListener('abort', () => resolve(), { once: true });
                    });
                    await sleep(60);
                    return 'late';
                } finally {
                    active--;
                }
            });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers({ task: implementation }, { connection, prefix: scope }),
        );
        await workers.start();
        await expect((await client.jobs.task.publish()).wait()).rejects.toThrow();
        expect(starts).toBe(2);
        expect(maximum).toBe(1);
        expect(active).toBe(0);
    });

    it('keeps ownership until an abort-ignoring handler settles before retrying', async () => {
        let active = 0;
        let maximum = 0;
        let attempts = 0;
        const task = job('ignored-abort').options({ attempts: 2 });
        const implementation = implement(task)
            .worker({ timeoutMs: 20 })
            .handler(async () => {
                attempts++;
                active++;
                maximum = Math.max(maximum, active);
                try {
                    await sleep(80);
                    return 'late';
                } finally {
                    active--;
                }
            });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers({ task: implementation }, { connection, prefix: scope }),
        );
        await workers.start();
        await expect((await client.jobs.task.publish()).wait()).rejects.toMatchObject({
            details: { code: 'JOB_TIMEOUT' },
        });
        expect(attempts).toBe(2);
        expect(maximum).toBe(1);
        expect(active).toBe(0);
    });

    it('persists permanent domain error codes and rich data across client restarts', async () => {
        let attempts = 0;
        const data = { retryAt: new Date('2026-10-19'), balance: 42n, missing: undefined };
        const task = job('domain-error').options({ attempts: 4 });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                {
                    task: task.handler(() => {
                        attempts++;
                        throw new JobError('LIMIT_EXCEEDED', data, {
                            message: 'Account limit exceeded',
                        });
                    }),
                },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        const handle = await client.jobs.task.publish();
        await expect(handle.wait()).rejects.toMatchObject({
            details: { code: 'LIMIT_EXCEEDED', data },
        });
        expect(attempts).toBe(1);
        await client.close();
        const restarted = track(createQueueClient({ task }, { connection, prefix: scope }));
        await expect(restarted.jobs.task.get(handle.id).result()).rejects.toMatchObject({
            details: { code: 'LIMIT_EXCEEDED', data },
        });
    });

    it('fails invalid wire data and invalid output permanently without retrying', async () => {
        let invocations = 0;
        const task = job('invalid').input(z.number()).output(z.number()).options({ attempts: 4 });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                {
                    task: task.handler(() => {
                        invocations++;
                        return 'invalid' as unknown as number;
                    }),
                },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        await expect((await client.jobs.task.publish(1)).wait()).rejects.toThrow();
        expect(invocations).toBe(1);
        const raw = await client.jobs.task.raw();
        const malformed = await raw.add('invalid', { malformed: true } as unknown as WireInput, {
            attempts: 4,
        });
        await eventually(async () => {
            expect(await malformed.getState()).toBe('failed');
        });
        expect(invocations).toBe(1);
        expect((await raw.getJob(malformed.id!))?.attemptsMade).toBe(1);
    });

    it('deduplicates jobs, publishes batches and cancels delayed work', async () => {
        const task = job('publish').input(z.number());
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        await client.jobs.task.pause();
        const one = await client.jobs.task.publish(1, { deduplication: { id: 'duplicate' } });
        const duplicate = await client.jobs.task.publish(2, { deduplication: { id: 'duplicate' } });
        expect(duplicate.id).toBe(one.id);
        const delayed = await client.jobs.task.schedule(3, { delayMs: 60_000 });
        expect(await delayed.status()).toBe('delayed');
        expect(await delayed.remove()).toBe('removed');
        expect(await delayed.remove()).toBe('missing');
        const batch = await client.jobs.task.publishBulk([{ input: 4 }, { input: 5 }]);
        const workers = track(
            createWorkers(
                { task: task.handler(({ input }) => input) },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        await client.jobs.task.resume();
        expect(await one.wait()).toBe(1);
        expect(await Promise.all(batch.map((handle) => handle.wait()))).toEqual([4, 5]);
    });

    it('aborts a waiter without cancelling its job', async () => {
        const task = job('abort').input(z.number());
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const handle = await client.jobs.task.publish(7);
        const controller = new AbortController();
        const waiting = handle.wait({ signal: controller.signal });
        const rejected = expect(waiting).rejects.toThrow();
        controller.abort();
        await rejected;
        const workers = track(
            createWorkers(
                { task: task.handler(({ input }) => input) },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        expect(await handle.wait()).toBe(7);
    });

    it('retries transient failures and reports pending, active and missing jobs', async () => {
        let attempts = 0;
        let release!: () => void;
        let markStarted!: () => void;
        const started = new Promise<void>((resolve) => {
            markStarted = resolve;
        });
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const task = job('retry')
            .input(z.number())
            .options({ attempts: 2, backoff: { type: 'fixed', delay: 20 } });
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const handle = await client.jobs.task.publish(9);
        await expect(handle.result()).rejects.toThrow();
        await expect(handle.wait({ timeoutMs: 30 })).rejects.toThrow();
        await expect(client.jobs.task.get('missing').result()).rejects.toThrow();
        const workers = track(
            createWorkers(
                {
                    task: task.handler(async ({ input }) => {
                        attempts++;
                        if (attempts === 1) throw new Error('transient');
                        markStarted();
                        await gate;
                        return input;
                    }),
                },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        try {
            await Promise.race([
                started,
                sleep(3_000).then(() => {
                    throw new Error('Retry did not start');
                }),
            ]);
            expect(await handle.status()).toBe('active');
            expect(await handle.remove()).toBe('active');
        } finally {
            release();
        }
        expect(await handle.wait()).toBe(9);
        expect(attempts).toBe(2);
        const raw = await client.jobs.task.raw();
        expect((await raw.getJob(handle.id))?.attemptsMade).toBe(2);
    });

    it('filters scheduler events by public ID and removes only the selected schedule', async () => {
        const task = job('scheduler').input(z.string()).output(z.string());
        const scope = prefix();
        const client = track(createQueueClient({ task }, { connection, prefix: scope }));
        const workers = track(
            createWorkers(
                { task: task.handler(({ input }) => input) },
                { connection, prefix: scope },
            ),
        );
        await workers.start();
        const chosen = client.jobs.task.schedules.get('chosen');
        const subscription = await chosen.subscribe({ capacity: 10 });
        try {
            await client.jobs.task.schedules.upsert('other', { everyMs: 150 }, 'other');
            await client.jobs.task.schedules.upsert('chosen', { everyMs: 150 }, 'chosen');
            const event = await Promise.race([
                subscription.next(),
                sleep(5_000).then(() => {
                    throw new Error('No scheduler event received');
                }),
            ]);
            expect(event.done).toBe(false);
            expect(event.value).toMatchObject({ type: 'completed', result: 'chosen' });
            expect((await client.jobs.task.schedules.list()).map((item) => item.id).sort()).toEqual(
                ['chosen', 'other'],
            );
            await chosen.remove();
            expect((await client.jobs.task.schedules.list()).map((item) => item.id)).toEqual([
                'other',
            ]);
            await client.jobs.task.schedules.get('other').remove();
        } finally {
            await subscription.close();
        }
    });
});
