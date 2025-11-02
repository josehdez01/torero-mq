import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { deriveJobId } from './helpers.js';

// Mock runtime
vi.mock('./runtime/bullmq-runtime.js', () => ({
    createBullmqRuntime: vi.fn(),
    spawnWorker: vi.fn(),
}));

import * as runtimeMod from './runtime/bullmq-runtime.js';
import { QueueService } from './service.js';

type AddCall = { name: string; data: any; opts: any };

function setupService(logger?: any) {
    const addCalls: AddCall[] = [];
    const jobs = new Map<string, any>();
    (runtimeMod.createBullmqRuntime as any).mockReset();
    (runtimeMod.createBullmqRuntime as any).mockImplementation(async () => ({
        queue: {
            add: vi.fn(async (name: string, data: any, opts: any) => {
                addCalls.push({ name, data, opts });
                const id = String(opts?.jobId ?? `${addCalls.length}`);
                const job = {
                    id,
                    waitUntilFinished: vi.fn(
                        async (_events: any, _timeout: number) => jobs.get(id)?.result,
                    ),
                    remove: vi.fn(async () => {
                        if (jobs.get(id)?.removeReject) throw new Error('remove failed');
                    }),
                };
                jobs.set(id, { job });
                return job as any;
            }),
            // used by repeat tests, safe defaults here
            upsertJobScheduler: vi.fn(async () => {}),
            removeJobScheduler: vi.fn(async () => {}),
            getJob: vi.fn(async (id: string) => jobs.get(String(id))?.job ?? null),
        },
        events: {
            on: vi.fn(),
            off: vi.fn(),
            close: vi.fn(),
        },
        shutdown: vi.fn(async () => {}),
    }));

    const svc = new QueueService({
        prefix: 'p',
        logger: logger ?? { info: () => {}, warn: () => {}, error: () => {} },
    });
    const q = svc.defineQueue({
        name: 'q1',
        inputSchema: z.object({ n: z.number() }),
        outputSchema: z.number(),
        async process(input) {
            return input.n * 2;
        },
        defaults: { attempts: 2, timeoutMs: 1234 },
    });
    return { svc, q, addCalls, jobs };
}

beforeEach(() => {
    (runtimeMod.createBullmqRuntime as any).mockReset();
});

describe('enqueue and schedule', () => {
    it('publish validates input, builds options and awaits result with precedence', async () => {
        const { svc, q, addCalls, jobs } = setupService();
        await svc.initQueues({ connection: {} as any });

        // invalid input -> throws
        await expect(q.publish({} as any)).rejects.toThrow(/Invalid input/);

        // valid publish
        const handle = await q.publish({ n: 2 }, { priority: 7, retries: 5 });
        // add called
        expect(addCalls).toHaveLength(1);
        const call = addCalls[0];
        expect(call.name).toBe('job');
        expect(call.data).toEqual({ payload: { n: 2 } });
        expect(call.opts.attempts).toBe(5);
        expect(call.opts.priority).toBe(7);
        // job id auto-generated as '1'
        expect(handle.jobId).toBe('1');

        // wait precedence: explicit arg > publish option > defaults > fallback
        jobs.get('1')!.result = 10;
        const result1 = await handle.awaitResult({ timeoutMs: 11 });
        expect(result1).toBe(10);

        const h2 = await q.publish({ n: 3 }, { timeoutMs: 22 });
        jobs.get('2')!.result = 6;
        const result2 = await h2.awaitResult();
        expect(result2).toBe(6);

        const h3 = await q.publish({ n: 4 });
        jobs.get('3')!.result = 8;
        const result3 = await h3.awaitResult();
        expect(result3).toBe(8);
    });

    it('cancel removes job and logs on failure', async () => {
        const warn = vi.fn();
        const { svc, q, jobs } = setupService({ info: () => {}, warn, error: () => {} });
        await svc.initQueues({ connection: {} as any });

        const h1 = await q.publish({ n: 1 });
        // success path
        await expect(h1.cancel()).resolves.toBeUndefined();

        const h2 = await q.publish({ n: 2 });
        jobs.get(h2.jobId)!.removeReject = true;
        await h2.cancel();
        expect(warn).toHaveBeenCalled();
    });

    it('idempotencyKey string and function produce jobId via deriveJobId', async () => {
        const { svc, q, addCalls } = setupService();
        await svc.initQueues({ connection: {} as any });

        await q.publish({ n: 1 }, { idempotencyKey: 'abc' });
        const expected1 = deriveJobId('p', 'q1', 'abc');
        expect(addCalls.at(-1)!.opts.jobId).toBe(expected1);

        await q.publish({ n: 2 }, { idempotencyKey: (i) => `n=${i.n}` });
        const expected2 = deriveJobId('p', 'q1', 'n=2');
        expect(addCalls.at(-1)!.opts.jobId).toBe(expected2);
    });

    it('scheduleIn adds delay and scheduleAt never negative', async () => {
        const { svc, q, addCalls } = setupService();
        await svc.initQueues({ connection: {} as any });

        await q.scheduleIn({ delayMs: 123 }, { n: 1 });
        expect(addCalls.at(-1)!.opts.delay).toBe(123);

        const whenPast = new Date(Date.now() - 10_000);
        await q.scheduleAt({ when: whenPast }, { n: 2 });
        expect(addCalls.at(-1)!.opts.delay).toBe(0);

        const whenFuture = new Date(Date.now() + 50);
        await q.scheduleAt({ when: whenFuture }, { n: 3 });
        expect(addCalls.at(-1)!.opts.delay).toBeGreaterThanOrEqual(0);
    });
});
