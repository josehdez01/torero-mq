import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

// Hoist fakes so the mock factory can reference them
const hoisted = vi.hoisted(() => {
    const constructedWorkers: any[] = [];
    class FakeQueue {
        name: string;
        constructor(name: string, _opts: any) {
            this.name = name;
        }
        async close() {}
        async add() {
            return {} as any;
        }
        async upsertJobScheduler() {}
        async removeJobScheduler() {}
        async getJob() {
            return null;
        }
    }
    class FakeEvents {
        private listeners = new Map<string, Set<Function>>();
        on(evt: string, fn: any) {
            if (!this.listeners.has(evt)) this.listeners.set(evt, new Set());
            this.listeners.get(evt)!.add(fn);
        }
        off(evt: string, fn: any) {
            this.listeners.get(evt)?.delete(fn);
        }
        emit(evt: string, payload: any) {
            for (const fn of this.listeners.get(evt) ?? []) fn(payload);
        }
        async close() {}
    }
    class FakeWorker {
        processor: Function;
        opts: any;
        constructor(_name: string, processor: any, opts: any) {
            this.processor = processor;
            this.opts = opts;
            constructedWorkers.push(this);
        }
        async close() {}
    }
    return { constructedWorkers, FakeQueue, FakeEvents, FakeWorker };
});

vi.mock('bullmq', () => ({
    Queue: hoisted.FakeQueue,
    QueueEvents: hoisted.FakeEvents,
    Worker: hoisted.FakeWorker,
}));

import { createBullmqRuntime } from './bullmq-runtime.js';

function makeConn() {
    const store = new Map<string, string>();
    return {
        set: vi.fn(async (key: string, val: string, _px: string, _ttl: number, mode: string) => {
            if (mode === 'NX' && store.has(key)) return null;
            store.set(key, val);
            return 'OK';
        }),
        get: vi.fn(async (key: string) => store.get(key) ?? null),
        del: vi.fn(async (key: string) => {
            store.delete(key);
            return 1;
        }),
        pexpire: vi.fn(async (_key: string, _ttl: number) => 1),
    } as any;
}

beforeEach(() => {
    hoisted.constructedWorkers.length = 0;
});

describe('createBullmqRuntime wiring and worker', () => {
    it('wires hooks and logs on events', async () => {
        const info = vi.fn();
        const error = vi.fn();
        const def = {
            name: 'alpha',
            inputSchema: z.any(),
            async process(v: any) {
                return v;
            },
            hooks: {
                onComplete: vi.fn(),
                onFailed: vi.fn(),
            },
        } as const;
        const runtime = await createBullmqRuntime(def as any, {
            connection: makeConn(),
            prefix: 'p',
            runWorkers: false,
            exclusiveWorkers: true,
            logger: { info, warn: () => {}, error },
        });
        // emit events
        (runtime.events as any).emit('completed', { jobId: '1' });
        (runtime.events as any).emit('failed', { jobId: '2', failedReason: 'nope' });
        expect(info).toHaveBeenCalled();
        expect(error).toHaveBeenCalled();
        expect(def.hooks!.onComplete).toHaveBeenCalledWith({ jobId: '1', queue: 'alpha' });
        expect(def.hooks!.onFailed).toHaveBeenCalledWith({
            jobId: '2',
            queue: 'alpha',
            failedReason: 'nope',
        });
    });

    it('spawns worker with concurrency and limiter when allowed', async () => {
        vi.useFakeTimers();
        const conn = makeConn();
        const runWithContext = vi.fn(async (fn: any) => await fn());
        const def = {
            name: 'beta',
            inputSchema: z.object({ x: z.number() }),
            async process(i: any) {
                return i.x + 1;
            },
            defaults: { concurrency: 7, limiter: { max: 1, duration: 1000 } },
        } as const;
        const runtime = await createBullmqRuntime(def as any, {
            connection: conn,
            prefix: 'p',
            runWorkers: true,
            exclusiveWorkers: true,
            runWithContext,
            logger: { info: () => {}, warn: () => {}, error: () => {} },
        });
        expect(hoisted.constructedWorkers.length).toBe(1);
        const w = hoisted.constructedWorkers[0];
        expect(w.opts.concurrency).toBe(7);
        expect(w.opts.limiter).toEqual({ max: 1, duration: 1000 });

        // processor validates inputs and uses runWithContext
        const result = await w.processor({ data: { payload: { x: 2 } } });
        expect(result).toBe(3);
        expect(runWithContext).toHaveBeenCalled();

        await runtime.shutdown();
        vi.runOnlyPendingTimers();
        vi.useRealTimers();
    });

    it('worker throws on invalid input, invalid output and timeout', async () => {
        vi.useFakeTimers();
        const conn = makeConn();
        const def = {
            name: 'gamma',
            inputSchema: z.object({ y: z.number() }),
            outputSchema: z.number(),
            async process(_i: any) {
                return 'bad' as any;
            },
            defaults: { timeoutMs: 10 },
        } as const;
        const runtime = await createBullmqRuntime(def as any, {
            connection: conn,
            prefix: 'p',
            runWorkers: true,
            exclusiveWorkers: true,
            logger: { info: () => {}, warn: () => {}, error: () => {} },
        });
        const w = hoisted.constructedWorkers.at(-1)!;

        // invalid input
        await expect(w.processor({ data: { payload: {} } })).rejects.toThrow(/Invalid input/);

        // invalid output
        await expect(w.processor({ data: { payload: { y: 1 } } })).rejects.toThrow(
            /Invalid output/,
        );

        // timeout path using slow job
        const def2 = {
            name: 'delta',
            inputSchema: z.object({ y: z.number() }),
            async process() {
                await new Promise((r) => setTimeout(r, 50));
                return 1;
            },
            defaults: { timeoutMs: 5 },
        } as const;
        const runtime2 = await createBullmqRuntime(def2 as any, {
            connection: conn,
            prefix: 'p',
            runWorkers: true,
            exclusiveWorkers: true,
            logger: { info: () => {}, warn: () => {}, error: () => {} },
        });
        const w2 = hoisted.constructedWorkers.at(-1)!;
        const p = w2.processor({ data: { payload: { y: 1 } } });
        vi.advanceTimersByTime(10);
        await expect(p).rejects.toThrow(/timed out/);

        await runtime.shutdown();
        await runtime2.shutdown();
        vi.useRealTimers();
    });
});
