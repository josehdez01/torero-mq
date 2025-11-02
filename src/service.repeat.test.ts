import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

// Mock runtime module
vi.mock('./runtime/bullmq-runtime.js', () => ({
    createBullmqRuntime: vi.fn(),
    spawnWorker: vi.fn(),
}));

import * as runtimeMod from './runtime/bullmq-runtime.js';
import { QueueService } from './service.js';
import { deriveJobId } from './helpers.js';

beforeEach(() => {
    (runtimeMod.createBullmqRuntime as any).mockReset();
});

describe('scheduleRepeat', () => {
    it('creates scheduler with proper options and yields results', async () => {
        // Simple event hub for completed
        const completedListeners: Array<(e: { jobId: string }) => void> = [];
        const jobs = new Map<string, any>();
        const upserts: any[] = [];
        const removes: any[] = [];
        (runtimeMod.createBullmqRuntime as any).mockImplementation(async () => ({
            queue: {
                upsertJobScheduler: vi.fn(async (id: string, opts: any, template: any) => {
                    upserts.push({ id, opts, template });
                }),
                removeJobScheduler: vi.fn(async (id: string) => {
                    removes.push(id);
                }),
                getJob: vi.fn(async (id: string) => jobs.get(String(id)) ?? null),
            },
            events: {
                on: vi.fn((evt: string, fn: any) => {
                    if (evt === 'completed') completedListeners.push(fn);
                }),
                off: vi.fn((evt: string, fn: any) => {
                    if (evt === 'completed') {
                        const idx = completedListeners.indexOf(fn);
                        if (idx >= 0) completedListeners.splice(idx, 1);
                    }
                }),
            },
            shutdown: vi.fn(),
        }));

        const svc = new QueueService({ prefix: 'pref' });
        const q = svc.defineQueue({
            name: 'repeat-q',
            inputSchema: z.object({ v: z.number() }),
            outputSchema: z.number(),
            async process(i) {
                return i.v * 3;
            },
            defaults: { attempts: 1 },
        });

        await svc.initQueues({ connection: {} as any });

        const startDate = new Date(Date.now() + 1000);
        const endDate = new Date(Date.now() + 5000);
        const stream = await q.scheduleRepeat(
            { type: 'interval', everyMs: 250, startDate, endDate, limit: 10 },
            { v: 2 },
            { priority: 1 },
        );

        // validate upsert args
        expect(upserts).toHaveLength(1);
        const { id: schedId, opts, template } = upserts[0];
        expect(opts).toMatchObject({ every: 250, startDate, endDate, limit: 10 });
        expect(template).toMatchObject({ name: 'job', data: { payload: { v: 2 } } });
        expect(template.opts).toMatchObject({ priority: 1 });

        // Simulate a completed job arrival
        jobs.set('42', {
            async getState() {
                return 'completed';
            },
            returnvalue: 99,
        });
        // Notify listeners
        for (const fn of completedListeners) await fn({ jobId: '42' });

        const iterator = stream[Symbol.asyncIterator]();
        const next = await iterator.next();
        expect(next.done).toBe(false);
        expect(next.value).toEqual({ jobId: '42', result: 99 });

        // Cancel should remove scheduler and close stream
        await stream.cancel();
        expect(removes).toEqual([schedId]);
        const end = await iterator.next();
        expect(end.done).toBe(true);
    });

    it('derives scheduler id from idempotencyKey and validates input', async () => {
        const upserts: any[] = [];
        (runtimeMod.createBullmqRuntime as any).mockImplementation(async () => ({
            queue: {
                upsertJobScheduler: vi.fn(async (id: string, opts: any, template: any) => {
                    upserts.push({ id, opts, template });
                }),
                removeJobScheduler: vi.fn(async () => {}),
                getJob: vi.fn(async () => null),
            },
            events: { on: vi.fn(), off: vi.fn() },
            shutdown: vi.fn(),
        }));

        const svc = new QueueService({ prefix: 'pref' });
        const q = svc.defineQueue({
            name: 'repeat-q2',
            inputSchema: z.object({ s: z.string() }),
            async process() {
                return 0;
            },
        });
        await svc.initQueues({ connection: {} as any });

        await q.scheduleRepeat(
            { type: 'cron', cron: '* * * * *' },
            { s: 'key' },
            { idempotencyKey: 'abc' },
        );
        const expected = deriveJobId('pref', 'repeat-q2', 'scheduler:abc');
        expect(upserts.at(-1)!.id).toBe(expected);

        await expect(
            q.scheduleRepeat({ type: 'cron', cron: '*' } as any, {} as any),
        ).rejects.toThrow(/Invalid input/);
    });
});
