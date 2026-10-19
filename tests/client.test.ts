import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import {
    createQueueClient,
    createWorkers,
    type ConnectionSettings,
    implement,
    job,
    type PublishOptions,
    type ScheduleSpec,
} from '../src/index.js';
import { assertJson } from '../src/shared.js';

const settings = { connection: { host: '127.0.0.1', port: 1 }, prefix: 'validation' };

describe('producer validation before Redis access', () => {
    it('rejects incomplete wiring and accidental input on no-input jobs before connecting', async () => {
        const task = job('no-input');
        for (const options of [
            { ...settings, prefix: '' },
            { ...settings, prefix: ' padded ' },
            { ...settings, connection: undefined },
            { ...settings, connection: { ...settings.connection, keyPrefix: 'unsafe' } },
        ])
            expect(() => createQueueClient({ task }, options as ConnectionSettings)).toThrow();
        expect(() => createWorkers({ task } as never, settings)).toThrow('Missing handler');
        expect(() => createQueueClient({ invalid: 1 } as never, settings)).toThrow('Routers');
        const client = createQueueClient({ task }, settings);
        try {
            await expect(client.jobs.task.publish(42 as never)).rejects.toMatchObject({
                code: 'VALIDATION_ERROR',
            });
            expect(() => client.jobs.task.get('')).toThrow('Job id');
            expect(() => client.jobs.task.schedules.get('')).toThrow('Scheduler id');
        } finally {
            await client.close();
        }
    });
    it('rejects timer overflows instead of turning a long deadline into 1ms', () => {
        expect(() => implement(job('overflow')).worker({ timeoutMs: 2_147_483_648 })).toThrow(
            'at most',
        );
        expect(() =>
            createQueueClient(
                { task: job('overflow') },
                { ...settings, startupTimeoutMs: 2_147_483_648 },
            ),
        ).toThrow('at most');
    });
    it('rejects invalid inputs, identifiers, and native options without connecting', async () => {
        const task = job('validation').input(z.number());
        const client = createQueueClient({ task }, settings);
        try {
            await expect(
                client.jobs.task.publish('wrong' as unknown as number),
            ).rejects.toMatchObject({ code: 'VALIDATION_ERROR' });
            for (const options of [
                { jobId: '12' },
                { jobId: 'a:b' },
                { attempts: 0 },
                { priority: 2 ** 21 + 1 },
                { backoff: { type: 'custom' } },
                { backoff: { type: 'fixed', jitter: 2 } },
                { removeOnComplete: { count: -1 } },
                { removeOnFail: null },
                { deduplication: { id: '', ttl: 100 } },
                { delay: -1 },
                { deduplication: { id: 'valid', ttl: 0 } },
            ]) {
                await expect(
                    client.jobs.task.publish(1, options as PublishOptions),
                ).rejects.toBeInstanceOf(TypeError);
            }
        } finally {
            await client.close();
        }
    });
    it('rejects ambiguous schedules and invalid dates', async () => {
        const client = createQueueClient({ task: job('scheduled') }, settings);
        try {
            await expect(
                client.jobs.task.schedule(undefined, { at: new Date(), delayMs: 1 } as never),
            ).rejects.toThrow('exactly one');
            await expect(
                client.jobs.task.schedule(undefined, { at: new Date(NaN) }),
            ).rejects.toThrow('valid');
            for (const spec of [
                { everyMs: 0 },
                { cron: '' },
                { cron: '* * * * *', startDate: new Date(NaN) },
                { everyMs: 1, endDate: '2030-01-01' },
                { everyMs: 1, cron: '* * * * *' },
                { everyMs: 1, timezone: 'UTC' },
                { cron: '* * * * *', timezone: 'Unknown/Timezone' },
                { everyMs: 1, startDate: new Date('2030-01-02'), endDate: new Date('2030-01-01') },
            ])
                await expect(
                    client.jobs.task.schedules.upsert('id', spec as ScheduleSpec, undefined),
                ).rejects.toThrow();
        } finally {
            await client.close();
        }
    });
    it('rejects duplicate names and circular routers at construction', () => {
        expect(() => createQueueClient({ one: job('same'), two: job('same') }, settings)).toThrow(
            'Duplicate',
        );
        const recursive: Record<string, unknown> = {};
        recursive.self = recursive;
        expect(() => createQueueClient(recursive as never, settings)).toThrow('Circular');
    });
});

describe('progress serialization', () => {
    it('accepts JSON and shared references but rejects lossy values and cycles', () => {
        const shared = { done: true };
        expect(() => assertJson({ items: [null, 0, 'ok', shared, shared] })).not.toThrow();
        const cycle: unknown[] = [];
        cycle.push(cycle);
        const disguisedSparse = Array(1) as unknown[] & { extra?: number };
        disguisedSparse.extra = 1;
        for (const value of [
            undefined,
            NaN,
            Infinity,
            new Date(),
            1n,
            cycle,
            disguisedSparse,
            {
                get value() {
                    throw new Error('Getter must not execute');
                },
            },
        ]) {
            expect(() => assertJson(value)).toThrow();
        }
    });
});
