import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { createQueueClient, createWorkers, implement, job, JobError } from '../../src/index.js';

const connection = { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' };
const logger = { error() {}, warn() {}, info() {} };
const settings = () => ({ connection, prefix: `torero-project-${randomUUID()}`, logger });
const resources: { close(): Promise<unknown> }[] = [];
function track<T extends { close(): Promise<unknown> }>(resource: T): T {
    resources.push(resource);
    return resource;
}
async function bounded<T>(work: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            work,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error('Project pattern timed out')), 4_000);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}
afterEach(async () => {
    const outcomes = await Promise.allSettled(
        resources
            .splice(0)
            .reverse()
            .map((resource) => resource.close()),
    );
    for (const outcome of outcomes) if (outcome.status === 'rejected') throw outcome.reason;
});

describe('application patterns', () => {
    it('isolates concurrent ORM request scopes and cleans up successful and failed jobs', async () => {
        type Session = { requestId: string; closed: boolean };
        const storage = new AsyncLocalStorage<Session>();
        const sessions: Session[] = [];
        let arrived = 0;
        let release!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const task = job('wedding-email')
            .input(z.object({ recipient: z.string(), fail: z.boolean() }))
            .options({ attempts: 1 });
        const implementation = implement(task)
            .$context<{ orm: { fork(requestId: string): Session } }>()
            .use(({ context, job: currentJob, next }) => {
                const session = context.orm.fork(currentJob.id);
                return storage.run(session, async () => {
                    try {
                        return await next({ context: { session } });
                    } finally {
                        session.closed = true;
                    }
                });
            })
            .worker({ concurrency: 3 })
            .handler(async ({ input, context, job: currentJob }) => {
                expect(storage.getStore()).toBe(context.session);
                expect(context.session.requestId).toBe(currentJob.id);
                arrived++;
                if (arrived === 3) release();
                await gate;
                await Promise.resolve();
                expect(storage.getStore()).toBe(context.session);
                expect(context.session.closed).toBe(false);
                if (input.fail)
                    throw new JobError('RECIPIENT_SUPPRESSED', { recipient: input.recipient });
                return { recipient: input.recipient, requestId: context.session.requestId };
            });
        const scope = settings();
        const client = track(createQueueClient({ task }, scope));
        const workers = track(
            createWorkers(
                { task: implementation },
                {
                    ...scope,
                    context: () => ({
                        orm: {
                            fork(requestId) {
                                const session = { requestId, closed: false };
                                sessions.push(session);
                                return session;
                            },
                        },
                    }),
                },
            ),
        );
        const handles = await client.jobs.task.publishBulk([
            { input: { recipient: 'a', fail: false } },
            { input: { recipient: 'b', fail: false } },
            { input: { recipient: 'c', fail: true } },
        ]);
        await workers.start();
        const outcomes = await bounded(
            Promise.allSettled(handles.map((handle) => handle.wait())),
        ).finally(() => release());
        expect(outcomes.map((outcome) => outcome.status)).toEqual([
            'fulfilled',
            'fulfilled',
            'rejected',
        ]);
        expect(new Set(sessions.map((session) => session.requestId)).size).toBe(3);
        expect(sessions.every((session) => session.closed)).toBe(true);
        expect(storage.getStore()).toBeUndefined();
        expect(outcomes[0]).toMatchObject({ status: 'fulfilled', value: { recipient: 'a' } });
        expect(outcomes[1]).toMatchObject({ status: 'fulfilled', value: { recipient: 'b' } });
    });

    it('replays recipient outbox fanout without duplicate sends and resolves photo IDs in workers', async () => {
        const deliver = job('recipient-delivery')
            .input(z.object({ eventId: z.string(), recipientId: z.string() }))
            .output(z.object({ recipientId: z.string(), sent: z.literal(true) }));
        const photo = job('photo-normalize')
            .input(z.object({ photoId: z.string() }))
            .output(z.object({ photoId: z.string(), width: z.number(), url: z.string() }));
        const deliveries: string[] = [];
        const database = new Map([
            ['photo-1', { width: 2048, url: 'https://example.test/photo-1.jpg' }],
        ]);
        const scope = settings();
        const client = track(createQueueClient({ deliver, photo }, scope));
        const workers = track(
            createWorkers(
                {
                    deliver: deliver.handler(({ input }) => {
                        deliveries.push(input.recipientId);
                        return { recipientId: input.recipientId, sent: true };
                    }),
                    photo: photo.handler(({ input }) => {
                        const record = database.get(input.photoId);
                        if (!record)
                            throw new JobError('PHOTO_MISSING', { photoId: input.photoId });
                        return { ...record, photoId: input.photoId };
                    }),
                },
                scope,
            ),
        );
        const recipients = ['guest-a', 'guest-b'];
        const publishOutbox = () =>
            client.jobs.deliver.publishBulk(
                recipients.map((recipientId) => ({
                    input: { eventId: 'outbox-1', recipientId },
                    options: { jobId: `outbox-1-${recipientId}`, removeOnComplete: false },
                })),
            );
        const first = await publishOutbox();
        const replay = await publishOutbox();
        expect(replay.map((handle) => handle.id)).toEqual(first.map((handle) => handle.id));
        const image = await client.jobs.photo.publish({ photoId: 'photo-1' });
        await workers.start();
        expect(await Promise.all(first.map((handle) => handle.wait()))).toEqual(
            recipients.map((recipientId) => ({ recipientId, sent: true })),
        );
        const afterCompletionReplay = await publishOutbox();
        expect(await Promise.all(afterCompletionReplay.map((handle) => handle.wait()))).toEqual(
            recipients.map((recipientId) => ({ recipientId, sent: true })),
        );
        expect(deliveries.sort()).toEqual(recipients);
        expect(await image.wait()).toEqual({
            photoId: 'photo-1',
            width: 2048,
            url: 'https://example.test/photo-1.jpg',
        });
    });

    it('cancels delayed re-engagement and rechecks stale eligibility when work executes', async () => {
        const eligible = new Map([
            ['cancelled', true],
            ['returned', true],
            ['inactive', true],
        ]);
        const sent: string[] = [];
        const task = job('curu-reengagement').input(z.object({ userId: z.string() }));
        const scope = settings();
        const client = track(createQueueClient({ task }, scope));
        const workers = track(
            createWorkers(
                {
                    task: task.handler(({ input }) => {
                        if (!eligible.get(input.userId)) return { status: 'skipped' as const };
                        sent.push(input.userId);
                        return { status: 'sent' as const };
                    }),
                },
                scope,
            ),
        );
        const cancelled = await client.jobs.task.schedule(
            { userId: 'cancelled' },
            { delayMs: 100 },
            { jobId: 'reengage-cancelled' },
        );
        const stale = await client.jobs.task.schedule({ userId: 'returned' }, { delayMs: 100 });
        const active = await client.jobs.task.schedule({ userId: 'inactive' }, { delayMs: 100 });
        expect(await cancelled.remove()).toBe('removed');
        eligible.set('returned', false);
        await workers.start();
        expect(await stale.wait()).toEqual({ status: 'skipped' });
        expect(await active.wait()).toEqual({ status: 'sent' });
        expect(await cancelled.status()).toBe('unknown');
        expect(sent).toEqual(['inactive']);
    });

    it('upserts stable timezone-aware cron metadata without duplicate schedulers', async () => {
        const task = job('daily-reminder').input(z.object({ campaign: z.string() }));
        const client = track(createQueueClient({ task }, settings()));
        await client.jobs.task.schedules.upsert(
            'daily',
            {
                cron: '0 9 * * *',
                timezone: 'America/Mexico_City',
                startDate: new Date('2050-01-01'),
            },
            { campaign: 'first' },
        );
        await client.jobs.task.schedules.upsert(
            'daily',
            {
                cron: '30 10 * * *',
                timezone: 'America/Mexico_City',
                startDate: new Date('2050-01-01'),
            },
            { campaign: 'updated' },
        );
        expect(await client.jobs.task.schedules.list()).toMatchObject([
            { id: 'daily', cron: '30 10 * * *', timezone: 'America/Mexico_City' },
        ]);
        expect(await client.jobs.task.schedules.get('daily').remove()).toBe(true);
        expect(await client.jobs.task.schedules.list()).toEqual([]);
    });

    it('streams undefined completions and structured permanent failures without retained jobs', async () => {
        const absent = job('maintenance-void').handler(() => undefined);
        const fail = job('maintenance-failed').handler(() => {
            throw new JobError('MAINTENANCE_BLOCKED', { retryAt: new Date('2050-01-01') });
        });
        const scope = settings();
        const client = track(createQueueClient({ absent, fail }, scope));
        const workers = track(createWorkers({ absent, fail }, scope));
        const completions = await client.jobs.absent.schedules.get('void').subscribe();
        const failures = await client.jobs.fail.schedules.get('failed').subscribe();
        try {
            await client.jobs.absent.schedules.upsert(
                'void',
                { everyMs: 60_000, limit: 1 },
                undefined,
                { removeOnComplete: true },
            );
            await client.jobs.fail.schedules.upsert(
                'failed',
                { everyMs: 60_000, limit: 1 },
                undefined,
                { attempts: 3, removeOnFail: true },
            );
            await workers.start();
            const [completed, failed] = await bounded(
                Promise.all([completions.next(), failures.next()]),
            );
            expect(completed).toMatchObject({
                done: false,
                value: { type: 'completed', result: undefined },
            });
            expect(failed).toMatchObject({
                done: false,
                value: {
                    type: 'failed',
                    error: {
                        details: {
                            code: 'MAINTENANCE_BLOCKED',
                            data: { retryAt: new Date('2050-01-01') },
                        },
                    },
                },
            });
        } finally {
            completions.close();
            failures.close();
        }
    });
});
