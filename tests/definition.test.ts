import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { executeImplementation, implement, job, parseSchema } from '../src/definition.js';
import type { JobDefaults, MiddlewareArgs, MiddlewareResult } from '../src/definition.js';
import { decode, encode } from '../src/codec.js';
import { CodecError, ValidationError } from '../src/errors.js';
import type { JobContext } from '../src/definition.js';
const info: JobContext = {
    id: '1',
    name: 'test',
    attemptsMade: 0,
    async updateProgress() {},
    async log() {},
};
const args = { input: 2, context: { base: 3 }, job: info, signal: new AbortController().signal };
describe('definitions', () => {
    it('builds immutable contracts with stable names and versions', () => {
        const base = job('test');
        const configured = base.options({ attempts: 3 }).input(z.number()).output(z.number());
        expect(base.definition.defaults).toEqual({});
        expect(configured.definition).toMatchObject({
            name: 'test',
            version: 1,
            defaults: { attempts: 3 },
        });
        expect(Object.isFrozen(configured.definition.defaults)).toBe(true);
        expect(() => job('bad:name')).toThrow();
        expect(() => job('name', { version: 0 })).toThrow();
    });
    it('copies nested defaults and worker settings when builders branch', async () => {
        const backoff = { type: 'exponential' as const, delay: 10 };
        const configured = job('defaults').options({ backoff });
        backoff.delay = 99;
        expect(configured.definition.defaults.backoff).toEqual({ type: 'exponential', delay: 10 });
        const limiter = { max: 2, duration: 1000 };
        const builder = implement(job('worker')).worker({ limiter });
        limiter.max = 99;
        expect(builder.handler(() => 1).workerOptions.limiter?.max).toBe(2);
        expect(() => job('invalid').options({ attempts: 0 })).toThrow('attempts');
        expect(() => implement(job('invalid')).worker({ concurrency: -1 })).toThrow('concurrency');
    });
    it('retains the worker slot until detached next finishes after middleware failure', async () => {
        let release!: () => void;
        let handlerFinished = false;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        const task = implement(job('detached'))
            .use(({ next }) => {
                void next();
                throw new Error('middleware failed');
            })
            .handler(async () => {
                await gate;
                handlerFinished = true;
            });
        let settled = false;
        const execution = task.execute(args);
        void execution.then(
            () => {
                settled = true;
            },
            () => {
                settled = true;
            },
        );
        await Promise.resolve();
        expect(settled).toBe(false);
        release();
        await expect(execution).rejects.toThrow('middleware failed');
        expect(handlerFinished).toBe(true);
    });
    it('rejects a middleware that short circuits without invoking its handler', async () => {
        let invoked = false;
        const task = implement(job('missing-next'))
            .use(() => ({}) as MiddlewareResult<object>)
            .handler(() => {
                invoked = true;
            });
        await expect(task.execute(args)).rejects.toThrow('exactly once');
        expect(invoked).toBe(false);
    });
    it('rejects deferred next after middleware has finished without running detached work', async () => {
        let deferred!: MiddlewareArgs<void, object>['next'];
        let invoked = false;
        const task = implement(job('late-next'))
            .use(({ next }) => {
                deferred = next;
                return {} as MiddlewareResult<object>;
            })
            .handler(() => {
                invoked = true;
            });
        await expect(task.execute(args)).rejects.toThrow('exactly once');
        expect(() => deferred()).toThrow('after middleware settles');
        expect(invoked).toBe(false);
    });
    it('does not accept a previous invocation continuation for a different job', async () => {
        let previous: MiddlewareResult<object> | undefined;
        const task = implement(job('reused-token'))
            .use(async ({ next }) => {
                const current = await next();
                const returned = previous ?? current;
                previous = current;
                return returned;
            })
            .handler(() => ({ value: 42 }));
        expect(await executeImplementation(task, args)).toEqual({ value: 42 });
        await expect(executeImplementation(task, args)).rejects.toThrow('exact continuation');
    });
    it('guards context requirements even when called from untyped JavaScript', () => {
        const builder = implement(job('context-reset')).use(({ next }) => next());
        expect(() => Reflect.apply(builder.$context, builder, [])).toThrow(
            '$context must be called before use',
        );
    });
    it('keeps immutable defaults separate across branches including retention', () => {
        const keep = { age: 60, count: 10 };
        const base = job('branches').options({
            removeOnComplete: keep,
            removeOnFail: { count: 5 },
        });
        const first = base.options({ attempts: 2 });
        const second = base.options({ attempts: 3 });
        keep.age = 999;
        expect(first.definition.defaults).toMatchObject({
            attempts: 2,
            removeOnComplete: { age: 60, count: 10 },
            removeOnFail: { count: 5 },
        });
        expect(second.definition.defaults.attempts).toBe(3);
        expect(base.definition.defaults.attempts).toBeUndefined();
        expect(Object.isFrozen(first.definition.defaults.removeOnComplete)).toBe(true);
    });
    it.each([
        { priority: 2 ** 21 + 1 },
        { lifo: 'yes' },
        { backoff: { type: 'custom', delay: 5 } },
        { backoff: { type: 'fixed', delay: -1 } },
        { backoff: { type: 'fixed', jitter: 1.1 } },
        { backoff: -1 },
        { removeOnComplete: null },
        { removeOnFail: { age: -1 } },
        { removeOnComplete: -1 },
    ])('rejects unsafe operational defaults from JavaScript: %j', (defaults) => {
        expect(() => job('unsafe-options').options(defaults as JobDefaults)).toThrow(TypeError);
    });
    it('allows zero-count retention and fixed backoff while rejecting invalid worker deadlines', () => {
        expect(
            job('zero-retention').options({
                removeOnComplete: 0,
                removeOnFail: { count: 0 },
                backoff: 0,
                priority: 0,
            }).definition.defaults,
        ).toMatchObject({ removeOnComplete: 0, backoff: 0 });
        expect(() => implement(job('deadline')).worker({ timeoutMs: 2_147_483_648 })).toThrow(
            '2147483647',
        );
        expect(() =>
            implement(job('limiter')).worker({ limiter: { max: 1, duration: 0 } }),
        ).toThrow('limiter.duration');
    });
    it('validates asynchronous transformations and exposes structured issues', async () => {
        const schema = z.string().transform(async (value) => Number(value));
        expect(await parseSchema(schema, '12', { phase: 'input', queue: 'q' })).toBe(12);
        await expect(parseSchema(schema, 12, { phase: 'input', queue: 'q' })).rejects.toMatchObject(
            { name: 'ValidationError', phase: 'input', queue: 'q' },
        );
        await expect(parseSchema(schema, 12, { phase: 'input' })).rejects.toBeInstanceOf(
            ValidationError,
        );
    });
    it('wraps middleware in insertion order and supports cleanup', async () => {
        const events: string[] = [];
        const base = implement(job('test').input(z.number())).$context<{ base: number }>();
        const extended = base
            .use(async ({ context, next }) => {
                events.push('enter');
                try {
                    return await next({ context: { extra: context.base + 1 } });
                } finally {
                    events.push('exit');
                }
            })
            .use(async ({ context, next }) => {
                events.push(`extra:${context.extra}`);
                return next();
            });
        const task = extended.handler(({ input, context }) => {
            events.push('handler');
            return input + context.extra;
        });
        expect(await task.execute(args)).toBe(6);
        expect(events).toEqual(['enter', 'extra:4', 'handler', 'exit']);
        expect(await base.handler(({ input }) => input).execute(args)).toBe(2);
    });
    it('rejects forged and copied continuation tokens instead of replacing output', async () => {
        const forged = implement(job('forged'))
            .use(async ({ next }) => {
                await next();
                return {} as Awaited<ReturnType<typeof next>>;
            })
            .handler(() => 42);
        await expect(forged.execute(args)).rejects.toThrow('exact continuation');
        const copied = implement(job('copied'))
            .use(async ({ next }) => ({ ...(await next()) }))
            .handler(() => 42);
        await expect(copied.execute(args)).rejects.toThrow('exact continuation');
    });
    it('keeps continuation results private and preserves handler object identity', async () => {
        const output = { answer: 42 };
        const task = implement(job('opaque'))
            .use(async ({ next }) => {
                const continuation = await next();
                expect(Reflect.ownKeys(continuation)).toEqual([]);
                expect(Object.isFrozen(continuation)).toBe(true);
                return continuation;
            })
            .handler(() => output);
        expect(await task.execute(args)).toBe(output);
    });
    it('rejects repeated next invocations', async () => {
        const task = implement(job('test'))
            .use(async ({ next }) => {
                await next();
                return next();
            })
            .handler(() => 1);
        await expect(task.execute(args)).rejects.toThrow('only be called once');
    });
    it('executes middleware cleanup when a handler fails', async () => {
        let cleaned = false;
        const task = implement(job('test'))
            .use(async ({ next }) => {
                try {
                    return await next();
                } finally {
                    cleaned = true;
                }
            })
            .handler(() => {
                throw new Error('failure');
            });
        await expect(task.execute(args)).rejects.toThrow('failure');
        expect(cleaned).toBe(true);
    });
});
describe('codec', () => {
    it('roundtrips rich values and undefined properties', () => {
        const value = {
            absent: undefined,
            nil: null,
            date: new Date('2025-01-01'),
            big: 42n,
            map: new Map([[1, new Set([undefined, 2])]]),
            special: [NaN, Infinity, -Infinity, -0],
            regex: /test/gi,
        };
        expect(decode(encode(value))).toEqual(value);
        expect(decode(encode(undefined))).toBeUndefined();
    });
    it('roundtrips cyclic and shared references', () => {
        const shared = { value: 1 };
        const value: { first: typeof shared; second: typeof shared; self?: unknown } = {
            first: shared,
            second: shared,
        };
        value.self = value;
        const restored = decode(encode(value)) as typeof value;
        expect(restored.self).toBe(restored);
        expect(restored.first).toBe(restored.second);
    });
    it('rejects accessors, sparse arrays, subclasses and hidden lossy state', () => {
        let invoked = false;
        const accessor = {
            get value() {
                invoked = true;
                return 1;
            },
        };
        const regex = /test/g;
        regex.lastIndex = 1;
        const customDate = Object.assign(new Date(), { extra: true });
        for (const value of [
            accessor,
            Array(2),
            new (class extends Map {})(),
            new (class extends Array {})(),
            regex,
            customDate,
            Object.defineProperty({}, 'secret', { value: 1 }),
        ])
            expect(() => encode(value)).toThrow(CodecError);
        expect(invoked).toBe(false);
    });
    it('rejects values which would silently lose meaning', () => {
        for (const value of [
            () => 1,
            Symbol('x'),
            { nested: () => 1 },
            new (class Custom {})(),
            new Date(NaN),
        ])
            expect(() => encode(value)).toThrow(CodecError);
        expect(() => decode('{"version":2,"payload":{"json":1}}')).toThrow(CodecError);
    });
});
