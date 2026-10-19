import { Queue, type JobsOptions, type JobState, type RepeatOptions } from 'bullmq';
import {
    parseSchema,
    validateDefaults,
    validateTimeout,
    type JobDefaults,
    type AnyContract,
    type InputOf,
    type OutputOf,
} from './definition.js';
import {
    JobCancelledError,
    JobNotFoundError,
    ValidationError,
    WaitTimeoutError,
} from './errors.js';
import { EventHub, subscription, type ResultSubscription } from './events.js';
import {
    assertId,
    assertPositive,
    assertSettings,
    closeAll,
    ClosedError,
    connectionFor,
    failedError,
    flatten,
    inputEnvelope,
    isContract,
    JobFailedError,
    JobNotFinishedError,
    readOutput,
    readInput,
    stableId,
    within,
    type ConnectionSettings,
    type ContractRouter,
    type WireInput,
    type WireOutput,
} from './shared.js';

/** BullMQ options with internal scheduling/flow controls excluded. attempts includes the first run. */
export type PublishOptions = JobDefaults & Pick<JobsOptions, 'jobId' | 'delay' | 'deduplication'>;
export type PublishArgs<I> = undefined extends I
    ? [input?: I, options?: PublishOptions]
    : [input: I, options?: PublishOptions];
export interface WaitOptions {
    timeoutMs?: number;
    signal?: AbortSignal;
}
export type RemoveOutcome = 'removed' | 'missing' | 'active';
export interface JobHandle<O> {
    readonly id: string;
    readonly queueName: string;
    wait(options?: WaitOptions): Promise<O>;
    result(): Promise<O>;
    status(): Promise<JobState | 'unknown'>;
    remove(): Promise<RemoveOutcome>;
    retry(): Promise<void>;
}
export type ScheduleSpec = (
    | { everyMs: number; cron?: never; timezone?: never }
    | { cron: string; timezone?: string; everyMs?: never }
) & { startDate?: Date; endDate?: Date; limit?: number };
export type SchedulePublishOptions = Omit<PublishOptions, 'jobId' | 'delay' | 'deduplication'>;
export type ScheduleEvent<O> =
    | { type: 'completed'; jobId: string; result: O }
    | { type: 'failed'; jobId: string; error: JobFailedError };
export interface ScheduleHandle<O> {
    readonly id: string;
    remove(): Promise<boolean>;
    /** Future events only. Subscribe before upsert to observe the first immediate run. */
    subscribe(options?: {
        capacity?: number;
        signal?: AbortSignal;
    }): Promise<ResultSubscription<ScheduleEvent<O>>>;
}
export interface ScheduleInfo {
    id: string;
    next?: number;
    everyMs?: number;
    cron?: string;
    timezone?: string;
    iterationCount?: number;
}
export interface QueueHandle<I, O, N extends string = string> {
    readonly name: N;
    publish(...args: PublishArgs<I>): Promise<JobHandle<O>>;
    publishBulk(items: readonly { input: I; options?: PublishOptions }[]): Promise<JobHandle<O>[]>;
    schedule(
        input: I,
        when: { at: Date; delayMs?: never } | { delayMs: number; at?: never },
        options?: Omit<PublishOptions, 'delay'>,
    ): Promise<JobHandle<O>>;
    get(id: string): JobHandle<O>;
    counts(): Promise<Record<string, number>>;
    pause(): Promise<void>;
    resume(): Promise<void>;
    setGlobalConcurrency(concurrency: number): Promise<void>;
    removeGlobalConcurrency(): Promise<void>;
    /** Administrative escape hatch. Payloads use Torero's versioned envelopes. */
    raw(): Promise<Queue<WireInput, WireOutput>>;
    readonly schedules: {
        get(id: string): ScheduleHandle<O>;
        upsert(
            id: string,
            spec: ScheduleSpec,
            input: I,
            options?: SchedulePublishOptions,
        ): Promise<ScheduleHandle<O>>;
        list(start?: number, end?: number): Promise<ScheduleInfo[]>;
    };
}
export type QueueClientJobs<R> = {
    readonly [K in keyof R]: R[K] extends AnyContract
        ? QueueHandle<InputOf<R[K]>, OutputOf<R[K]>, R[K]['~types']['name']>
        : QueueClientJobs<R[K]>;
};
interface Runtime {
    queue: Queue<WireInput, WireOutput>;
    hub: EventHub;
}
const RETENTION = {
    removeOnComplete: { age: 86_400, count: 1_000 },
    removeOnFail: { age: 604_800, count: 5_000 },
} as const;

function publishOptions(contract: AnyContract, options: PublishOptions = {}): JobsOptions {
    options = structuredClone(options);
    validateDefaults(options);
    if (options.jobId !== undefined) assertId(options.jobId);
    if (options.delay !== undefined && (!Number.isSafeInteger(options.delay) || options.delay < 0))
        throw new TypeError('delay must be a nonnegative safe integer');
    if (options.attempts !== undefined) assertPositive(options.attempts, 'attempts');
    if (options.deduplication) {
        if (typeof options.deduplication.id !== 'string' || !options.deduplication.id)
            throw new TypeError('deduplication.id is required');
        if (options.deduplication.ttl !== undefined)
            assertPositive(options.deduplication.ttl, 'deduplication.ttl');
    }
    return { ...RETENTION, ...contract.definition.defaults, ...options };
}
function repeatOptions(spec: ScheduleSpec): RepeatOptions {
    if ((spec.everyMs !== undefined) === (spec.cron !== undefined))
        throw new TypeError('Supply exactly one of everyMs or cron');
    if (spec.everyMs !== undefined && spec.timezone !== undefined)
        throw new TypeError('timezone requires a cron schedule');
    if ('everyMs' in spec && spec.everyMs !== undefined) assertPositive(spec.everyMs, 'everyMs');
    else if (!spec.cron?.trim()) throw new TypeError('cron is required');
    if (spec.limit !== undefined) assertPositive(spec.limit, 'limit');
    for (const date of [spec.startDate, spec.endDate])
        if (date !== undefined && (!(date instanceof Date) || Number.isNaN(date.getTime())))
            throw new TypeError('Schedule dates must be valid Dates');
    if (spec.startDate && spec.endDate && spec.endDate <= spec.startDate)
        throw new TypeError('endDate must be after startDate');
    if (spec.timezone) new Intl.DateTimeFormat('en', { timeZone: spec.timezone });
    return {
        ...('everyMs' in spec && spec.everyMs !== undefined
            ? { every: spec.everyMs }
            : { pattern: spec.cron, ...(spec.timezone ? { tz: spec.timezone } : {}) }),
        ...(spec.startDate ? { startDate: spec.startDate } : {}),
        ...(spec.endDate ? { endDate: spec.endDate } : {}),
        ...(spec.limit === undefined ? {} : { limit: spec.limit }),
    };
}

export function createQueueClient<const R extends ContractRouter>(
    router: R,
    options: ConnectionSettings,
) {
    assertSettings(options);
    const contracts = flatten(router);
    const runtimes = new Map<string, Runtime>();
    const controller = new AbortController();
    const operations = new Set<Promise<unknown>>();
    const streams = new Map<string, Set<ResultSubscription<unknown>>>();
    let starting: Promise<void> | undefined;
    let closing: Promise<void> | undefined;
    let closed = false;
    const start = (): Promise<void> => {
        if (closed) return Promise.reject(new ClosedError());
        if (starting) return starting;
        starting = (async () => {
            try {
                for (const contract of contracts) {
                    const queue = new Queue<WireInput, WireOutput>(contract.definition.name, {
                        connection: connectionFor(options, false),
                        prefix: options.prefix,
                    });
                    queue.on('error', (error) =>
                        (options.logger ?? console).error(
                            { error, queue: contract.definition.name },
                            'queue error',
                        ),
                    );
                    runtimes.set(contract.definition.name, {
                        queue,
                        hub: new EventHub(contract.definition.name, options),
                    });
                }
                await within(
                    Promise.all(
                        [...runtimes.values()].map((runtime) => runtime.queue.waitUntilReady()),
                    ),
                    options.startupTimeoutMs ?? 10_000,
                );
            } catch (error) {
                closed = true;
                controller.abort(error);
                try {
                    await closeAll([...runtimes.values()].map((runtime) => runtime.queue));
                } catch (cleanup) {
                    throw new AggregateError([error, cleanup], 'Client startup and cleanup failed');
                }
                throw error;
            }
        })();
        return starting;
    };
    const run = <T>(contract: AnyContract, work: (runtime: Runtime) => Promise<T>): Promise<T> => {
        if (closed) return Promise.reject(new ClosedError());
        const operation = (async () => {
            await start();
            controller.signal.throwIfAborted();
            return work(runtimes.get(contract.definition.name)!);
        })();
        operations.add(operation);
        void operation.finally(() => operations.delete(operation)).catch(() => {});
        return operation;
    };
    const close = (): Promise<void> => {
        if (closing) return closing;
        closed = true;
        controller.abort(new ClosedError());
        closing = (async () => {
            await starting?.catch(() => {});
            for (const group of streams.values()) for (const stream of group) stream.close();
            await Promise.allSettled(operations);
            await closeAll(
                [...runtimes.values()].flatMap((runtime) => [runtime.hub, runtime.queue]),
            );
            runtimes.clear();
        })();
        return closing;
    };
    const prepare = async (
        contract: AnyContract,
        input: unknown,
        schedulerId?: string,
    ): Promise<WireInput> => {
        // Snapshot before async validation, so caller mutation cannot change the queued value.
        const envelope = inputEnvelope(contract, input, schedulerId);
        const snapshot = readInput(contract, envelope);
        if (contract.definition.input)
            await parseSchema(contract.definition.input, snapshot, {
                phase: 'input',
                queue: contract.definition.name,
            });
        else if (snapshot !== undefined)
            throw new ValidationError(
                'input',
                [{ message: 'This job accepts no input' }],
                contract.definition.name,
            );
        // Preserve raw input. Workers validate and transform the original value per attempt.
        return envelope;
    };
    const handle = (contract: AnyContract, id: string): JobHandle<unknown> => {
        if (!id) throw new TypeError('Job id is required');
        const fetch = async (runtime: Runtime) => {
            const job = await runtime.queue.getJob(id);
            if (!job) throw new JobNotFoundError(contract.definition.name, id);
            return job;
        };
        return {
            id,
            queueName: contract.definition.name,
            status: () =>
                run(
                    contract,
                    async (runtime) => (await runtime.queue.getJob(id))?.getState() ?? 'unknown',
                ),
            result: () =>
                run(contract, async (runtime) => {
                    const job = await fetch(runtime);
                    const state = await job.getState();
                    if (state === 'failed')
                        throw failedError(
                            contract.definition.name,
                            id,
                            (await fetch(runtime)).failedReason,
                        );
                    if (state !== 'completed')
                        throw new JobNotFinishedError(contract.definition.name, id);
                    return readOutput(contract, (await fetch(runtime)).returnvalue);
                }),
            remove: () =>
                run(contract, async (runtime) => {
                    if (!(await runtime.queue.getJob(id))) return 'missing';
                    const removed = await runtime.queue.remove(id);
                    return removed ? 'removed' : 'active';
                }),
            retry: () =>
                run(contract, async (runtime) => {
                    const job = await fetch(runtime);
                    await job.retry('failed');
                }),
            wait: (waitOptions = {}) =>
                run(contract, async (runtime) => {
                    const timeoutMs = waitOptions.timeoutMs ?? 30_000;
                    validateTimeout(timeoutMs, 'timeoutMs');
                    const signal = waitOptions.signal
                        ? AbortSignal.any([controller.signal, waitOptions.signal])
                        : controller.signal;
                    signal.throwIfAborted();
                    await runtime.hub.start();
                    signal.throwIfAborted();
                    return new Promise<unknown>((resolve, reject) => {
                        let settled = false;
                        let detach = () => {};
                        let timer: ReturnType<typeof setTimeout> | undefined;
                        const finish = (error: unknown, value?: unknown) => {
                            if (settled) return;
                            settled = true;
                            detach();
                            clearTimeout(timer);
                            signal.removeEventListener('abort', abort);
                            if (error !== undefined) reject(error);
                            else resolve(value);
                        };
                        const abort = () => finish(signal.reason);
                        const result = (value: unknown) => {
                            try {
                                finish(undefined, readOutput(contract, value));
                            } catch (error) {
                                finish(error);
                            }
                        };
                        detach = runtime.hub.listen((event) => {
                            if (event.type === 'completed') result(event.value);
                            else if (event.type === 'failed')
                                finish(failedError(contract.definition.name, id, event.reason));
                            else finish(new JobCancelledError(contract.definition.name, id));
                        }, id);
                        signal.addEventListener('abort', abort, { once: true });
                        timer = setTimeout(
                            () =>
                                finish(
                                    new WaitTimeoutError(contract.definition.name, id, timeoutMs),
                                ),
                            timeoutMs,
                        );
                        if (signal.aborted) abort();
                        // Subscribe first, then inspect persisted state to close the completion race.
                        void (async () => {
                            const job = await runtime.queue.getJob(id);
                            if (!job)
                                return finish(new JobNotFoundError(contract.definition.name, id));
                            const state = await job.getState();
                            if (state === 'completed') result((await fetch(runtime)).returnvalue);
                            else if (state === 'failed')
                                finish(
                                    failedError(
                                        contract.definition.name,
                                        id,
                                        (await fetch(runtime)).failedReason,
                                    ),
                                );
                        })().catch((error) => finish(error));
                    });
                }),
        };
    };
    const scheduler = (contract: AnyContract, id: string): ScheduleHandle<unknown> => {
        if (!id) throw new TypeError('Scheduler id is required');
        const key = stableId(id);
        const streamKey = `${contract.definition.name}:${key}`;
        return {
            id,
            remove: () =>
                run(contract, async (runtime) => {
                    const removed = await runtime.queue.removeJobScheduler(key);
                    if (removed) {
                        for (const stream of streams.get(streamKey) ?? []) stream.close();
                        streams.delete(streamKey);
                    }
                    return removed;
                }),
            subscribe: (subscriptionOptions = {}) =>
                run(contract, async (runtime) => {
                    const capacity = subscriptionOptions.capacity ?? 100;
                    assertPositive(capacity, 'capacity');
                    const signal = subscriptionOptions.signal
                        ? AbortSignal.any([controller.signal, subscriptionOptions.signal])
                        : controller.signal;
                    signal.throwIfAborted();
                    await runtime.hub.start();
                    signal.throwIfAborted();
                    const group = streams.get(streamKey) ?? new Set<ResultSubscription<unknown>>();
                    streams.set(streamKey, group);
                    const stream = subscription<ScheduleEvent<unknown>>(
                        capacity,
                        (push) =>
                            runtime.hub.listen((event) => {
                                if (
                                    !event.jobId.startsWith(`repeat:${key}:`) ||
                                    event.type === 'removed'
                                )
                                    return;
                                if (event.type === 'failed')
                                    push({
                                        type: 'failed',
                                        jobId: event.jobId,
                                        error: failedError(
                                            contract.definition.name,
                                            event.jobId,
                                            event.reason,
                                        ),
                                    });
                                else {
                                    try {
                                        push({
                                            type: 'completed',
                                            jobId: event.jobId,
                                            result: readOutput(contract, event.value),
                                        });
                                    } catch (error) {
                                        push({
                                            type: 'failed',
                                            jobId: event.jobId,
                                            error: failedError(
                                                contract.definition.name,
                                                event.jobId,
                                                error instanceof Error
                                                    ? error.message
                                                    : String(error),
                                            ),
                                        });
                                    }
                                }
                            }),
                        signal,
                        () => {
                            group.delete(stream);
                            if (!group.size) streams.delete(streamKey);
                        },
                    );
                    group.add(stream);
                    return stream;
                }),
        };
    };
    const queueHandle = (contract: AnyContract): QueueHandle<unknown, unknown> => ({
        name: contract.definition.name,
        publish: async (input, publishOpts) => {
            const opts = publishOptions(contract, publishOpts);
            const data = await prepare(contract, input);
            return run(contract, async (runtime) => {
                const job = await runtime.queue.add(contract.definition.name, data, opts);
                return handle(contract, String(job.id));
            });
        },
        publishBulk: async (items) => {
            const jobs = await Promise.all(
                items.map(async (item) => {
                    const opts = publishOptions(contract, item.options);
                    return {
                        name: contract.definition.name,
                        data: await prepare(contract, item.input),
                        opts,
                    };
                }),
            );
            return run(contract, async (runtime) => {
                if (!jobs.length) return [];
                return (await runtime.queue.addBulk(jobs)).map((job) =>
                    handle(contract, String(job.id)),
                );
            });
        },
        schedule: async (input, when, publishOpts) => {
            if (
                (when.at !== undefined) === (when.delayMs !== undefined) ||
                (when.at !== undefined && !(when.at instanceof Date))
            )
                throw new TypeError('Supply exactly one valid at Date or delayMs');
            const delay = when.at !== undefined ? when.at.getTime() - Date.now() : when.delayMs;
            if (
                delay === undefined ||
                !Number.isFinite(delay) ||
                (when.at === undefined && delay < 0)
            )
                return Promise.reject(
                    new TypeError('Supply a valid at Date or nonnegative delayMs'),
                );
            return queueHandle(contract).publish(input, {
                ...publishOpts,
                delay: Math.max(0, delay),
            });
        },
        get: (id) => handle(contract, id),
        counts: () =>
            run(contract, (runtime) =>
                runtime.queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed'),
            ),
        pause: () => run(contract, (runtime) => runtime.queue.pause()),
        resume: () => run(contract, (runtime) => runtime.queue.resume()),
        setGlobalConcurrency: (concurrency) =>
            run(contract, async (runtime) => {
                assertPositive(concurrency, 'concurrency');
                await runtime.queue.setGlobalConcurrency(concurrency);
            }),
        removeGlobalConcurrency: () =>
            run(contract, async (runtime) => {
                await runtime.queue.removeGlobalConcurrency();
            }),
        raw: () => run(contract, async (runtime) => runtime.queue),
        schedules: {
            get: (id) => scheduler(contract, id),
            upsert: async (id, spec, input, publishOpts) => {
                const result = scheduler(contract, id);
                const repeat = repeatOptions(spec);
                const opts = publishOptions(contract, publishOpts);
                const data = await prepare(contract, input, id);
                return run(contract, async (runtime) => {
                    await runtime.queue.upsertJobScheduler(stableId(id), repeat, {
                        name: contract.definition.name,
                        data,
                        opts,
                    });
                    return result;
                });
            },
            list: (start = 0, end = 99) =>
                run(contract, async (runtime) =>
                    (await runtime.queue.getJobSchedulers(start, end, true)).map((item) => ({
                        id: item.template?.data?.schedulerId ?? item.key,
                        next: item.next,
                        everyMs: item.every,
                        cron: item.pattern,
                        timezone: item.tz,
                        iterationCount: item.iterationCount,
                    })),
                ),
        },
    });
    const map = (node: ContractRouter): Record<string, unknown> => {
        const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
        for (const [key, value] of Object.entries(node))
            result[key] = isContract(value) ? queueHandle(value) : map(value);
        return Object.freeze(result);
    };
    return { jobs: map(router) as QueueClientJobs<R>, start, close, [Symbol.asyncDispose]: close };
}
