import { Queue, QueueEvents, Worker, type JobsOptions, type WorkerOptions } from 'bullmq';
import { Redis as IORedisClient } from 'ioredis';
import { randomUUID, createHash } from 'node:crypto';
import type { Logger } from '@prbux/logger';
import { createLogger } from '@prbux/logger';
import { z } from 'zod';
import { getEnv } from '../config/env.ts';

export type BackoffOptions = { type: 'fixed' | 'exponential'; delay: number };

export interface PublishOptions<TInput> {
    idempotencyKey?: string | ((input: TInput) => string);
    retries?: number;
    backoff?: BackoffOptions;
    timeoutMs?: number;
    priority?: number;
    removeOnComplete?: boolean | number;
    removeOnFail?: boolean | number;
    jobId?: string;
}

export type ScheduleOptions<TInput> = PublishOptions<TInput>;

export type AwaitResult<T> = {
    jobId: string;
    awaitResult: (opts?: { timeoutMs?: number }) => Promise<T>;
    cancel: () => Promise<void>;
};

export type RepeatSpec = { everyMs?: number; cron?: string };

export type RepeatYield<T> = { jobId: string; result: T };

export interface RepeatStream<T> extends AsyncIterable<RepeatYield<T>> {
    cancel(): Promise<void>;
    close(): void;
}

export interface QueueDefaults {
    concurrency?: number;
    attempts?: number;
    backoff?: BackoffOptions;
    timeoutMs?: number;
    removeOnComplete?: boolean | number;
    removeOnFail?: boolean | number;
    limiter?: WorkerOptions['limiter'];
}

export interface ProcessCtx {
    logger: Logger;
    queueName: string;
}

export interface QueueDefinition<TInput, TOutput> {
    name: string;
    inputSchema: z.ZodType;
    outputSchema?: z.ZodType;
    process: (input: TInput, ctx: ProcessCtx) => Promise<TOutput>;
    defaults?: QueueDefaults;
    hooks?: {
        onComplete?: (evt: { jobId: string; queue: string }) => void;
        onFailed?: (evt: { jobId: string; queue: string; failedReason: string }) => void;
    };
    init?: () => Promise<void> | void;
}

type EnqueuedData<T> = { payload: T; __meta?: { repeatToken?: string } };

export interface ConcreteQueue<TInput, TOutput> {
    name: string;
    publish(input: TInput, opts?: PublishOptions<TInput>): Promise<AwaitResult<TOutput>>;
    scheduleAt(when: Date, input: TInput, opts?: ScheduleOptions<TInput>): Promise<AwaitResult<TOutput>>;
    scheduleIn(delayMs: number, input: TInput, opts?: ScheduleOptions<TInput>): Promise<AwaitResult<TOutput>>;
    scheduleRepeat(spec: RepeatSpec, input: TInput, opts?: ScheduleOptions<TInput>): RepeatStream<TOutput>;
}

interface QueueRuntime<TInput, TOutput> {
    def: QueueDefinition<TInput, TOutput>;
    queue: Queue<EnqueuedData<TInput>, TOutput>;
    events: QueueEvents;
    worker?: Worker<EnqueuedData<TInput>, TOutput>;
    lock?: { key: string; id: string; heartbeat?: NodeJS.Timeout };
}

type RegistryValue = QueueDefinition<unknown, unknown> & { __brand?: 'QueueDefinition' };

function hashString(s: string): string {
    return createHash('sha1').update(s).digest('hex');
}

function toJobsOptions<T>(defaults: QueueDefaults | undefined, opts?: PublishOptions<T>): JobsOptions {
    const out: JobsOptions = {} as JobsOptions;
    const attempts = opts?.retries ?? defaults?.attempts;
    if (attempts !== undefined) out.attempts = attempts;
    const backoff = opts?.backoff ?? defaults?.backoff;
    if (backoff !== undefined) out.backoff = backoff;
    const priority = opts?.priority;
    if (priority !== undefined) out.priority = priority;
    const roc = opts?.removeOnComplete ?? defaults?.removeOnComplete ?? true;
    if (roc !== undefined) out.removeOnComplete = roc;
    const rof = opts?.removeOnFail ?? defaults?.removeOnFail ?? false;
    if (rof !== undefined) out.removeOnFail = rof;
    return out;
}

export class QueueService {
    private readonly logger: Logger;
    private readonly registry = new Map<string, RegistryValue>();
    private readonly runtimes = new Map<string, QueueRuntime<any, any>>();
    private inited = false;
    private connection: IORedisClient | undefined;
    private prefix = 'prbux';
    private withRequestContext: (<T>(fn: () => Promise<T>) => Promise<T>) | undefined;
    private allowLateRegistration = false;
    private exclusiveWorkers = true;

    constructor(logger?: Logger) {
        this.logger = logger ?? createLogger();
    }

    register<TInput, TOutput>(def: QueueDefinition<TInput, TOutput>): ConcreteQueue<TInput, TOutput> {
        if (this.inited && !this.allowLateRegistration) {
            throw new Error(
                `Queue '${def.name}' registered after initQueues; import earlier or enable allowLateRegistration.`,
            );
        }
        if (this.registry.has(def.name)) {
            throw new Error(`Queue '${def.name}' already registered`);
        }
        this.registry.set(def.name, def as RegistryValue);
        const handle: ConcreteQueue<TInput, TOutput> = {
            name: def.name,
            publish: async (input, opts) => this.publish(def.name, input, opts),
            scheduleAt: async (when, input, opts) => this.scheduleAt(def.name, when, input, opts),
            scheduleIn: async (delayMs, input, opts) => this.scheduleIn(def.name, delayMs, input, opts),
            scheduleRepeat: (spec, input, opts) => this.scheduleRepeat(def.name, spec, input, opts),
        };

        if (this.inited && this.allowLateRegistration) {
            void this.materialize(def as any).catch(err => {
                this.logger.error({ err, queue: def.name }, 'late registration materialize failed');
            });
        }
        return handle;
    }

    async initQueues(opts: {
        runWorkers: boolean;
        withRequestContext?: <T>(fn: () => Promise<T>) => Promise<T>;
        exclusiveWorkers?: boolean;
        allowLateRegistration?: boolean;
        prefix?: string;
    }): Promise<void> {
        if (this.inited) return;
        const env = getEnv();
        this.connection = new IORedisClient(env.REDIS_URL);
        this.prefix = opts.prefix ?? 'prbux';
        this.withRequestContext = opts.withRequestContext;
        this.allowLateRegistration = opts.allowLateRegistration ?? false;
        this.exclusiveWorkers = opts.exclusiveWorkers ?? true;

        // Materialize all registered queues
        for (const def of this.registry.values()) {
            await this.materialize(def as any, { runWorkers: opts.runWorkers });
        }
        this.inited = true;
    }

    private getRuntime<TInput, TOutput>(name: string): QueueRuntime<TInput, TOutput> {
        const rt = this.runtimes.get(name) as QueueRuntime<TInput, TOutput> | undefined;
        if (!rt) throw new Error(`Queue '${name}' not initialized. Call queueService.initQueues() first.`);
        return rt;
    }

    private async materialize<TInput, TOutput>(
        def: QueueDefinition<TInput, TOutput>,
        opts?: { runWorkers?: boolean },
    ): Promise<void> {
        if (!this.connection) throw new Error('Connection not initialized');
        const queue = new Queue<EnqueuedData<TInput>, TOutput>(def.name, {
            connection: this.connection,
            prefix: this.prefix,
        });
        const events = new QueueEvents(def.name, { connection: this.connection, prefix: this.prefix });

        const rt: QueueRuntime<TInput, TOutput> = { def, queue, events };
        this.runtimes.set(def.name, rt);

        // Hooks wiring
        events.on('completed', ({ jobId }) => {
            this.logger.info({ jobId, queue: def.name }, 'job completed');
            def.hooks?.onComplete?.({ jobId, queue: def.name });
        });
        events.on('failed', ({ jobId, failedReason }) => {
            this.logger.error({ jobId, queue: def.name, failedReason }, 'job failed');
            def.hooks?.onFailed?.({ jobId, queue: def.name, failedReason });
        });

        // Optional per-queue async init
        if (def.init) await def.init();

        // Worker creation
        if (opts?.runWorkers) {
            let mayRunWorker = true;
            if (this.exclusiveWorkers) {
                mayRunWorker = await this.acquireWorkerLock(def.name, rt);
            }
            if (mayRunWorker) {
                const concurrency = def.defaults?.concurrency ?? 5;
                const workerOpts: WorkerOptions = { connection: this.connection!, concurrency };
                if (def.defaults?.limiter) {
                    workerOpts.limiter = def.defaults.limiter;
                }
                const worker = new Worker<EnqueuedData<TInput>, TOutput>(
                    def.name,
                    async job => {
                        const ctx: ProcessCtx = { logger: this.logger, queueName: def.name };
                        const payload = (job.data?.payload ?? undefined) as TInput;
                        const parsedIn = def.inputSchema.safeParse(payload);
                        if (!parsedIn.success) {
                            throw new Error(
                                `Invalid input for queue '${def.name}': ${parsedIn.error.issues
                                    .map(i => `${i.path.join('.')}: ${i.message}`)
                                    .join(', ')}`,
                            );
                        }
                        const exec = async () => {
                            const res = await def.process(parsedIn.data as TInput, ctx);
                            return res;
                        };
                        const run = this.withRequestContext ? () => this.withRequestContext!(exec) : exec;
                        const timeoutMs = def.defaults?.timeoutMs;
                        const runWithTimeout = async () => {
                            if (!timeoutMs || timeoutMs <= 0) return await run();
                            return await Promise.race([
                                run(),
                                new Promise<never>((_, rej) => setTimeout(() => rej(new Error('job timed out')), timeoutMs)),
                            ]);
                        };
                        const out = await runWithTimeout();
                        if (def.outputSchema) {
                            const parsedOut = def.outputSchema.safeParse(out);
                            if (!parsedOut.success) {
                                throw new Error(
                                    `Invalid output from queue '${def.name}': ${parsedOut.error.issues
                                        .map(i => `${i.path.join('.')}: ${i.message}`)
                                        .join(', ')}`,
                                );
                            }
                            return parsedOut.data as TOutput;
                        }
                        return out;
                    },
                    workerOpts,
                );
                rt.worker = worker;
            }
        }
    }

    private async acquireWorkerLock<TInput, TOutput>(name: string, rt: QueueRuntime<TInput, TOutput>): Promise<boolean> {
        if (!this.connection) return false;
        const key = `${this.prefix}:queues:${name}:worker-lock`;
        const id = randomUUID();
        const ttlMs = 30_000;
        const acquired = await this.connection.set(key, id, 'PX', ttlMs, 'NX');
        if (acquired !== 'OK') return false;

        const heartbeat = setInterval(async () => {
            try {
                const current = await this.connection!.get(key);
                if (current === id) {
                    await this.connection!.pexpire(key, ttlMs);
                }
            } catch (err) {
                this.logger.warn({ err, queue: name }, 'lock heartbeat failed');
            }
        }, Math.floor(ttlMs / 3));
        rt.lock = { key, id, heartbeat };
        return true;
    }

    private resolveJobId<T>(name: string, input: T, opts?: PublishOptions<T>): string | undefined {
        if (opts?.jobId) return String(opts.jobId);
        if (opts?.idempotencyKey) {
            const key = typeof opts.idempotencyKey === 'function' ? opts.idempotencyKey(input) : opts.idempotencyKey;
            return `${this.prefix}:${name}:${hashString(key)}`;
        }
        return undefined;
    }

    async publish<TInput, TOutput>(
        name: string,
        input: TInput,
        opts?: PublishOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const rt = this.getRuntime<TInput, TOutput>(name);
        const parsed = rt.def.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        const jobId = this.resolveJobId(name, parsed.data as TInput, opts);
        const data: EnqueuedData<TInput> = { payload: parsed.data as TInput };
        const baseOpts: JobsOptions = { ...toJobsOptions(rt.def.defaults, opts) };
        const jobOptions: JobsOptions = jobId ? { ...baseOpts, jobId } : baseOpts;
        const job = await rt.queue.add('job', data, jobOptions);
        const awaitResult = async ({ timeoutMs }: { timeoutMs?: number } = {}): Promise<TOutput> => {
            const t = timeoutMs ?? (opts?.timeoutMs ?? rt.def.defaults?.timeoutMs ?? 60_000);
            return (await job.waitUntilFinished(rt.events, t)) as TOutput;
        };
        const cancel = async () => {
            try {
                await job.remove();
            } catch (err) {
                this.logger.warn({ err, jobId: job.id, queue: name }, 'cancel failed');
            }
        };
        return { jobId: String(job.id), awaitResult, cancel };
    }

    async scheduleAt<TInput, TOutput>(
        name: string,
        when: Date,
        input: TInput,
        opts?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const delay = Math.max(0, when.getTime() - Date.now());
        return this.scheduleIn(name, delay, input, opts);
    }

    async scheduleIn<TInput, TOutput>(
        name: string,
        delayMs: number,
        input: TInput,
        opts?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const rt = this.getRuntime<TInput, TOutput>(name);
        const parsed = rt.def.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        const jobId = this.resolveJobId(name, parsed.data as TInput, opts);
        const data: EnqueuedData<TInput> = { payload: parsed.data as TInput };
        const baseOpts: JobsOptions = { ...toJobsOptions(rt.def.defaults, opts), delay: delayMs } as JobsOptions;
        const jobOptions: JobsOptions = jobId ? { ...baseOpts, jobId } : baseOpts;
        const job = await rt.queue.add('job', data, jobOptions);
        const awaitResult = async ({ timeoutMs }: { timeoutMs?: number } = {}): Promise<TOutput> => {
            const t = timeoutMs ?? (opts?.timeoutMs ?? rt.def.defaults?.timeoutMs ?? 60_000);
            return (await job.waitUntilFinished(rt.events, t)) as TOutput;
        };
        const cancel = async () => {
            try {
                await job.remove();
            } catch (err) {
                this.logger.warn({ err, jobId: job.id, queue: name }, 'cancel failed');
            }
        };
        return { jobId: String(job.id), awaitResult, cancel };
    }

    scheduleRepeat<TInput, TOutput>(
        name: string,
        spec: RepeatSpec,
        input: TInput,
        opts?: ScheduleOptions<TInput>,
    ): RepeatStream<TOutput> {
        const rt = this.getRuntime<TInput, TOutput>(name);
        const parsed = rt.def.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        if (!spec.cron && !spec.everyMs) throw new Error('RepeatSpec must have either cron or everyMs');
        if (spec.cron && spec.everyMs) throw new Error('RepeatSpec must not have both cron and everyMs');

        const token = randomUUID();
        const repeatJobId = this.resolveJobId(name, parsed.data as TInput, opts) ?? token;
        const data: EnqueuedData<TInput> = { payload: parsed.data as TInput, __meta: { repeatToken: token } };
        const repeat = spec.cron ? { cron: spec.cron, jobId: repeatJobId } : { every: spec.everyMs!, jobId: repeatJobId };
        const jobOptions: JobsOptions = { ...toJobsOptions(rt.def.defaults, opts), repeat } as JobsOptions;

        // Fire-and-forget schedule; we are creating a stream for future completions
        void rt.queue.add('job', data, jobOptions).catch(err => {
            this.logger.error({ err, queue: name }, 'failed to add repeatable job');
        });

        // Create async iterable stream
        const listeners: Array<(...args: any[]) => void> = [];
        let closed = false;
        const pending: Array<RepeatYield<TOutput>> = [];
        let resolveNotify: (() => void) | undefined;
        const notify = () => {
            if (resolveNotify) {
                const fn = resolveNotify;
                resolveNotify = undefined;
                fn();
            }
        };
        const onCompleted = async ({ jobId }: { jobId: string }) => {
            if (closed) return;
            try {
                const job = await rt.queue.getJob(jobId);
                if (!job) return;
                const meta = (job.data as EnqueuedData<TInput> | undefined)?.__meta;
                if (meta?.repeatToken !== token) return;
                const result = (await job.getState()) === 'completed' ? ((await job.returnvalue) as TOutput) : undefined;
                if (result !== undefined) {
                    pending.push({ jobId: String(jobId), result });
                    notify();
                }
            } catch (err) {
                this.logger.warn({ err, queue: name }, 'repeat completion handling failed');
            }
        };
        rt.events.on('completed', onCompleted);
        listeners.push(onCompleted);

        const asyncIterator: AsyncIterator<RepeatYield<TOutput>> = {
            next: async () => {
                if (closed) return { done: true, value: undefined as any };
                if (pending.length > 0) {
                    return { done: false, value: pending.shift()! };
                }
                await new Promise<void>(res => {
                    resolveNotify = res;
                });
                if (closed) return { done: true, value: undefined as any };
                return { done: false, value: pending.shift()! };
            },
        };

        const stream: RepeatStream<TOutput> = {
            [Symbol.asyncIterator]() {
                return asyncIterator;
            },
            cancel: async () => {
                try {
                    await rt.queue.removeRepeatable('job', repeat as any);
                } catch (err) {
                    this.logger.warn({ err, queue: name }, 'failed to remove repeatable');
                } finally {
                    stream.close();
                }
            },
            close: () => {
                if (closed) return;
                closed = true;
                // detach listeners and wake iterator
                for (const l of listeners) {
                    rt.events.off('completed', l as any);
                }
                notify();
            },
        };
        return stream;
    }

    async shutdown(): Promise<void> {
        const closers: Array<Promise<unknown>> = [];
        for (const rt of this.runtimes.values()) {
            if (rt.worker) {
                closers.push(rt.worker.close());
            }
            closers.push(rt.events.close());
            closers.push(rt.queue.close());
            if (rt.lock) {
                if (rt.lock.heartbeat) clearInterval(rt.lock.heartbeat);
                // best effort lock release
                try {
                    const cur = await this.connection!.get(rt.lock.key);
                    if (cur === rt.lock.id) await this.connection!.del(rt.lock.key);
                } catch {
                    // ignore
                }
            }
        }
        await Promise.allSettled(closers);
        if (this.connection) {
            await this.connection.quit();
        }
        this.inited = false;
        this.runtimes.clear();
    }
}

export const queueService = new QueueService();
