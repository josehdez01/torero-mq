import type { Redis as IORedisClient } from 'ioredis';
import type {
    AwaitResult,
    ConcreteQueue,
    InferInput,
    PublishOptions,
    QueueDefinition,
    RepeatSpec,
    RepeatStream,
    ScheduleOptions,
} from './core/types.js';
import { toBullOptions, deriveJobId } from './helpers.js';
import { createBullmqRuntime, type BullmqRuntime, spawnWorker } from './runtime/bullmq-runtime.js';
import type { JobsOptions } from 'bullmq';
import type { Logger } from './core/types.js';
import type { ZodTypeAny, z } from 'zod';

type RegistryValue = QueueDefinition<unknown, unknown> & { __brand?: 'QueueDefinition' };

/** Options for initializing a QueueService instance. */
export type QueueServiceOpts = {
    connection: IORedisClient;
    runWorkers: boolean;
    prefix?: string;
    runWithContext?: <T>(fn: () => Promise<T>) => Promise<T>;
    allowLateRegistration?: boolean;
    exclusiveWorkers?: boolean;
};

/**
 * Manages queue definitions, materializes BullMQ objects, and exposes
 * high-level APIs to publish, schedule and repeat jobs.
 */
export class QueueService {
    /** Logger used internally; defaults to a no-op logger. */
    private readonly logger: Logger;
    /** Registered queue definitions by name. */
    private readonly registry = new Map<string, RegistryValue>();
    /** Materialized BullMQ runtimes by name. */
    private readonly runtimes = new Map<string, BullmqRuntime<any, any>>();

    private initialized = false;
    private connection: IORedisClient | undefined;
    private prefix: string;
    private runWithContext: (<T>(fn: () => Promise<T>) => Promise<T>) | undefined;
    private _allowLateRegistration = false;
    private exclusiveWorkers = true;
    private runWorkers = false;

    constructor(prefix: string, logger?: Logger) {
        this.prefix = prefix;
        this.logger = logger ?? { info: () => {}, warn: () => {}, error: () => {} };
    }

    /**
     * Define and register a queue in a type-safe way using Zod schemas.
     */
    defineQueue<
        const Name extends string,
        InSchema extends ZodTypeAny,
        OutSchema extends ZodTypeAny | undefined = undefined,
    >(definition: {
        name: Name;
        inputSchema: InSchema;
        outputSchema?: OutSchema;
        process: (
            input: z.infer<InSchema>,
            context: { logger: Logger; queueName: Name },
        ) => Promise<OutSchema extends ZodTypeAny ? z.infer<OutSchema> : unknown>;
        defaults?: QueueDefinition<z.infer<InSchema>, any>['defaults'];
        hooks?: QueueDefinition<z.infer<InSchema>, any>['hooks'];
        init?: () => Promise<void> | void;
    }): ConcreteQueue<
        InferInput<InSchema>,
        OutSchema extends ZodTypeAny ? z.infer<OutSchema> : unknown
    > {
        const baseDefinition: QueueDefinition<
            InferInput<InSchema>,
            OutSchema extends ZodTypeAny ? z.infer<OutSchema> : unknown,
            Name
        > = {
            name: definition.name,
            inputSchema: definition.inputSchema,
            process: definition.process,
        };
        if (definition.defaults !== undefined) baseDefinition.defaults = definition.defaults;
        if (definition.hooks !== undefined) baseDefinition.hooks = definition.hooks;
        if (definition.outputSchema !== undefined)
            baseDefinition.outputSchema = definition.outputSchema;
        if (definition.init !== undefined) baseDefinition.init = definition.init;
        return this.register(baseDefinition);
    }

    /** Allow queues to be registered after initQueues() has been called. */
    allowLateRegistration(): void {
        this._allowLateRegistration = true;
    }

    /** Register a queue definition and return a public handle for enqueuing. */
    register<TInput, TOutput, Name extends string = string>(
        definition: QueueDefinition<TInput, TOutput, Name>,
    ): ConcreteQueue<TInput, TOutput> {
        if (this.initialized && !this._allowLateRegistration) {
            throw new Error(
                `Queue '${definition.name}' was registered after initQueues(). Import earlier, enable allowLateRegistration(), or call materialize('${definition.name}') manually.`,
            );
        }
        if (this.registry.has(definition.name)) {
            throw new Error(`Queue '${definition.name}' already registered`);
        }
        this.registry.set(definition.name, definition as unknown as RegistryValue);
        const handle: ConcreteQueue<TInput, TOutput> = {
            name: definition.name,
            publish: async (input, options) => this.publish(definition.name, input, options),
            scheduleAt: async (params, input, options) =>
                this.scheduleAt(definition.name, params, input, options),
            scheduleIn: async (params, input, options) =>
                this.scheduleIn(definition.name, params, input, options),
            scheduleRepeat: (spec, input, options) =>
                this.scheduleRepeat(definition.name, spec, input, options),
        };

        if (this.initialized && this._allowLateRegistration) {
            void this.materialize(definition.name, { runWorkers: this.runWorkers }).catch((err) => {
                this.logger.error(
                    { err, queue: definition.name },
                    'late registration materialize failed',
                );
            });
        }
        return handle;
    }

    /** Initialize connections and materialize already-registered queues. */
    async initQueues(options: QueueServiceOpts): Promise<void> {
        if (this.initialized) return;

        this.connection = options.connection;
        this.prefix = options.prefix ?? 'queues';
        this.runWithContext = options.runWithContext;
        this._allowLateRegistration = options.allowLateRegistration ?? false;
        this.exclusiveWorkers = options.exclusiveWorkers ?? true;
        this.runWorkers = options.runWorkers;

        for (const definition of this.registry.values()) {
            await this.materialize(definition.name, { runWorkers: options.runWorkers });
        }
        this.initialized = true;
    }

    /** Get the materialized runtime for a queue or throw a helpful error. */
    private getRuntime<TInput, TOutput>(name: string): BullmqRuntime<TInput, TOutput> {
        const runtime = this.runtimes.get(name) as BullmqRuntime<TInput, TOutput> | undefined;
        if (!runtime) throw new Error(`Queue '${name}' not initialized — call initQueues() first.`);
        return runtime;
    }

    /** Materialize BullMQ objects for a registered queue by name. */
    async materialize(name: string, options?: { runWorkers?: boolean }): Promise<void> {
        if (!this.connection) throw new Error('Connection not initialized');
        const definition = this.registry.get(name) as RegistryValue | undefined;
        if (!definition) throw new Error(`Queue '${name}' not registered`);

        const runtime = await createBullmqRuntime(
            definition as unknown as QueueDefinition<unknown, unknown>,
            {
                connection: this.connection,
                prefix: this.prefix,
                runWorkers: options?.runWorkers ?? false,
                exclusiveWorkers: this.exclusiveWorkers,
                logger: this.logger,
                ...(this.runWithContext ? { runWithContext: this.runWithContext } : {}),
            },
        );
        this.runtimes.set(name, runtime);
    }

    /** Attempt to spawn workers for runtimes that are missing one. */
    async reconcileWorkers(): Promise<void> {
        if (!this.exclusiveWorkers) return;
        if (!this.connection) return;
        if (!this.runWorkers) return;

        for (const [name, runtime] of this.runtimes.entries()) {
            if (runtime.worker) continue;
            const definition = this.registry.get(name) as RegistryValue | undefined;
            if (!definition) continue;
            try {
                await spawnWorker(definition, runtime, {
                    connection: this.connection,
                    prefix: this.prefix,
                    exclusiveWorkers: this.exclusiveWorkers,
                    logger: this.logger,
                    ...(this.runWithContext ? { runWithContext: this.runWithContext } : {}),
                });
            } catch (err) {
                this.logger.warn({ err, queue: name }, 'reconcileWorkers spawn failed');
            }
        }
    }

    /** Compute the final BullMQ job id considering idempotency and overrides. */
    private resolveJobId<T>(
        name: string,
        input: T,
        options?: PublishOptions<T>,
    ): string | undefined {
        if (options?.jobId) return String(options.jobId);
        if (options?.idempotencyKey) {
            const key =
                typeof options.idempotencyKey === 'function'
                    ? options.idempotencyKey(input)
                    : options.idempotencyKey;
            return deriveJobId(this.prefix, name, key);
        }
        return undefined;
    }

    /** Enqueue a job immediately and return a handle to await the result. */
    async publish<TInput, TOutput>(
        name: string,
        input: TInput,
        options?: PublishOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const runtime = this.getRuntime<TInput, TOutput>(name);
        const definition = this.registry.get(name) as RegistryValue;
        const parsed = definition.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues
                .map((i: unknown) => {
                    const ii = i as { path: Array<string | number>; message: string };
                    return `${ii.path.join('.')}: ${ii.message}`;
                })
                .join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        const jobId = this.resolveJobId(name, parsed.data as TInput, options);
        const data = { payload: parsed.data as TInput };
        const baseOptions = { ...toBullOptions(definition.defaults, options) } as JobsOptions;
        const jobOptions = jobId ? { ...baseOptions, jobId } : baseOptions;
        const job = await runtime.queue.add(
            'job',
            data as unknown as { payload: TInput },
            jobOptions,
        );
        const awaitResult = async ({
            timeoutMs,
        }: { timeoutMs?: number } = {}): Promise<TOutput> => {
            const timeout =
                timeoutMs ?? options?.timeoutMs ?? definition.defaults?.timeoutMs ?? 60_000;
            return (await job.waitUntilFinished(runtime.events, timeout)) as TOutput;
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

    /** Schedule a job to run at a specific time. */
    async scheduleAt<TInput, TOutput>(
        name: string,
        params: { when: Date },
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const delay = Math.max(0, params.when.getTime() - Date.now());
        return this.scheduleIn(name, { delayMs: delay }, input, options);
    }

    /** Schedule a job to run after a delay in milliseconds. */
    async scheduleIn<TInput, TOutput>(
        name: string,
        params: { delayMs: number },
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>> {
        const runtime = this.getRuntime<TInput, TOutput>(name);
        const definition = this.registry.get(name) as RegistryValue;
        const parsed = definition.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues
                .map((i: unknown) => {
                    const ii = i as { path: Array<string | number>; message: string };
                    return `${ii.path.join('.')}: ${ii.message}`;
                })
                .join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        const jobId = this.resolveJobId(name, parsed.data as TInput, options);
        const data = { payload: parsed.data as TInput };
        const baseOptions: JobsOptions = {
            ...toBullOptions(definition.defaults, options),
            delay: params.delayMs,
        };
        const jobOptions = jobId ? { ...baseOptions, jobId } : baseOptions;
        const job = await runtime.queue.add(
            'job',
            data as unknown as { payload: TInput },
            jobOptions,
        );
        const awaitResult = async ({
            timeoutMs,
        }: { timeoutMs?: number } = {}): Promise<TOutput> => {
            const timeout =
                timeoutMs ?? options?.timeoutMs ?? definition.defaults?.timeoutMs ?? 60_000;
            return (await job.waitUntilFinished(runtime.events, timeout)) as TOutput;
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

    /** Create a repeatable job and return a stream of results for each run. */
    scheduleRepeat<TInput, TOutput>(
        name: string,
        spec: RepeatSpec,
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): RepeatStream<TOutput> {
        const runtime = this.getRuntime<TInput, TOutput>(name);
        const definition = this.registry.get(name);
        if (!definition) {
            throw new Error(`Queue ${name} is not registered`);
        }

        const parsed = definition.inputSchema.safeParse(input);
        if (!parsed.success) {
            const issues = parsed.error.issues
                .map((i: unknown) => {
                    const ii = i as { path: Array<string | number>; message: string };
                    return `${ii.path.join('.')}: ${ii.message}`;
                })
                .join(', ');
            throw new Error(`Invalid input for queue '${name}': ${issues}`);
        }
        if (!spec.cron && !spec.everyMs)
            throw new Error('RepeatSpec must have either cron or everyMs');
        if (spec.cron && spec.everyMs)
            throw new Error('RepeatSpec must not have both cron and everyMs');
        const repeat = spec.cron
            ? { cron: spec.cron }
            : { every: spec.everyMs!, immediately: true };
        const data = { payload: parsed.data as TInput };
        const defaults = definition.defaults;
        const baseOptions = {
            ...toBullOptions(defaults, options),
            repeat: repeat as unknown as JobsOptions['repeat'],
        } as JobsOptions;
        const jobPromise = runtime.queue.add('job', data, baseOptions);
        void jobPromise.catch((err) =>
            this.logger.warn({ err, queue: name }, 'failed to schedule repeatable job'),
        );

        let closed = false;
        const listeners: Array<(evt: { jobId: string }) => void> = [];
        const pending: Array<{ jobId: string; result: TOutput }> = [];
        let resolveNotify: (() => void) | undefined;
        const notify = () => {
            const resolver = resolveNotify;
            resolveNotify = undefined;
            if (resolver) resolver();
        };
        const onCompleted = async ({ jobId }: { jobId: string }) => {
            try {
                const job = await runtime.queue.getJob(jobId);
                if (!job) return;
                const result =
                    (await job.getState()) === 'completed'
                        ? ((await job.returnvalue) as TOutput)
                        : undefined;
                if (result !== undefined) {
                    pending.push({ jobId: String(jobId), result });
                    notify();
                }
            } catch (err) {
                this.logger.warn({ err, queue: name }, 'repeat completion handling failed');
            }
        };
        runtime.events.on('completed', onCompleted);
        listeners.push(onCompleted);

        const asyncIterator: AsyncIterator<{ jobId: string; result: TOutput }> = {
            next: async () => {
                if (closed)
                    return {
                        done: true,
                        value: undefined as unknown as { jobId: string; result: TOutput },
                    };
                if (pending.length > 0) {
                    return { done: false, value: pending.shift()! };
                }
                await new Promise<void>((res) => {
                    resolveNotify = res;
                });
                if (closed)
                    return {
                        done: true,
                        value: undefined as unknown as { jobId: string; result: TOutput },
                    };
                return { done: false, value: pending.shift()! };
            },
        };

        const stream: RepeatStream<TOutput> = {
            [Symbol.asyncIterator]() {
                return asyncIterator;
            },
            cancel: async () => {
                try {
                    await runtime.queue.removeRepeatable(
                        'job',
                        repeat as unknown as NonNullable<JobsOptions['repeat']>,
                    );
                } catch (err) {
                    this.logger.warn({ err, queue: name }, 'failed to remove repeatable');
                } finally {
                    stream.close();
                }
            },
            close: () => {
                if (closed) return;
                closed = true;
                for (const listener of listeners)
                    (
                        runtime.events as unknown as {
                            off: (
                                event: 'completed',
                                listener: (evt: { jobId: string }) => void,
                            ) => void;
                        }
                    ).off('completed', listener);
                notify();
            },
        };
        return stream;
    }

    /** Gracefully stop workers and close BullMQ connections held by this service. */
    async shutdown(): Promise<void> {
        const closers: Array<Promise<unknown>> = [];
        for (const runtime of this.runtimes.values()) {
            closers.push(runtime.shutdown());
        }
        await Promise.allSettled(closers);
        if (this.connection) {
            await this.connection.quit();
        }
        this.initialized = false;
        this.runtimes.clear();
    }
}

export type {
    AwaitResult,
    PublishOptions,
    ScheduleOptions,
    RepeatSpec,
    RepeatStream,
} from './core/types.js';
