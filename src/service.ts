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
import type { Logger, ToreroQueues, QueueInput, QueueOutput } from './core/types.js';
import type { z } from 'zod';

const DEFAULT_AWAIT_TIMEOUT_FALLBACK_MS = 60_000;

type RegistryValue = QueueDefinition<unknown, unknown> & { __brand?: 'QueueDefinition' };

/** Options for initializing a QueueService instance. */
export type QueueServiceOpts = {
    connection: IORedisClient;
    runWorkers: boolean;
    runWithContext?: <T>(fn: () => Promise<T>) => Promise<T>;
    allowLateRegistration?: boolean;
    exclusiveWorkers?: boolean;
};

const internalsSymbol = Symbol();

/**
 * Manages queue definitions, materializes BullMQ objects, and exposes
 * high-level APIs to publish, schedule and repeat jobs.
 */
export class QueueService {
    /** Logger used internally; defaults to a no-op logger. */
    readonly logger: Logger;

    /** Used to namespace keys in Redis */
    readonly prefix: string;

    /** Materialized BullMQ runtimes by name. */
    private readonly runtimes = new Map<string, BullmqRuntime<any, any>>();

    /**
     * Internal API, do not create app code depending on this. This can and will have breaking
     * changes. Guarded behind a non exposed symbol so it is hard to use outside of this file
     */
    public [internalsSymbol] = {
        /** Registered queue definitions by name. */
        registry: new Map<string, RegistryValue>(),

        /** Get the materialized runtime for a queue or throw a helpful error. */
        getRuntime: <TInput, TOutput>(name: string): BullmqRuntime<TInput, TOutput> => {
            const runtime = this.runtimes.get(name) as BullmqRuntime<TInput, TOutput> | undefined;
            if (!runtime)
                throw new Error(`Queue '${name}' not initialized — call initQueues() first.`);
            return runtime;
        },
    } as const;

    private initialized = false;
    private connection: IORedisClient | undefined;
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
        InSchema extends z.ZodType,
        OutSchema extends z.ZodType | undefined = undefined,
    >(definition: {
        name: Name;
        inputSchema: InSchema;
        outputSchema?: OutSchema;
        process: (
            input: z.infer<InSchema>,
            context: { logger: Logger; queueName: Name },
        ) => Promise<OutSchema extends z.ZodType ? z.infer<OutSchema> : unknown>;
        defaults?: QueueDefinition<z.infer<InSchema>, any>['defaults'];
        hooks?: QueueDefinition<z.infer<InSchema>, any>['hooks'];
        init?: () => Promise<void> | void;
    }): ConcreteQueue<
        InferInput<InSchema>,
        OutSchema extends z.ZodType ? z.infer<OutSchema> : unknown
    > {
        const baseDefinition: QueueDefinition<
            InferInput<InSchema>,
            OutSchema extends z.ZodType ? z.infer<OutSchema> : unknown,
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

    /**
     * Return a ConcreteQueue handle bound to the named queue.
     * Intended for dynamic scenarios; prefer importing the exported handle.
     */
    getQueue<Name extends keyof ToreroQueues & string>(
        name: Name,
    ): ConcreteQueue<QueueInput<Name>, QueueOutput<Name>> {
        if (!this[internalsSymbol].registry.has(name)) {
            throw new Error(`Queue '${name}' not registered`);
        }
        return makeHandle<QueueInput<Name>, QueueOutput<Name>>(this, name);
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
        if (this[internalsSymbol].registry.has(definition.name)) {
            throw new Error(`Queue '${definition.name}' already registered`);
        }
        this[internalsSymbol].registry.set(definition.name, definition as unknown as RegistryValue);
        const handle: ConcreteQueue<TInput, TOutput> = makeHandle<TInput, TOutput>(
            this,
            definition.name,
        );

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
        this.runWithContext = options.runWithContext;
        this._allowLateRegistration = options.allowLateRegistration ?? false;
        this.exclusiveWorkers = options.exclusiveWorkers ?? true;
        this.runWorkers = options.runWorkers;

        for (const definition of this[internalsSymbol].registry.values()) {
            await this.materialize(definition.name, { runWorkers: options.runWorkers });
        }
        this.initialized = true;
    }

    /** Materialize BullMQ objects for a registered queue by name. */
    async materialize(name: string, options?: { runWorkers?: boolean }): Promise<void> {
        if (!this.connection) throw new Error('Connection not initialized');
        const definition = this[internalsSymbol].registry.get(name);
        if (!definition) throw new Error(`Queue '${name}' not registered`);

        const runtime = await createBullmqRuntime(definition, {
            connection: this.connection,
            prefix: this.prefix,
            runWorkers: options?.runWorkers ?? false,
            exclusiveWorkers: this.exclusiveWorkers,
            logger: this.logger,
            ...(this.runWithContext ? { runWithContext: this.runWithContext } : {}),
        });
        this.runtimes.set(name, runtime);
    }

    /** Attempt to spawn workers for runtimes that are missing one. */
    async reconcileWorkers(): Promise<void> {
        if (!this.exclusiveWorkers) return;
        if (!this.connection) return;
        if (!this.runWorkers) return;

        for (const [name, runtime] of this.runtimes.entries()) {
            if (runtime.worker) continue;
            const definition = this[internalsSymbol].registry.get(name);
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

// === File-scope helpers for enqueuing and scheduling ===

function resolveJobId<T>(
    service: QueueService,
    name: string,
    input: T,
    options?: PublishOptions<T>,
): string | undefined {
    const { prefix } = service;
    if (options?.jobId) return String(options.jobId);
    if (options?.idempotencyKey) {
        const key =
            typeof options.idempotencyKey === 'function'
                ? options.idempotencyKey(input)
                : options.idempotencyKey;
        return deriveJobId(prefix, name, key);
    }
    return undefined;
}

function getRuntimeOrThrow<TInput, TOutput>(
    service: QueueService,
    name: string,
): BullmqRuntime<TInput, TOutput> {
    const { getRuntime } = service[internalsSymbol];
    const runtime = getRuntime<TInput, TOutput>(name);
    if (!runtime) throw new Error(`Queue '${name}' not registered`);
    return runtime;
}

function getDefinitionOrThrow(service: QueueService, name: string): RegistryValue {
    const { registry } = service[internalsSymbol];
    const def = registry.get(name);
    if (!def) throw new Error(`Queue '${name}' not registered`);
    return def;
}

async function enqueueNow<TInput, TOutput>(
    service: QueueService,
    name: string,
    input: TInput,
    options?: PublishOptions<TInput>,
): Promise<AwaitResult<TOutput>> {
    const runtime = getRuntimeOrThrow<TInput, TOutput>(service, name);
    const definition = getDefinitionOrThrow(service, name);
    const parsed = definition.inputSchema.safeParse(input);
    if (!parsed.success) {
        const issues = parsed.error.issues
            .map((i) => {
                return `${i.path.join('.')}: ${i.message}`;
            })
            .join(', ');
        throw new Error(`Invalid input for queue '${name}': ${issues}`);
    }
    const jobId = resolveJobId(service, name, parsed.data as TInput, options);
    const data = { payload: parsed.data as TInput };
    const baseOptions = { ...toBullOptions(definition.defaults, options) } satisfies JobsOptions;
    const jobOptions = jobId ? { ...baseOptions, jobId } : baseOptions;
    const job = await runtime.queue.add('job', data, jobOptions);
    const awaitResult = async ({ timeoutMs }: { timeoutMs?: number } = {}): Promise<TOutput> => {
        const timeout = timeoutMs ?? options?.timeoutMs ?? definition.defaults?.timeoutMs ?? 60_000;
        return (await job.waitUntilFinished(runtime.events, timeout)) as TOutput;
    };
    const cancel = async () => {
        const { logger } = service;
        try {
            await job.remove();
        } catch (err) {
            logger.warn({ err, jobId: job.id, queue: name }, 'cancel failed');
        }
    };
    return { jobId: String(job.id), awaitResult, cancel };
}

async function enqueueIn<TInput, TOutput>(
    service: QueueService,
    name: string,
    params: { delayMs: number },
    input: TInput,
    options?: ScheduleOptions<TInput>,
): Promise<AwaitResult<TOutput>> {
    const runtime = getRuntimeOrThrow<TInput, TOutput>(service, name);
    const definition = getDefinitionOrThrow(service, name);
    const parsed = definition.inputSchema.safeParse(input);
    if (!parsed.success) {
        const issues = parsed.error.issues
            .map((i) => {
                return `${i.path.join('.')}: ${i.message}`;
            })
            .join(', ');
        throw new Error(`Invalid input for queue '${name}': ${issues}`);
    }
    const jobId = resolveJobId(service, name, parsed.data as TInput, options);
    const data = { payload: parsed.data as TInput };
    const baseOptions: JobsOptions = {
        ...toBullOptions(definition.defaults, options),
        delay: params.delayMs,
    };
    const jobOptions = jobId ? { ...baseOptions, jobId } : baseOptions;
    const job = await runtime.queue.add('job', data, jobOptions);
    const awaitResult = async ({ timeoutMs }: { timeoutMs?: number } = {}): Promise<TOutput> => {
        const timeout =
            timeoutMs ??
            options?.timeoutMs ??
            definition.defaults?.timeoutMs ??
            DEFAULT_AWAIT_TIMEOUT_FALLBACK_MS;
        return (await job.waitUntilFinished(runtime.events, timeout)) as TOutput;
    };
    const cancel = async () => {
        const { logger } = service;
        try {
            await job.remove();
        } catch (err) {
            logger.warn({ err, jobId: job.id, queue: name }, 'cancel failed');
        }
    };
    return { jobId: String(job.id), awaitResult, cancel };
}

function enqueueAt<TInput, TOutput>(
    service: QueueService,
    name: string,
    params: { when: Date },
    input: TInput,
    options?: ScheduleOptions<TInput>,
): Promise<AwaitResult<TOutput>> {
    const delay = Math.max(0, params.when.getTime() - Date.now());
    return enqueueIn(service, name, { delayMs: delay }, input, options);
}

function enqueueRepeat<TInput, TOutput>(
    service: QueueService,
    name: string,
    spec: RepeatSpec,
    input: TInput,
    options?: ScheduleOptions<TInput>,
): RepeatStream<TOutput> {
    const runtime = getRuntimeOrThrow<TInput, TOutput>(service, name);
    const definition = getDefinitionOrThrow(service, name);
    const { logger } = service;

    const parsed = definition.inputSchema.safeParse(input);
    if (!parsed.success) {
        const issues = parsed.error.issues
            .map((i) => {
                return `${i.path.join('.')}: ${i.message}`;
            })
            .join(', ');
        throw new Error(`Invalid input for queue '${name}': ${issues}`);
    }

    const repeat =
        spec.type === 'cron' ? { cron: spec.cron } : { every: spec.everyMs!, immediately: true };
    const data = { payload: parsed.data as TInput };
    const defaults = definition.defaults;
    const baseOptions = {
        ...toBullOptions(defaults, options),
        repeat: repeat,
    } satisfies JobsOptions;
    const jobPromise = runtime.queue.add('job', data, baseOptions);
    void jobPromise.catch((err) =>
        logger.warn({ err, queue: name }, 'failed to schedule repeatable job'),
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
            logger.warn({ err, queue: name }, 'repeat completion handling failed');
        }
    };
    runtime.events.on('completed', onCompleted);
    listeners.push(onCompleted);

    const asyncIterator: AsyncIterator<{ jobId: string; result: TOutput } | undefined> = {
        next: async () => {
            if (closed)
                return {
                    done: true,
                    value: undefined,
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
                    value: undefined,
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
                await runtime.queue.removeRepeatable('job', repeat);
            } catch (err) {
                logger.warn({ err, queue: name }, 'failed to remove repeatable');
            } finally {
                stream.close();
            }
        },
        close: () => {
            if (closed) return;
            closed = true;
            for (const listener of listeners) runtime.events.off('completed', listener);
            notify();
        },
    };
    return stream;
}

// Factory for a ConcreteQueue bound to a service and name
function makeHandle<TInput, TOutput>(
    service: QueueService,
    name: string,
): ConcreteQueue<TInput, TOutput> {
    return {
        name,
        publish: (input, options) => enqueueNow<TInput, TOutput>(service, name, input, options),
        scheduleAt: (params, input, options) =>
            enqueueAt<TInput, TOutput>(service, name, params, input, options),
        scheduleIn: (params, input, options) =>
            enqueueIn<TInput, TOutput>(service, name, params, input, options),
        scheduleRepeat: (spec, input, options) =>
            enqueueRepeat<TInput, TOutput>(service, name, spec, input, options),
    } satisfies ConcreteQueue<TInput, TOutput>;
}

export type {
    AwaitResult,
    PublishOptions,
    ScheduleOptions,
    RepeatSpec,
    RepeatStream,
} from './core/types.js';
