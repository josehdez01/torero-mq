import { Queue, QueueEvents, Worker, type WorkerOptions, type Job } from 'bullmq';
import type { Redis as IORedisClient } from 'ioredis';
import { randomUUID } from 'node:crypto';
import type { QueueDefinition, ProcessCtx, Logger } from '../core/types.js';

/**
 * Payload wrapper for enqueued jobs. We keep room for future metadata.
 */
type EnqueuedData<T> = { payload: T; __meta?: { repeatToken?: string } };

/**
 * Concrete BullMQ objects backing a queue plus a shutdown helper.
 */
export interface BullmqRuntime<TInput, TOutput> {
    queue: Queue<EnqueuedData<TInput>, TOutput>;
    events: QueueEvents;
    worker?: Worker<EnqueuedData<TInput>, TOutput>;
    lock?: { key: string; id: string; heartbeat?: NodeJS.Timeout };
    shutdown: () => Promise<void>;
}

/** Create-time options for a runtime. */
export interface CreateRuntimeOpts {
    connection: IORedisClient;
    prefix: string;
    runWorkers: boolean;
    exclusiveWorkers: boolean;
    runWithContext?: <T>(fn: () => Promise<T>) => Promise<T>;
    logger: Logger;
}

/** Options used when spawning a worker for an existing runtime. */
export interface WorkerSpawnOpts {
    connection: IORedisClient;
    prefix: string;
    exclusiveWorkers: boolean;
    runWithContext?: <T>(fn: () => Promise<T>) => Promise<T>;
    logger: Logger;
}

/**
 * Instantiate BullMQ objects and wire hooks for the provided queue definition.
 */
export async function createBullmqRuntime<TInput, TOutput, const Name extends string>(
    definition: QueueDefinition<TInput, TOutput, Name>,
    options: CreateRuntimeOpts,
): Promise<BullmqRuntime<TInput, TOutput>> {
    const { connection, prefix, runWorkers, exclusiveWorkers, runWithContext, logger } = options;

    const queue = new Queue<EnqueuedData<TInput>, TOutput>(definition.name, { connection, prefix });
    const events = new QueueEvents(definition.name, { connection, prefix });

    // Hooks wiring
    events.on('completed', ({ jobId }) => {
        logger.info({ jobId, queue: definition.name }, 'job completed');
        definition.hooks?.onComplete?.({ jobId, queue: definition.name });
    });
    events.on('failed', ({ jobId, failedReason }) => {
        logger.error({ jobId, queue: definition.name, failedReason }, 'job failed');
        definition.hooks?.onFailed?.({ jobId, queue: definition.name, failedReason });
    });

    // Optional per-queue async init
    if (definition.init) await definition.init();

    const runtime: BullmqRuntime<TInput, TOutput> = {
        queue,
        events,
        shutdown: async () => {
            const closers: Array<Promise<unknown>> = [];

            if (runtime.worker) closers.push(runtime.worker.close());
            closers.push(events.close());
            closers.push(queue.close());

            if (runtime.lock) {
                if (runtime.lock.heartbeat) clearInterval(runtime.lock.heartbeat);
                try {
                    const current = await connection.get(runtime.lock.key);
                    if (current === runtime.lock.id) await connection.del(runtime.lock.key);
                } catch {
                    // best-effort unlock
                }
            }

            await Promise.allSettled(closers);
        },
    };

    if (runWorkers) {
        await spawnWorker(definition, runtime, {
            connection,
            prefix,
            exclusiveWorkers,
            logger,
            ...(runWithContext ? { runWithContext } : {}),
        });
    }

    return runtime;
}

/**
 * Acquire a distributed lock to ensure only a single worker per queue in the cluster.
 */
async function acquireWorkerLock<TInput, TOutput>(
    queueName: string,
    connection: IORedisClient,
    prefix: string,
    runtime: BullmqRuntime<TInput, TOutput>,
    logger: Logger,
): Promise<boolean> {
    const lockKey = `${prefix}:queues:${queueName}:worker-lock`;
    const lockId = randomUUID();
    const ttlMs = 30_000;
    const acquired = await connection.set(lockKey, lockId, 'PX', ttlMs, 'NX');
    if (acquired !== 'OK') return false;

    const heartbeat = setInterval(
        async () => {
            try {
                const current = await connection.get(lockKey);
                if (current === lockId) {
                    await connection.pexpire(lockKey, ttlMs);
                }
            } catch (err) {
                logger.warn({ err, queue: queueName }, 'lock heartbeat failed');
            }
        },
        Math.floor(ttlMs / 3),
    );

    runtime.lock = { key: lockKey, id: lockId, heartbeat };
    return true;
}

/**
 * Spawn a BullMQ worker to process jobs for the provided definition and runtime.
 */
export async function spawnWorker<TInput, TOutput, const Name extends string>(
    definition: QueueDefinition<TInput, TOutput, Name>,
    runtime: BullmqRuntime<TInput, TOutput>,
    options: WorkerSpawnOpts,
): Promise<boolean> {
    const { connection, prefix, exclusiveWorkers, runWithContext, logger } = options;

    if (runtime.worker) return true;

    let mayRunWorker = true;
    if (exclusiveWorkers) {
        mayRunWorker = await acquireWorkerLock(
            definition.name,
            connection,
            prefix,
            runtime,
            logger,
        );
    }
    if (!mayRunWorker) return false;

    const concurrency = definition.defaults?.concurrency ?? 5;
    const workerOptions: WorkerOptions = { connection, concurrency };
    const limiter = definition.defaults?.limiter as WorkerOptions['limiter'] | undefined;
    if (limiter !== undefined) workerOptions.limiter = limiter;

    const worker = new Worker<EnqueuedData<TInput>, TOutput>(
        definition.name,
        async (job: Job<EnqueuedData<TInput>, TOutput>) => {
            const context: ProcessCtx<Name> = {
                logger,
                queueName: definition.name,
            } as ProcessCtx<Name>;

            // Validate input early to fail fast and with a clear message.
            const payload = (job.data?.payload ?? undefined) as TInput;
            const validation = definition.inputSchema.safeParse(payload);
            if (!validation.success) {
                throw new Error(
                    `Invalid input for queue '${definition.name}': ${validation.error.issues
                        .map((issue) => `${issue.path.join('.')}: ${issue.message}`)
                        .join(', ')}`,
                );
            }

            const execute = async () =>
                await definition.process(validation.data as TInput, context);
            const run = runWithContext ? () => runWithContext(execute) : execute;

            // Respect queue-level timeout if provided.
            const timeoutMs = definition.defaults?.timeoutMs;
            const runWithTimeout = async () => {
                if (!timeoutMs || timeoutMs <= 0) return await run();
                return await Promise.race([
                    run(),
                    new Promise<never>((_, reject) =>
                        setTimeout(() => reject(new Error('job timed out')), timeoutMs),
                    ),
                ]);
            };

            const result = await runWithTimeout();
            if (definition.outputSchema) {
                const parsedOut = definition.outputSchema.safeParse(result);
                if (!parsedOut.success) {
                    throw new Error(
                        `Invalid output from queue '${definition.name}': ${parsedOut.error.issues
                            .map((issue) => `${issue.path.join('.')}: ${issue.message}`)
                            .join(', ')}`,
                    );
                }
                return parsedOut.data as TOutput;
            }
            return result;
        },
        workerOptions,
    );

    runtime.worker = worker;
    return true;
}
