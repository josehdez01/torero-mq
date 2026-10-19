import { Worker, UnrecoverableError, type Job } from 'bullmq';
import {
    parseSchema,
    type AnyImplementation,
    type HandlerArgs,
    type RequiredContext,
} from './definition.js';
import { CodecError, JobTimeoutError, ValidationError } from './errors.js';
import {
    assertJson,
    assertSettings,
    closeAll,
    ClosedError,
    connectionFor,
    failureReason,
    flatten,
    outputEnvelope,
    ProtocolError,
    readInput,
    within,
    type ConnectionSettings,
    type ImplementationRouter,
    type WireInput,
    type WireOutput,
} from './shared.js';

export type WorkerContextFactory<C> = (
    args: Omit<HandlerArgs<unknown, C>, 'input' | 'context'>,
) => C | Promise<C>;
type ContextOptions<R> =
    object extends RequiredContext<R>
        ? { context?: WorkerContextFactory<RequiredContext<R>> }
        : { context: WorkerContextFactory<RequiredContext<R>> };
export type WorkersOptions<R> = ConnectionSettings & ContextOptions<R>;

/** Workers are explicit resources: definitions never connect to Redis or start processing. */
export function createWorkers<const R extends ImplementationRouter>(
    router: R,
    options: WorkersOptions<R>,
) {
    assertSettings(options);
    const definitions = flatten(router).map((contract) => {
        if (!('execute' in contract) || typeof contract.execute !== 'function')
            throw new TypeError(`Missing handler for ${contract.definition.name}`);
        return contract as AnyImplementation;
    });
    const logger = options.logger ?? console;
    const workers: Worker<WireInput, WireOutput>[] = [];
    let starting: Promise<void> | undefined;
    let closing: Promise<void> | undefined;
    let closed = false;
    const process =
        (implementation: AnyImplementation) =>
        async (job: Job<WireInput, WireOutput>): Promise<WireOutput> => {
            const controller = new AbortController();
            const contextJob = {
                id: String(job.id),
                name: implementation.definition.name,
                attemptsMade: job.attemptsMade,
                updateProgress: async (value: unknown) => {
                    assertJson(value);
                    await job.updateProgress(value as Parameters<typeof job.updateProgress>[0]);
                },
                log: (message: string) => job.log(message),
            };
            const args = { job: contextJob, signal: controller.signal };
            const timeoutMs = implementation.workerOptions.timeoutMs;
            const timeout =
                timeoutMs === undefined
                    ? undefined
                    : setTimeout(
                          () =>
                              controller.abort(
                                  new JobTimeoutError(implementation.definition.name, timeoutMs),
                              ),
                          timeoutMs,
                      );
            try {
                const raw = readInput(implementation, job.data);
                const input = implementation.definition.input
                    ? await parseSchema(implementation.definition.input, raw, {
                          phase: 'input',
                          queue: implementation.definition.name,
                      })
                    : raw;
                if (!implementation.definition.input && raw !== undefined)
                    throw new ValidationError(
                        'input',
                        [{ message: 'This job accepts no input' }],
                        implementation.definition.name,
                    );
                const context = options.context ? await options.context(args) : {};
                controller.signal.throwIfAborted();
                // Never race the handler: its lock and concurrency slot remain held until it settles.
                const result = await implementation.execute({ ...args, input, context });
                controller.signal.throwIfAborted();
                const output = implementation.definition.output
                    ? await parseSchema(implementation.definition.output, result, {
                          phase: 'output',
                          queue: implementation.definition.name,
                      })
                    : result;
                controller.signal.throwIfAborted();
                return outputEnvelope(implementation, output);
            } catch (cause) {
                const error: unknown = controller.signal.aborted ? controller.signal.reason : cause;
                const permanent =
                    error instanceof ValidationError ||
                    error instanceof CodecError ||
                    error instanceof ProtocolError ||
                    (error !== null &&
                        typeof error === 'object' &&
                        'retryable' in error &&
                        error.retryable === false);
                const message = failureReason(error);
                throw permanent
                    ? new UnrecoverableError(message)
                    : new Error(message, { cause: error });
            } finally {
                clearTimeout(timeout);
            }
        };
    const start = (): Promise<void> => {
        if (closed) return Promise.reject(new ClosedError());
        if (starting) return starting;
        starting = (async () => {
            try {
                for (const definition of definitions) {
                    const { timeoutMs: _timeoutMs, ...workerSettings } = definition.workerOptions;
                    const worker = new Worker<WireInput, WireOutput>(
                        definition.definition.name,
                        process(definition),
                        {
                            ...workerSettings,
                            connection: connectionFor(options, true),
                            prefix: options.prefix,
                            autorun: false,
                        },
                    );
                    worker.on('error', (error) =>
                        logger.error({ error, queue: definition.definition.name }, 'worker error'),
                    );
                    worker.on('failed', (job, error) =>
                        logger.warn(
                            { error, jobId: job?.id, queue: definition.definition.name },
                            'job attempt failed',
                        ),
                    );
                    workers.push(worker);
                }
                await within(
                    Promise.all(workers.map((worker) => worker.waitUntilReady())),
                    options.startupTimeoutMs ?? 10_000,
                );
                if (closed) return;
                for (const worker of workers)
                    void worker
                        .run()
                        .catch((error) =>
                            logger.error({ error, queue: worker.name }, 'worker stopped'),
                        );
            } catch (error) {
                closed = true;
                try {
                    await closeAll(workers);
                } catch (cleanup) {
                    throw new AggregateError([error, cleanup], 'Worker startup and cleanup failed');
                }
                throw error;
            }
        })();
        return starting;
    };
    const close = (): Promise<void> => {
        if (closing) return closing;
        closed = true;
        closing = (async () => {
            await starting?.catch(() => {});
            await closeAll(workers);
        })();
        return closing;
    };
    return { start, close, [Symbol.asyncDispose]: close };
}
