import type { z } from 'zod';

/**
 * Minimal logger interface used by the queue runtime and service.
 */

export interface Logger {
    info: (...args: unknown[]) => void;
    warn: (...args: unknown[]) => void;
    error: (...args: unknown[]) => void;
}

/** Backoff behaviour for retries. */
export type BackoffOptions = { type: 'fixed' | 'exponential'; delay: number };

/** Options that affect how a job is enqueued and executed. */
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

/** Scheduling options are identical to publish options. */
export type ScheduleOptions<TInput> = PublishOptions<TInput>;

/** Handle returned by publish/schedule that allows awaiting results and canceling. */
export type AwaitResult<T> = {
    jobId: string;
    awaitResult: (options?: { timeoutMs?: number }) => Promise<T>;
    cancel: () => Promise<void>;
};

/**
 * Repeat interval specification. Discriminated union with either a fixed interval
 * or a cron expression.
 */
export type RepeatSpec =
    | {
          type: 'interval';
          everyMs: number;
          startDate?: Date;
          endDate?: Date;
          limit?: number;
      }
    | {
          type: 'cron';
          cron: string;
          startDate?: Date;
          endDate?: Date;
          limit?: number;
      };

/** Yielded values from a repeat stream: per-job result with its id. */
export type RepeatYield<T> = { jobId: string; result: T };

/**
 * Async iterable that yields results for each run of a repeatable job.
 * Call `cancel()` to stop scheduling future runs, or `close()` to stop listening.
 */
export interface RepeatStream<T> extends AsyncIterable<RepeatYield<T>> {
    cancel(): Promise<void>;
    close(): void;
}

/** Per-queue defaults controlling worker and job behaviour. */
export interface QueueDefaults {
    concurrency?: number;
    attempts?: number;
    backoff?: BackoffOptions;
    timeoutMs?: number;
    removeOnComplete?: boolean | number;
    removeOnFail?: boolean | number;
    // Narrow at runtime/bullmq boundary
    limiter?: unknown;
}

/** Context object passed to each processor. */
export interface ProcessCtx<Name extends string = string> {
    logger: Logger;
    queueName: Name;
}

/** Optional hooks for queue lifecycle events. */
export interface QueueHooks {
    onComplete?: (evt: { jobId: string; queue: string }) => void;
    onFailed?: (evt: { jobId: string; queue: string; failedReason: string }) => void;
}

/** A fully-typed queue definition: schemas, processor, defaults and hooks. */
export interface QueueDefinition<TInput, TOutput, Name extends string = string> {
    name: Name;
    inputSchema: z.ZodType;
    outputSchema?: z.ZodType;
    process: (input: TInput, context: ProcessCtx<Name>) => Promise<TOutput>;
    defaults?: QueueDefaults;
    hooks?: QueueHooks;
    init?: () => Promise<void> | void;
}

/** Public handle returned after registration that exposes enqueue methods. */
export interface ConcreteQueue<TInput, TOutput> {
    name: string;
    publish(input: TInput, options?: PublishOptions<TInput>): Promise<AwaitResult<TOutput>>;
    scheduleAt(
        params: { when: Date },
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>>;
    scheduleIn(
        params: { delayMs: number },
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): Promise<AwaitResult<TOutput>>;
    scheduleRepeat(
        spec: RepeatSpec,
        input: TInput,
        options?: ScheduleOptions<TInput>,
    ): Promise<RepeatStream<TOutput>>;
}

/** Infer the input type from a Zod schema. */
export type InferInput<T extends z.ZodType> = z.infer<T>;
/** Infer the output type from an optional Zod schema. */
export type InferOutput<T extends z.ZodType | undefined> = T extends z.ZodType
    ? z.infer<Exclude<T, undefined>>
    : unknown;

/**
 * Global registry for strongly-typed dynamic access via QueueService.getQueue().
 * Apps can augment this interface in a .d.ts file to describe their queues:
 *
 * declare module 'torero-mq' {
 *   interface ToreroQueues {
 *     sum: { input: { a: number; b: number }; output: { sum: number } };
 *   }
 * }
 */
export interface ToreroQueues {}

/** Helper types for getQueue() to extract mapped input/output types. */
export type QueueInput<Name extends keyof ToreroQueues> = ToreroQueues[Name] extends {
    input: infer I;
}
    ? I
    : never;
export type QueueOutput<Name extends keyof ToreroQueues> = ToreroQueues[Name] extends {
    output: infer O;
}
    ? O
    : never;
