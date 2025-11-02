import { createHash } from 'node:crypto';
import type { JobsOptions } from 'bullmq';
import type { PublishOptions, QueueDefaults } from './core/types.js';

/**
 * Stable, short hash used to construct idempotent job IDs.
 */
export function hashString(value: string): string {
    return createHash('sha1').update(value).digest('hex');
}

/**
 * Build a deterministic BullMQ job id from a prefix, queue and idempotency key.
 */
export function deriveJobId(prefix: string, queueName: string, idempotencyKey: string): string {
    return `${prefix}:${queueName}:${hashString(idempotencyKey)}`;
}

/**
 * Translate our friendly options into BullMQ's JobsOptions.
 */
export function toBullOptions<T>(
    defaults: QueueDefaults | undefined,
    options?: PublishOptions<T>,
): JobsOptions {
    const jobOptions: JobsOptions = {} as JobsOptions;

    const attempts = options?.retries ?? defaults?.attempts;
    if (attempts !== undefined) jobOptions.attempts = attempts;

    const backoff = options?.backoff ?? defaults?.backoff;
    if (backoff !== undefined)
        jobOptions.backoff = backoff as Exclude<JobsOptions['backoff'], undefined>;

    const priority = options?.priority;
    if (priority !== undefined) jobOptions.priority = priority;

    const removeOnComplete = options?.removeOnComplete ?? defaults?.removeOnComplete ?? true;
    if (removeOnComplete !== undefined) jobOptions.removeOnComplete = removeOnComplete;

    const removeOnFail = options?.removeOnFail ?? defaults?.removeOnFail ?? false;
    if (removeOnFail !== undefined) jobOptions.removeOnFail = removeOnFail;

    return jobOptions;
}
