import type { StandardSchemaV1 } from '@standard-schema/spec';

export type ValidationPhase = 'input' | 'output';
export class ValidationError extends Error {
    readonly code = 'VALIDATION_ERROR';
    constructor(
        readonly phase: ValidationPhase,
        readonly issues: readonly StandardSchemaV1.Issue[],
        readonly queue?: string,
    ) {
        super(
            `${phase} validation failed${queue ? ` for ${queue}` : ''}: ${issues.map((issue) => issue.message).join('; ')}`,
        );
        this.name = 'ValidationError';
    }
}
export class CodecError extends Error {
    readonly code = 'CODEC_ERROR';
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = 'CodecError';
    }
}
export class JobTimeoutError extends Error {
    readonly code = 'JOB_TIMEOUT';
    constructor(
        readonly queue: string,
        readonly timeoutMs: number,
    ) {
        super(`Job in ${queue} exceeded ${timeoutMs}ms`);
        this.name = 'JobTimeoutError';
    }
}
export class WaitTimeoutError extends Error {
    readonly code = 'WAIT_TIMEOUT';
    constructor(
        readonly queue: string,
        readonly jobId: string,
        readonly timeoutMs: number,
    ) {
        super(`Waiting for ${queue}/${jobId} exceeded ${timeoutMs}ms`);
        this.name = 'WaitTimeoutError';
    }
}
export class JobNotFoundError extends Error {
    readonly code = 'JOB_NOT_FOUND';
    constructor(
        readonly queue: string,
        readonly jobId: string,
    ) {
        super(`Job ${queue}/${jobId} was not found`);
        this.name = 'JobNotFoundError';
    }
}
export class JobCancelledError extends Error {
    readonly code = 'JOB_CANCELLED';
    constructor(
        readonly queue: string,
        readonly jobId: string,
    ) {
        super(`Job ${queue}/${jobId} was cancelled`);
        this.name = 'JobCancelledError';
    }
}

/**
 * A domain failure. Permanent by default; opt in to retries for transient failures.
 * Code and data retain their types locally; JavaScript Promise rejections are not statically typed.
 */
export class JobError<Code extends string = string, Data = unknown> extends Error {
    readonly retryable: boolean;
    constructor(
        readonly code: Code,
        readonly data: Data,
        options: { message?: string; retryable?: boolean; cause?: unknown } = {},
    ) {
        super(
            options.message ?? code,
            options.cause === undefined ? undefined : { cause: options.cause },
        );
        this.name = 'JobError';
        this.retryable = options.retryable ?? false;
    }
}
