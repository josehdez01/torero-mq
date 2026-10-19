import { createHash } from 'node:crypto';
import type { ConnectionOptions } from 'bullmq';
import { validateTimeout, type AnyContract, type AnyImplementation } from './definition.js';
import { decode, encode } from './codec.js';
import { CodecError } from './errors.js';

export interface Logger {
    error(...args: unknown[]): void;
    warn(...args: unknown[]): void;
    info(...args: unknown[]): void;
}
export interface ConnectionSettings {
    /** Plain options create owned connections. Supplied Redis clients remain caller-owned. */
    connection: ConnectionOptions;
    prefix: string;
    logger?: Logger;
    startupTimeoutMs?: number;
}
export type ContractRouter = { readonly [key: string]: AnyContract | ContractRouter };
export type ImplementationRouter = {
    readonly [key: string]: AnyImplementation | ImplementationRouter;
};
export type WireInput = {
    torero: 1;
    name: string;
    version: number;
    input: string;
    schedulerId?: string;
};
export type WireOutput = { torero: 1; name: string; version: number; output: string };

export class ProtocolError extends Error {
    readonly code = 'PROTOCOL_ERROR';
    constructor(message: string) {
        super(message);
        this.name = 'ProtocolError';
    }
}
export class ClosedError extends Error {
    readonly code = 'CLOSED';
    constructor() {
        super('This Torero instance is closed; create a new instance to reconnect.');
        this.name = 'ClosedError';
    }
}
export class JobFailedError extends Error {
    readonly code = 'JOB_FAILED';
    constructor(
        readonly queue: string,
        readonly jobId: string,
        readonly reason: string,
        readonly details?: unknown,
    ) {
        super(`Job ${queue}/${jobId} failed: ${reason}`);
        this.name = 'JobFailedError';
    }
}
export class JobNotFinishedError extends Error {
    readonly code = 'JOB_NOT_FINISHED';
    constructor(
        readonly queue: string,
        readonly jobId: string,
    ) {
        super(`Job ${queue}/${jobId} has not completed`);
        this.name = 'JobNotFinishedError';
    }
}
export class StreamOverflowError extends Error {
    readonly code = 'STREAM_OVERFLOW';
    constructor(readonly capacity: number) {
        super(`Result subscription exceeded its ${capacity}-event buffer`);
        this.name = 'StreamOverflowError';
    }
}
export function failedError(queue: string, jobId: string, reason: string): JobFailedError {
    try {
        const data: unknown = JSON.parse(reason);
        if (isRecord(data) && data.torero === 1 && typeof data.message === 'string') {
            if (typeof data.data === 'string') data.data = decode(data.data);
            return new JobFailedError(queue, jobId, data.message, data);
        }
    } catch {
        /* Ordinary BullMQ errors are plain strings. */
    }
    return new JobFailedError(queue, jobId, reason);
}
export function failureReason(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    const record =
        error !== null && typeof error === 'object' ? (error as Record<string, unknown>) : {};
    const base = {
        torero: 1,
        message,
        code: typeof record.code === 'string' ? record.code : 'PROCESSOR_ERROR',
    };
    try {
        return JSON.stringify({
            ...base,
            ...(record.phase ? { phase: record.phase } : {}),
            ...(record.issues ? { issues: record.issues } : {}),
            ...(record.data !== undefined ? { data: encode(record.data) } : {}),
        });
    } catch {
        return JSON.stringify({
            ...base,
            serializationError: 'Error details could not be serialized',
        });
    }
}
export function isRecord(value: unknown): value is Record<string, unknown> {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}
export function assertPositive(value: number, name: string): void {
    if (!Number.isSafeInteger(value) || value < 1)
        throw new TypeError(`${name} must be a positive safe integer`);
}
export function assertJson(value: unknown, seen = new Set<object>()): void {
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
    if (typeof value === 'number' && Number.isFinite(value)) return;
    if (typeof value !== 'object' || value === null)
        throw new CodecError('Progress must be JSON-compatible');
    if (seen.has(value)) throw new CodecError('Progress cannot contain cycles');
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null && prototype !== Array.prototype)
        throw new CodecError('Progress must contain plain JSON values');
    seen.add(value);
    if (Array.isArray(value) && Object.keys(value).length !== value.length)
        throw new CodecError('Progress cannot contain sparse arrays');
    for (const key of Reflect.ownKeys(value)) {
        if (Array.isArray(value) && key === 'length') continue;
        if (typeof key === 'symbol')
            throw new CodecError('Progress cannot contain symbol properties');
        if (Array.isArray(value) && (!/^(0|[1-9]\d*)$/.test(key) || Number(key) >= value.length))
            throw new CodecError('Progress arrays cannot have custom properties');
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (!descriptor || !('value' in descriptor) || !descriptor.enumerable)
            throw new CodecError('Progress cannot contain accessors or hidden properties');
        assertJson(descriptor.value, seen);
    }
    seen.delete(value);
}
export function assertSettings(options: ConnectionSettings): void {
    if (
        typeof options.prefix !== 'string' ||
        !options.prefix ||
        options.prefix.trim() !== options.prefix
    )
        throw new TypeError('prefix must be a nonempty string without surrounding whitespace');
    if (!options.connection) throw new TypeError('connection is required');
    if ('keyPrefix' in options.connection && options.connection.keyPrefix)
        throw new TypeError('Use Torero prefix, not Redis keyPrefix');
    if (options.startupTimeoutMs !== undefined)
        validateTimeout(options.startupTimeoutMs, 'startupTimeoutMs');
}
export function connectionFor(options: ConnectionSettings, worker: boolean): ConnectionOptions {
    const connection = options.connection;
    if ('duplicate' in connection && typeof connection.duplicate === 'function') return connection;
    return {
        ...connection,
        maxRetriesPerRequest: worker ? null : 1,
        ...(worker ? {} : { enableOfflineQueue: false }),
    };
}
export function stableId(value: string): string {
    return `t-${createHash('sha256').update(value).digest('hex')}`;
}
export function assertId(id: string): void {
    if (typeof id !== 'string' || !id || id.includes(':') || /^\d+$/.test(id))
        throw new TypeError('jobId must be nonempty, contain no colons, and not be purely numeric');
}
export function isContract(value: unknown): value is AnyContract {
    return (
        isRecord(value) && isRecord(value.definition) && typeof value.definition.name === 'string'
    );
}
export function flatten(router: ContractRouter): AnyContract[] {
    const definitions: AnyContract[] = [];
    const names = new Set<string>();
    const active = new Set<object>();
    const visit = (node: ContractRouter) => {
        if (active.has(node)) throw new TypeError('Circular job router');
        active.add(node);
        for (const value of Object.values(node)) {
            if (isContract(value)) {
                if (names.has(value.definition.name))
                    throw new TypeError(`Duplicate job name: ${value.definition.name}`);
                names.add(value.definition.name);
                definitions.push(value);
            } else if (isRecord(value)) visit(value as ContractRouter);
            else throw new TypeError('Routers may only contain job contracts or nested routers');
        }
        active.delete(node);
    };
    visit(router);
    return definitions;
}
export function inputEnvelope(
    contract: AnyContract,
    input: unknown,
    schedulerId?: string,
): WireInput {
    return {
        torero: 1,
        name: contract.definition.name,
        version: contract.definition.version,
        input: encode(input),
        ...(schedulerId === undefined ? {} : { schedulerId }),
    };
}
export function readInput(contract: AnyContract, value: unknown): unknown {
    assertEnvelope(contract, value);
    if (typeof value.input !== 'string') throw new ProtocolError('Missing encoded input');
    return decode(value.input);
}
export function outputEnvelope(contract: AnyContract, output: unknown): WireOutput {
    return {
        torero: 1,
        name: contract.definition.name,
        version: contract.definition.version,
        output: encode(output),
    };
}
export function readOutput(contract: AnyContract, value: unknown): unknown {
    assertEnvelope(contract, value);
    if (typeof value.output !== 'string') throw new ProtocolError('Missing encoded output');
    return decode(value.output);
}
function assertEnvelope(
    contract: AnyContract,
    value: unknown,
): asserts value is Record<string, unknown> {
    if (
        !isRecord(value) ||
        value.torero !== 1 ||
        value.name !== contract.definition.name ||
        value.version !== contract.definition.version
    )
        throw new ProtocolError(
            `Incompatible job envelope for ${contract.definition.name} v${contract.definition.version}`,
        );
}
export async function within<T>(work: Promise<T>, timeoutMs: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            work,
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new Error(`Startup exceeded ${timeoutMs}ms`)),
                    timeoutMs,
                );
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}
export async function closeAll(resources: readonly { close(): Promise<unknown> }[]): Promise<void> {
    const results = await Promise.allSettled(resources.map((resource) => resource.close()));
    const errors = results.flatMap((result) =>
        result.status === 'rejected' ? [result.reason as unknown] : [],
    );
    if (errors.length) throw new AggregateError(errors, 'Could not close all Torero resources');
}
