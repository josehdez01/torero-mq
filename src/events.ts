import { QueueEvents, type QueueEventsOptions } from 'bullmq';
import {
    ClosedError,
    connectionFor,
    StreamOverflowError,
    within,
    type ConnectionSettings,
} from './shared.js';

export type CompletionEvent =
    | { type: 'completed'; jobId: string; value: unknown }
    | { type: 'failed'; jobId: string; reason: string }
    | { type: 'removed'; jobId: string };
type Listener = (event: CompletionEvent) => void;
/** One Redis event connection and one listener per event type, independent of waiter count. */
export class EventHub {
    private events?: QueueEvents;
    private starting?: Promise<void>;
    private readonly jobs = new Map<string, Set<Listener>>();
    private readonly listeners = new Set<Listener>();
    constructor(
        private readonly name: string,
        private readonly options: ConnectionSettings,
    ) {}
    start(): Promise<void> {
        if (!this.starting)
            this.starting = (async () => {
                const events = new QueueEvents(this.name, {
                    connection: connectionFor(this.options, true),
                    prefix: this.options.prefix,
                    autorun: false,
                });
                this.events = events;
                events.on('error', (error) =>
                    (this.options.logger ?? console).error(
                        { error, queue: this.name },
                        'queue events error',
                    ),
                );
                events.on('completed', ({ jobId, returnvalue }) =>
                    this.emit({ type: 'completed', jobId, value: returnvalue as unknown }),
                );
                events.on('failed', ({ jobId, failedReason }) =>
                    this.emit({ type: 'failed', jobId, reason: failedReason }),
                );
                events.on('removed', ({ jobId }) => this.emit({ type: 'removed', jobId }));
                try {
                    await within(
                        (async () => {
                            await events.waitUntilReady();
                            // '$' is resolved at the first XREAD, which may happen after subscribe returns.
                            // Pin the current Redis cursor before starting so immediate completions cannot slip through.
                            const redis = await events.backend.client;
                            redis.defineCommand('toreroEventCursorV1', {
                                numberOfKeys: 1,
                                lua: "local tail = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1); if #tail == 0 then return '0-0' end; return tail[1][1]",
                            });
                            const cursor: unknown = await redis.runCommand('toreroEventCursorV1', [
                                events.toKey('events'),
                            ]);
                            if (typeof cursor !== 'string')
                                throw new TypeError('Invalid Redis event cursor');
                            const settings: QueueEventsOptions = {
                                ...events.opts,
                                lastEventId: cursor,
                            };
                            events.opts = settings;
                            if (events.closing) throw new ClosedError();
                            void events
                                .run()
                                .catch((error) =>
                                    (this.options.logger ?? console).error(
                                        { error, queue: this.name },
                                        'queue events stopped',
                                    ),
                                );
                        })(),
                        this.options.startupTimeoutMs ?? 10_000,
                    );
                } catch (error) {
                    await events.close();
                    throw error;
                }
            })();
        return this.starting;
    }
    private emit(event: CompletionEvent): void {
        for (const listener of this.jobs.get(event.jobId) ?? []) listener(event);
        for (const listener of this.listeners) listener(event);
    }
    listen(listener: Listener, jobId?: string): () => void {
        const listeners =
            jobId === undefined ? this.listeners : (this.jobs.get(jobId) ?? new Set<Listener>());
        if (jobId !== undefined) this.jobs.set(jobId, listeners);
        listeners.add(listener);
        return () => {
            listeners.delete(listener);
            if (jobId !== undefined && !listeners.size) this.jobs.delete(jobId);
        };
    }
    async close(): Promise<void> {
        await this.starting?.catch(() => {});
        await this.events?.close();
        this.jobs.clear();
        this.listeners.clear();
    }
}

export interface ResultSubscription<T> extends AsyncIterableIterator<T> {
    close(): void;
}
export function subscription<T>(
    capacity: number,
    attach: (push: (value: T) => void) => () => void,
    signal: AbortSignal,
    onClose: () => void = () => {},
): ResultSubscription<T> {
    let pending:
        { resolve(value: IteratorResult<T>): void; reject(error: unknown): void } | undefined;
    const buffer: T[] = [];
    let closed = false;
    let failure: unknown;
    let detach = () => {};
    const close = (error?: unknown) => {
        if (closed) return;
        closed = true;
        failure = error;
        detach();
        signal.removeEventListener('abort', abort);
        buffer.length = 0;
        onClose();
        if (pending) {
            const waiter = pending;
            pending = undefined;
            if (error !== undefined) waiter.reject(error);
            else waiter.resolve({ done: true, value: undefined });
        }
    };
    const abort = () => close(signal.reason ?? new ClosedError());
    detach = attach((value) => {
        if (closed) return;
        if (pending) {
            const waiter = pending;
            pending = undefined;
            waiter.resolve({ done: false, value });
        } else if (buffer.length >= capacity) close(new StreamOverflowError(capacity));
        else buffer.push(value);
    });
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    return {
        [Symbol.asyncIterator]() {
            return this;
        },
        next() {
            if (failure !== undefined) return Promise.reject(failure);
            if (closed) return Promise.resolve({ done: true, value: undefined });
            if (buffer.length) return Promise.resolve({ done: false, value: buffer.shift()! });
            if (pending)
                return Promise.reject(new TypeError('Concurrent next() calls are not supported'));
            return new Promise<IteratorResult<T>>((resolve, reject) => {
                pending = { resolve, reject };
            });
        },
        return() {
            close();
            return Promise.resolve({ done: true, value: undefined });
        },
        throw(error: unknown) {
            close(error);
            return Promise.reject(error);
        },
        close: () => close(),
    };
}
