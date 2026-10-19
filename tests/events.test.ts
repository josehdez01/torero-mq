import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventHub, subscription } from '../src/events.js';
import { ClosedError, StreamOverflowError } from '../src/shared.js';

const backend = vi.hoisted(() => ({
    instances: [] as Array<{
        emit(event: string, value: unknown): boolean;
        opts: Record<string, unknown>;
        close: ReturnType<typeof vi.fn>;
        run: ReturnType<typeof vi.fn>;
    }>,
    readyError: undefined as Error | undefined,
    cursor: '10-0' as unknown,
    runError: undefined as Error | undefined,
}));
vi.mock('bullmq', () => ({
    QueueEvents: class extends EventEmitter {
        opts: Record<string, unknown>;
        closing = false;
        backend = {
            client: Promise.resolve({
                defineCommand: vi.fn(),
                runCommand: vi.fn(async () => backend.cursor),
            }),
        };
        constructor(_name: string, opts: Record<string, unknown>) {
            super();
            this.opts = opts;
            backend.instances.push(this);
        }
        async waitUntilReady() {
            if (backend.readyError) throw backend.readyError;
        }
        toKey(key: string) {
            return `prefix:queue:${key}`;
        }
        run = vi.fn(async () => {
            if (backend.runError) throw backend.runError;
        });
        close = vi.fn(async () => {
            this.closing = true;
        });
    },
}));
afterEach(() => {
    backend.instances.length = 0;
    backend.readyError = undefined;
    backend.cursor = '10-0';
    backend.runError = undefined;
    vi.restoreAllMocks();
});

function channel(capacity = 3, signal = new AbortController().signal) {
    const listeners = new Set<(value: number) => void>();
    const detach = vi.fn();
    const onClose = vi.fn();
    const stream = subscription<number>(
        capacity,
        (push) => {
            listeners.add(push);
            return () => {
                detach();
                listeners.delete(push);
            };
        },
        signal,
        onClose,
    );
    return {
        stream,
        listeners,
        detach,
        onClose,
        push(value: number) {
            for (const listener of listeners) listener(value);
        },
    };
}

describe('result subscription lifecycle', () => {
    it('buffers in FIFO order and switches between buffered and pending delivery', async () => {
        const source = channel();
        expect(source.stream[Symbol.asyncIterator]()).toBe(source.stream);
        source.push(1);
        source.push(2);
        expect(await source.stream.next()).toEqual({ done: false, value: 1 });
        expect(await source.stream.next()).toEqual({ done: false, value: 2 });
        const pending = source.stream.next();
        source.push(3);
        expect(await pending).toEqual({ done: false, value: 3 });
        source.push(4);
        expect(await source.stream.next()).toEqual({ done: false, value: 4 });
        source.stream.close();
        expect(source.listeners.size).toBe(0);
    });

    it('rejects concurrent reads without discarding the original pending read', async () => {
        const source = channel();
        const pending = source.stream.next();
        await expect(source.stream.next()).rejects.toThrow('Concurrent next()');
        source.push(9);
        expect(await pending).toEqual({ done: false, value: 9 });
        source.stream.close();
    });

    it('handles a preaborted signal and removes its newly attached subscriber', async () => {
        const controller = new AbortController();
        const reason = new Error('already stopped');
        controller.abort(reason);
        const source = channel(3, controller.signal);
        await expect(source.stream.next()).rejects.toBe(reason);
        expect(source.listeners.size).toBe(0);
        expect(source.detach).toHaveBeenCalledOnce();
        expect(source.onClose).toHaveBeenCalledOnce();
        source.stream.close();
        expect(source.detach).toHaveBeenCalledOnce();
    });

    it('aborts a pending read with the original reason and detaches the abort listener', async () => {
        const controller = new AbortController();
        const remove = vi.spyOn(controller.signal, 'removeEventListener');
        const source = channel(3, controller.signal);
        const pending = source.stream.next();
        const reason = new Error('request disconnected');
        const assertion = expect(pending).rejects.toBe(reason);
        controller.abort(reason);
        await assertion;
        await expect(source.stream.next()).rejects.toBe(reason);
        expect(remove).toHaveBeenCalledWith('abort', expect.any(Function));
        expect(source.listeners.size).toBe(0);
        expect(source.onClose).toHaveBeenCalledOnce();
    });

    it('closes a pending read exactly once and ignores values after close', async () => {
        const source = channel();
        const pending = source.stream.next();
        source.stream.close();
        source.stream.close();
        source.push(5);
        expect(await pending).toEqual({ done: true, value: undefined });
        expect(await source.stream.next()).toEqual({ done: true, value: undefined });
        expect(source.detach).toHaveBeenCalledOnce();
        expect(source.onClose).toHaveBeenCalledOnce();
    });

    it('return discards buffered items and for-await break releases its subscriber', async () => {
        const source = channel();
        source.push(1);
        source.push(2);
        for await (const value of source.stream) {
            expect(value).toBe(1);
            break;
        }
        expect(await source.stream.next()).toEqual({ done: true, value: undefined });
        expect(await source.stream.return!()).toEqual({ done: true, value: undefined });
        expect(source.listeners.size).toBe(0);
        expect(source.detach).toHaveBeenCalledOnce();
    });

    it('return resolves an outstanding read', async () => {
        const source = channel();
        const pending = source.stream.next();
        await source.stream.return!();
        expect(await pending).toEqual({ done: true, value: undefined });
        expect(source.onClose).toHaveBeenCalledOnce();
    });

    it('throw rejects both the call and pending read, retaining the terminal error', async () => {
        const source = channel();
        const pending = source.stream.next();
        const reason = new Error('consumer failed');
        const assertion = expect(pending).rejects.toBe(reason);
        await expect(source.stream.throw!(reason)).rejects.toBe(reason);
        await assertion;
        await expect(source.stream.next()).rejects.toBe(reason);
        expect(source.listeners.size).toBe(0);
        expect(source.detach).toHaveBeenCalledOnce();
    });

    it('fails explicitly on overflow, discards the buffer, and detaches once', async () => {
        const source = channel(2);
        source.push(1);
        source.push(2);
        source.push(3);
        await expect(source.stream.next()).rejects.toBeInstanceOf(StreamOverflowError);
        await expect(source.stream.next()).rejects.toMatchObject({ capacity: 2 });
        expect(source.listeners.size).toBe(0);
        source.stream.close();
        expect(source.detach).toHaveBeenCalledOnce();
        expect(source.onClose).toHaveBeenCalledOnce();
    });
});

describe('event hub lifecycle', () => {
    const settings = {
        connection: { host: 'localhost' },
        prefix: 'unit',
        logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
    };
    it('shares startup, pins its cursor, routes by job ID, and releases listeners on close', async () => {
        const hub = new EventHub('queue', settings);
        const job = vi.fn();
        const all = vi.fn();
        const detach = hub.listen(job, 'one');
        hub.listen(all);
        const start = hub.start();
        expect(hub.start()).toBe(start);
        await start;
        expect(backend.instances).toHaveLength(1);
        const events = backend.instances[0]!;
        expect(events.opts.lastEventId).toBe('10-0');
        events.emit('completed', { jobId: 'one', returnvalue: { result: 1 } });
        events.emit('failed', { jobId: 'two', failedReason: 'failed' });
        events.emit('removed', { jobId: 'one' });
        expect(job.mock.calls.map(([event]) => event.type)).toEqual(['completed', 'removed']);
        expect(all).toHaveBeenCalledTimes(3);
        detach();
        detach();
        events.emit('completed', { jobId: 'one', returnvalue: 2 });
        expect(job).toHaveBeenCalledTimes(2);
        await hub.close();
        events.emit('completed', { jobId: 'one', returnvalue: 3 });
        expect(all).toHaveBeenCalledTimes(4);
        expect(events.close).toHaveBeenCalledOnce();
    });

    it('rolls back its Redis resource when readiness fails', async () => {
        const reason = new Error('cannot connect');
        backend.readyError = reason;
        const hub = new EventHub('queue', settings);
        await expect(hub.start()).rejects.toBe(reason);
        expect(backend.instances[0]!.close).toHaveBeenCalledOnce();
        expect(backend.instances[0]!.run).not.toHaveBeenCalled();
        await hub.close();
    });

    it('rejects invalid Redis cursors and closes before starting consumption', async () => {
        backend.cursor = 7;
        const hub = new EventHub('queue', settings);
        await expect(hub.start()).rejects.toThrow('Invalid Redis event cursor');
        expect(backend.instances[0]!.close).toHaveBeenCalledOnce();
        expect(backend.instances[0]!.run).not.toHaveBeenCalled();
        await hub.close();
    });

    it('logs asynchronous queue errors and unexpected event-loop termination', async () => {
        const reason = new Error('reader stopped');
        backend.runError = reason;
        const logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
        const hub = new EventHub('queue', { ...settings, logger });
        await hub.start();
        backend.instances[0]!.emit('error', reason);
        expect(logger.error).toHaveBeenCalledWith(
            { error: reason, queue: 'queue' },
            'queue events stopped',
        );
        expect(logger.error).toHaveBeenCalledWith(
            { error: reason, queue: 'queue' },
            'queue events error',
        );
        await hub.close();
    });

    it('rejects a startup that finds its underlying resource already closing', async () => {
        const hub = new EventHub('queue', settings);
        const starting = hub.start();
        const instance = backend.instances[0]!;
        Object.assign(instance, { closing: true });
        await expect(starting).rejects.toBeInstanceOf(ClosedError);
        expect(instance.run).not.toHaveBeenCalled();
        await hub.close();
    });
});
