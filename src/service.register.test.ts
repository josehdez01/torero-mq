import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

// Mock the runtime module used by the service
vi.mock('./runtime/bullmq-runtime.js', () => ({
    createBullmqRuntime: vi.fn(),
    spawnWorker: vi.fn(),
}));

// Import after mocks
import * as runtimeMod from './runtime/bullmq-runtime.js';
import { QueueService } from './service.js';

const makeDef = (name: string) => ({
    name,
    inputSchema: z.object({ n: z.number() }),
    async process(input: { n: number }) {
        return input.n;
    },
});

const fakeConnection = () =>
    ({
        // only what we actually call in shutdown tests
        quit: vi.fn().mockResolvedValue(undefined),
    }) as any;

beforeEach(() => {
    (runtimeMod.createBullmqRuntime as any).mockReset();
    (runtimeMod.spawnWorker as any).mockReset();
    (runtimeMod.createBullmqRuntime as any).mockImplementation(async () => ({
        queue: { close: vi.fn() },
        events: { close: vi.fn(), on: vi.fn(), off: vi.fn() },
        shutdown: vi.fn().mockResolvedValue(undefined),
    }));
});

describe('QueueService registration and lifecycle', () => {
    it('prevents duplicate registration', () => {
        const svc = new QueueService({ prefix: 'p' });
        svc.register(makeDef('dup'));
        expect(() => svc.register(makeDef('dup'))).toThrow(/already registered/);
    });

    it('throws if registering after initQueues() without allowLateRegistration', async () => {
        const svc = new QueueService({ prefix: 'p' });
        svc.register(makeDef('a'));
        await svc.initQueues({ connection: fakeConnection() });
        expect(() => svc.register(makeDef('b'))).toThrow(/registered after initQueues/);
    });

    it('late registration materializes queue when allowed', async () => {
        const svc = new QueueService({ prefix: 'p', runWorkers: true });
        svc.allowLateRegistration();
        svc.register(makeDef('a'));
        await svc.initQueues({ connection: fakeConnection() });
        expect(runtimeMod.createBullmqRuntime).toHaveBeenCalled();
        // late register after init, should auto-materialize
        (runtimeMod.createBullmqRuntime as any).mockClear();
        svc.register(makeDef('b'));
        // give the microtask a chance because materialize is async but not awaited
        await new Promise((r) => setTimeout(r, 0));
        expect(runtimeMod.createBullmqRuntime).toHaveBeenCalledTimes(1);
        const [defArg, optsArg] = (runtimeMod.createBullmqRuntime as any).mock.calls[0];
        expect(defArg.name).toBe('b');
        expect(optsArg.runWorkers).toBe(true);
    });

    it('materialize errors when connection is missing or name unknown', async () => {
        const svc = new QueueService({ prefix: 'p' });
        svc.register(makeDef('x'));
        await expect(svc.materialize('x')).rejects.toThrow(/Connection not initialized/);

        await svc.initQueues({ connection: fakeConnection() });
        await expect(svc.materialize('unknown')).rejects.toThrow(/not registered/);
    });

    it('shutdown closes runtimes and connection', async () => {
        const conn = fakeConnection();
        const runtimeClose = vi.fn().mockResolvedValue(undefined);
        (runtimeMod.createBullmqRuntime as any).mockResolvedValue({
            queue: { close: vi.fn() },
            events: { close: vi.fn(), on: vi.fn(), off: vi.fn() },
            shutdown: runtimeClose,
        });
        const svc = new QueueService({ prefix: 'p' });
        svc.register(makeDef('a'));
        await svc.initQueues({ connection: conn });
        await svc.shutdown();
        expect(runtimeClose).toHaveBeenCalled();
        expect(conn.quit).toHaveBeenCalled();
    });
});
