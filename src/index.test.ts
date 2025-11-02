import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { QueueService } from './index.js';

describe('public API', () => {
    it('QueueService.defineQueue registers queues', () => {
        const queueService = new QueueService('test');

        const q = queueService.defineQueue({
            name: 'test-q',
            inputSchema: z.object({ n: z.number() }),
            async process(input) {
                return input.n;
            },
        });
        expect(q.name).toBe('test-q');
    });

    it('QueueService does not expose enqueue methods; ConcreteQueue does', () => {
        const queueService = new QueueService('test');
        queueService.defineQueue({
            name: 'nq',
            inputSchema: z.object({ n: z.number() }),
            async process(input) {
                return input.n;
            },
        });
        // @ts-expect-error publish should not exist on the service type
        expect(queueService.publish).toBeUndefined();
        // Note: getQueue is strongly typed via module augmentation, not available by default here.
    });
});
