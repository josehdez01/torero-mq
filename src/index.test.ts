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
});
