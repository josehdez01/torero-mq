import { z } from 'zod';
import { QueueService } from '../src/index.js';

const queueService = new QueueService('queues');
export const sumQueue = queueService.defineQueue({
    name: 'sum',
    inputSchema: z.object({ a: z.number(), b: z.number() }),
    outputSchema: z.object({ sum: z.number() }),
    async process(input) {
        return { sum: input.a + input.b };
    },
    defaults: {
        attempts: 3,
        backoff: { type: 'exponential', delay: 500 },
        timeoutMs: 10_000,
        removeOnComplete: true,
        removeOnFail: false,
        concurrency: 5,
    },
});
