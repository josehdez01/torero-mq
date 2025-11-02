import { z } from 'zod';
import { QueueService } from '../src/index.js';

// This module defines and exports a ConcreteQueue handle.
// Consumers can import `sumQueue` anywhere and call
//   - sumQueue.publish(input)
//   - sumQueue.scheduleAt({ when }, input)
//   - sumQueue.scheduleIn({ delayMs }, input)
//   - sumQueue.scheduleRepeat(spec, input)
// Note: ensure the QueueService that registered this queue is initialized once
// in your app (e.g. at startup) via `queueService.initQueues({ connection, runWorkers })`.
// You can either export `queueService` from here or centralize it in a `queues/` index.
const queueService = new QueueService('queues');

// Export schemas to enable typed getQueue() via module augmentation
export const sumInputSchema = z.object({ a: z.number(), b: z.number() });
export const sumOutputSchema = z.object({ sum: z.number() });

export const sumQueue = queueService.defineQueue({
    name: 'sum',
    inputSchema: sumInputSchema,
    outputSchema: sumOutputSchema,
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

/**
 * Example usages of the exported `sumQueue` handle:
 *
 * // 1) Publish and await the typed result
 * const { jobId, awaitResult } = await sumQueue.publish({ a: 2, b: 3 });
 * const result = await awaitResult(); // { sum: number }
 *
 * // 2) Fire-and-forget
 * void sumQueue.publish({ a: 10, b: 5 });
 *
 * // 3) Idempotent publish (dedupe by input)
 * await sumQueue.publish(
 *   { a: 1, b: 2 },
 *   { idempotencyKey: ({ a, b }) => `${a}:${b}` }
 * );
 *
 * // 4) Schedule for later
 * await sumQueue.scheduleAt({ when: new Date(Date.now() + 15_000) }, { a: 4, b: 9 });
 * await sumQueue.scheduleIn({ delayMs: 30_000 }, { a: 3, b: 3 });
 *
 * // 5) Repeat and iterate results
 * const stream = sumQueue.scheduleRepeat({ everyMs: 5_000 }, { a: 2, b: 2 });
 * for await (const { jobId, result } of stream) {
 *   console.log('repeat', jobId, result);
 *   if (shouldStop()) await stream.cancel();
 * }
 */
