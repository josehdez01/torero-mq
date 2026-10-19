/** Run with Node >=26: pnpm install && pnpm build && node examples/sum-queue.ts */
import { z } from 'zod';
import { job, createQueueClient, createWorkers } from '../dist/index.js';

const sum = job('example.sum')
    .input(z.object({ a: z.number(), b: z.number() }))
    .output(z.object({ total: z.number() }))
    .options({ attempts: 3, backoff: { type: 'exponential', delay: 100 } });
const settings = {
    connection: { url: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379' },
    prefix: 'torero-example',
};
const client = createQueueClient({ sum }, settings);
const workers = createWorkers(
    {
        sum: sum.handler(({ input }) => ({ total: input.a + input.b })),
    },
    settings,
);

try {
    await workers.start();
    const handle = await client.jobs.sum.publish({ a: 2, b: 3 });
    console.log('Job', handle.id, 'returned', await handle.wait({ timeoutMs: 5_000 }));
} finally {
    await workers.close();
    await client.close();
}
