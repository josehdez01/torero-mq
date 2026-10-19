# Application patterns

Keep contracts small and stable. Put business identifiers in payloads, inject current dependencies through worker context, and make side effects idempotent. The examples below show application boundaries; database and provider methods are application-specific.

## Shared contracts and worker context

A wedding reminder contract can be shared by your HTTP application, admin tool and workers without importing the Redis runtime:

```ts
// contracts.ts
import { job } from 'torero-mq/contract';
import { z } from 'zod';

export const weddingReminder = job('wedding.reminder', { version: 1 })
    .input(z.object({ weddingId: z.string(), reminderId: z.string() }))
    .output(z.object({ providerId: z.string() }))
    .options({ attempts: 5, backoff: { type: 'exponential', delay: 1_000 } });
```

Inject services in the worker process. Context is rebuilt per attempt; it should not be encoded in a job. Middleware can add scoped values and run cleanup in `finally`:

```ts
import { implement, JobError } from 'torero-mq';
import { weddingReminder } from './contracts.js';

type Services = {
    wedding: { get(id: string): Promise<{ active: boolean; recipient: string }> };
    messages: {
        send(input: {
            recipient: string;
            idempotencyKey: string;
            signal: AbortSignal;
        }): Promise<{ id: string }>;
    };
};

export const sendReminder = implement(weddingReminder)
    .$context<Services>()
    .worker({ concurrency: 8, timeoutMs: 20_000 })
    .handler(async ({ input, context, signal }) => {
        const wedding = await context.wedding.get(input.weddingId);
        if (!wedding.active) throw new JobError('WEDDING_INACTIVE', { weddingId: input.weddingId });
        const sent = await context.messages.send({
            recipient: wedding.recipient,
            idempotencyKey: `wedding-reminder:${input.reminderId}`,
            signal,
        });
        return { providerId: sent.id };
    });
```

Pass these services from `createWorkers({ sendReminder }, { connection, prefix, context: async () => services })`. Use `createQueueClient({ reminders: weddingReminder }, settings)` in producers. The router key can differ because the explicit job name supplies queue identity.

Treat a deadline as a request to stop. Forward `signal` to provider requests and check it before another irreversible operation. A provider can accept a request even if your process loses its response; its idempotency key protects the retry.

## Transactional outbox

When an RSVP or payment must cause a job, write the business change and an outbox row in the same database transaction. A relay reads pending rows, publishes a contract payload, then marks each row delivered. Use a stable outbox ID in `jobId` and a separate business/provider idempotency key in the handler.

```ts
const handle = await client.jobs.reminders.publish(
    { weddingId: row.weddingId, reminderId: row.id },
    { jobId: `outbox-${row.id}` },
);
await outbox.markDelivered(row.id, handle.id);
```

The relay can crash after Redis accepts a job and before the database update. A stable ID reduces duplicate queue entries while that job remains retained; it is not a permanent business uniqueness guarantee. Enforce durable idempotency in the business database or provider. Retention and manual job removal can make an old ID reusable.

`publishBulk()` is useful for relay batches, but it does not transact with the business database. Track delivery per row and design recovery for partial application progress.

## Reengagement campaigns

For a reengagement campaign, persist campaign state, eligibility and send history in your database. A recurring scan job can select eligible user IDs and publish individual delivery jobs. Keep payloads to stable IDs and recheck consent, eligibility, campaign status and deduplication at execution time; delayed jobs can outlive the state that created them.

Use a stable campaign/user business key for the provider request. Native queue deduplication can reduce redundant pending work:

```ts
await client.jobs.reengagement.publish(
    { campaignId, userId },
    { deduplication: { id: `campaign-${campaignId}-user-${userId}` } },
);
```

For one future delivery, use `schedule(input, { at })`. For a campaign scanner, use a named recurring scheduler such as `campaign-scan`, with `{ cron: '0 * * * *', timezone: 'UTC' }`. Removing a schedule stops future generation, while already queued jobs still need eligibility checks. A scheduler result subscription can power an operational view, but its future-only, bounded stream cannot replace a campaign audit table.

Use global queue concurrency to cap processing across worker replicas, and worker `limiter` settings for BullMQ rate limits. Match limits to your provider's policy. Queue concurrency limits execution slots; it does not establish a database transaction or eliminate duplicate external effects.
