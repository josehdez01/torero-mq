import { expectTypeOf } from 'vitest';
import { z } from 'zod';
import { implement, job } from '../src/definition.js';
import type { InferInputs, InferOutputs, RequiredContext } from '../src/definition.js';
const contract = job('stable')
    .input(z.string().transform((value) => value.length))
    .output(z.number());
expectTypeOf<InferInputs<typeof contract>>().toEqualTypeOf<string>();
expectTypeOf<InferOutputs<typeof contract>>().toEqualTypeOf<number>();
expectTypeOf<(typeof contract)['~types']['name']>().toEqualTypeOf<'stable'>();
contract.handler(({ input }) => {
    expectTypeOf(input).toEqualTypeOf<number>();
    return input;
});
// @ts-expect-error handlers must return schema input
contract.handler(() => 'bad');
// @ts-expect-error scheduler options are excluded
contract.options({ repeat: { every: 10 } });
// @ts-expect-error identifiers are controlled by enqueue
contract.options({ jobId: 'bad' });
// @ts-expect-error readonly metadata
contract.definition.name = 'changed';
const task = implement(contract)
    .$context<{ tenant: string }>()
    .use(async ({ context, next }) => next({ context: { size: context.tenant.length } }))
    .handler(async ({ context, input }) => {
        expectTypeOf(context.tenant).toEqualTypeOf<string>();
        expectTypeOf(context.size).toEqualTypeOf<number>();
        return input + context.size;
    });
expectTypeOf<RequiredContext<{ nested: { task: typeof task } }>>().toEqualTypeOf<{
    tenant: string;
}>();
const inferred = job('inferred').handler(async () => ({ ok: true as const }));
expectTypeOf<InferOutputs<typeof inferred>>().toEqualTypeOf<{ ok: true }>();
const tree = { one: contract, two: inferred };
expectTypeOf<InferInputs<typeof tree>>().toEqualTypeOf<{ one: string; two: void }>();
const transformed = job('output-transform')
    .output(z.string().transform(Number))
    .handler(() => '1');
expectTypeOf<InferOutputs<typeof transformed>>().toEqualTypeOf<number>();
// @ts-expect-error context requirements must be declared before middleware
implement(contract)
    .use(({ next }) => next())
    .$context<{ wrong: string }>();
// @ts-expect-error worker configuration retains middleware state
implement(contract)
    .use(({ next }) => next())
    .worker({ concurrency: 2 })
    .$context<{ wrong: string }>();
const unknownOutput = job('unknown')
    .output(z.unknown())
    .handler(() => 'value');
expectTypeOf<InferOutputs<typeof unknownOutput>>().toEqualTypeOf<unknown>();
const noInput = job('none').handler(({ input }) => {
    expectTypeOf(input).toEqualTypeOf<void>();
    return 1;
});
expectTypeOf<RequiredContext<{ a: typeof task; b: typeof noInput }>>().toEqualTypeOf<{
    tenant: string;
}>();
const secondContext = implement(job('second'))
    .$context<{ count: number }>()
    .handler(() => 1);
expectTypeOf<
    RequiredContext<{ a: typeof task; b: typeof noInput; nested: { c: typeof secondContext } }>
>().toEqualTypeOf<{ tenant: string; count: number }>();
import { JobContract, ImplementationBuilder, JobImplementation } from '../src/definition.js';
// @ts-expect-error constructors require inaccessible internal capability
new JobContract<string>({ name: 'forged', version: 1, defaults: {} });
// @ts-expect-error constructors require inaccessible internal capability
new ImplementationBuilder(contract);
// @ts-expect-error constructors require inaccessible internal capability
new JobImplementation(contract.definition, {}, async () => 1);
expectTypeOf<RequiredContext<{ contract: typeof contract; task: typeof task }>>().toEqualTypeOf<{
    tenant: string;
}>();
// @ts-expect-error contexts must be object records to support safe middleware merging
implement(contract).$context<number>();

import { createQueueClient, createWorkers } from '../src/index.js';
const publicSettings = { connection: { host: 'localhost' }, prefix: 'types' };
const publicRouter = { nested: { contract }, empty: noInput };
const publicClient = createQueueClient(publicRouter, publicSettings);
publicClient.jobs.nested.contract.publish('raw');
publicClient.jobs.empty.publish();
// @ts-expect-error schema input is required
publicClient.jobs.nested.contract.publish();
// @ts-expect-error wire input is original schema input, not transformed output
publicClient.jobs.nested.contract.publish(2);
expectTypeOf(publicClient.jobs.nested.contract.name).toEqualTypeOf<'stable'>();
expectTypeOf(publicClient.jobs.nested.contract.publish('value'))
    .resolves.toHaveProperty('wait')
    .toBeFunction();
expectTypeOf<
    Awaited<ReturnType<typeof publicClient.jobs.nested.contract.publish>>['wait']
>().returns.resolves.toEqualTypeOf<number>();
createWorkers(
    { nested: { task }, empty: noInput },
    { ...publicSettings, context: () => ({ tenant: 'tenant' }) },
);
// @ts-expect-error worker context factory is required for declared root dependencies
createWorkers({ nested: { task }, empty: noInput }, publicSettings);
// @ts-expect-error context factory must supply the required dependency types
createWorkers({ nested: { task } }, { ...publicSettings, context: () => ({ tenant: 42 }) });
// @ts-expect-error bare contracts have no processor
createWorkers({ contract }, publicSettings);
createWorkers(
    { task, secondContext },
    // @ts-expect-error merged context must satisfy every implementation
    { ...publicSettings, context: () => ({ tenant: 'tenant' }) },
);
createWorkers(
    { task, secondContext },
    { ...publicSettings, context: () => ({ tenant: 'tenant', count: 1 }) },
);
// @ts-expect-error producer routing properties are readonly
publicClient.jobs.nested.contract = undefined as never;
// @ts-expect-error handler output must match declared schema input
implement(contract).handler(async () => ({ bad: 'output' }));

job('progress').handler(async ({ job: currentJob }) => {
    await currentJob.updateProgress({ percent: 50, steps: [true, null, 'done'] });
    // @ts-expect-error progress must be JSON compatible
    await currentJob.updateProgress({ date: new Date() });
    // @ts-expect-error progress excludes bigint
    await currentJob.updateProgress(1n);
    // @ts-expect-error undefined properties are lossy in JSON
    await currentJob.updateProgress({ value: undefined });
});

import type { MiddlewareResult } from '../src/definition.js';
// @ts-expect-error middleware continuations use an inaccessible unique-symbol brand
const _fakeContinuation: MiddlewareResult<{ foo: string }> = {};
// @ts-expect-error middleware cannot replace inferred output with arbitrary objects
implement(job('invalid-middleware')).use(() => ({ answer: 42 }));

import type { AnyContract } from '../src/definition.js';
const fabricatedContract = { definition: contract.definition, '~types': contract['~types'] };
// @ts-expect-error raw metadata and phantom types cannot forge a contract
const _forbiddenContract: AnyContract = fabricatedContract;
// @ts-expect-error producers require authentic branded contracts
createQueueClient({ forged: fabricatedContract }, publicSettings);

// @ts-expect-error custom backoff strategies are not configured by this runtime
job('unsupported-backoff').options({ backoff: { type: 'custom' } });
