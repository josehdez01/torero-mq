import { z } from 'zod';
import type { ConcreteQueue, QueueDefinition } from './service.ts';
import { queueService } from './service.ts';

export type InferInput<T extends z.ZodType> = z.infer<T>;
export type InferOutput<T extends z.ZodType | undefined> = T extends z.ZodType ? z.infer<Exclude<T, undefined>> : unknown;

export function defineQueue<TInSchema extends z.ZodType, TOutSchema extends z.ZodType | undefined>(def: {
    name: string;
    inputSchema: TInSchema;
    outputSchema?: TOutSchema;
    process: (
        input: InferInput<TInSchema>,
        ctx: { logger: import('@prbux/logger').Logger; queueName: string },
    ) => Promise<InferOutput<TOutSchema>>;
    defaults?: import('./service.ts').QueueDefaults;
    hooks?: QueueDefinition<InferInput<TInSchema>, InferOutput<TOutSchema>>['hooks'];
    init?: () => Promise<void> | void;
}): ConcreteQueue<InferInput<TInSchema>, InferOutput<TOutSchema>> {
    const fullDef: QueueDefinition<InferInput<TInSchema>, InferOutput<TOutSchema>> = {
        name: def.name,
        inputSchema: def.inputSchema,
        process: def.process as any,
    } as QueueDefinition<InferInput<TInSchema>, InferOutput<TOutSchema>>;
    if (def.outputSchema) (fullDef as any).outputSchema = def.outputSchema;
    if (def.defaults) (fullDef as any).defaults = def.defaults;
    if (def.hooks) (fullDef as any).hooks = def.hooks;
    if (def.init) (fullDef as any).init = def.init;
    return queueService.register(fullDef);
}

export type { ConcreteQueue } from './service.ts';
export type { PublishOptions, ScheduleOptions, AwaitResult, RepeatSpec, RepeatStream } from './service.ts';
