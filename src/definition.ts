import type { StandardSchemaV1 } from '@standard-schema/spec';
import type { JobsOptions } from 'bullmq';
import { ValidationError } from './errors.js';
import type { ValidationPhase } from './errors.js';

const constructionToken = Symbol('torero construction');

export type JobDefaults = Pick<
    JobsOptions,
    | 'attempts'
    | 'priority'
    | 'lifo'
    | 'removeOnComplete'
    | 'removeOnFail'
    | 'stackTraceLimit'
    | 'sizeLimit'
> & { backoff?: number | { type: 'fixed' | 'exponential'; delay?: number; jitter?: number } };
export interface WorkerSettings {
    readonly concurrency?: number;
    readonly limiter?: { readonly max: number; readonly duration: number };
    readonly timeoutMs?: number;
}
/** JSON-compatible progress. Numbers must be finite; undefined and rich values are excluded. */
export type JsonValue =
    string | number | boolean | null | readonly JsonValue[] | { readonly [key: string]: JsonValue };
export interface JobContext {
    readonly id: string;
    readonly name: string;
    readonly attemptsMade: number;
    /** Progress is stored by BullMQ as JSON. */
    updateProgress(value: JsonValue): Promise<void>;
    log(message: string): Promise<unknown>;
}
export interface HandlerArgs<I, C> {
    readonly input: I;
    readonly context: C;
    readonly job: JobContext;
    readonly signal: AbortSignal;
}
export interface JobDefinition {
    readonly name: string;
    readonly version: number;
    readonly input?: StandardSchemaV1;
    readonly output?: StandardSchemaV1;
    readonly defaults: Readonly<JobDefaults>;
}
export interface ContractTypes<I, P, O, H, N extends string> {
    readonly input: I;
    readonly parsed: P;
    readonly output: O;
    readonly handlerOutput: H;
    readonly name: N;
}
declare const contractBrand: unique symbol;
export type AnyContract = {
    readonly [contractBrand]: true;
    readonly definition: JobDefinition;
    readonly '~types': ContractTypes<unknown, unknown, unknown, unknown, string>;
};
export interface AnyImplementation extends AnyContract {
    readonly workerOptions: WorkerSettings;
    execute(args: HandlerArgs<unknown, unknown>): Promise<unknown>;
    readonly '~context': unknown;
}
export type InputOf<T extends AnyContract> = T['~types']['input'];
export type OutputOf<T extends AnyContract> = T['~types']['output'] extends UnspecifiedOutput
    ? unknown
    : T['~types']['output'];
export type InferInputs<T> = T extends AnyContract
    ? InputOf<T>
    : { [K in keyof T]: InferInputs<T[K]> };
export type InferOutputs<T> = T extends AnyContract
    ? OutputOf<T>
    : { [K in keyof T]: InferOutputs<T[K]> };
type UnionToIntersection<U> = (U extends unknown ? (arg: U) => void : never) extends (
    arg: infer I,
) => void
    ? I
    : never;
type ContextUnion<T> = T extends { readonly '~context': infer C }
    ? C
    : T extends AnyContract
      ? Record<never, never>
      : { [K in keyof T]: ContextUnion<T[K]> }[keyof T];
type Simplify<T> = { [K in keyof T]: T[K] };
export type RequiredContext<T> = Simplify<UnionToIntersection<ContextUnion<T>>>;

declare const unspecifiedOutput: unique symbol;
type UnspecifiedOutput = typeof unspecifiedOutput;
export class JobContract<
    I = void,
    P = I,
    O = UnspecifiedOutput,
    H = unknown,
    N extends string = string,
> {
    declare readonly [contractBrand]: true;
    declare readonly '~types': ContractTypes<I, P, O, H, N>;
    readonly definition: JobDefinition;
    constructor(_token: typeof constructionToken, definition: JobDefinition) {
        this.definition = Object.freeze({
            ...definition,
            defaults: Object.freeze({
                ...definition.defaults,
                ...(typeof definition.defaults.backoff === 'object'
                    ? { backoff: Object.freeze({ ...definition.defaults.backoff }) }
                    : {}),
                ...(typeof definition.defaults.removeOnComplete === 'object'
                    ? {
                          removeOnComplete: Object.freeze({
                              ...definition.defaults.removeOnComplete,
                          }),
                      }
                    : {}),
                ...(typeof definition.defaults.removeOnFail === 'object'
                    ? { removeOnFail: Object.freeze({ ...definition.defaults.removeOnFail }) }
                    : {}),
            }),
        });
    }
    input<S extends StandardSchemaV1>(
        schema: S,
    ): JobContract<StandardSchemaV1.InferInput<S>, StandardSchemaV1.InferOutput<S>, O, H, N> {
        return new JobContract(constructionToken, { ...this.definition, input: schema });
    }
    output<S extends StandardSchemaV1>(
        schema: S,
    ): JobContract<I, P, StandardSchemaV1.InferOutput<S>, StandardSchemaV1.InferInput<S>, N> {
        return new JobContract(constructionToken, { ...this.definition, output: schema });
    }
    options(defaults: JobDefaults): JobContract<I, P, O, H, N> {
        validateDefaults(defaults);
        return new JobContract(constructionToken, {
            ...this.definition,
            defaults: { ...this.definition.defaults, ...defaults },
        });
    }
    handler<R extends H>(
        handler: (args: HandlerArgs<P, Record<never, never>>) => R | Promise<R>,
    ): JobImplementation<
        I,
        P,
        O extends UnspecifiedOutput ? Awaited<R> : O,
        H,
        N,
        Record<never, never>
    > {
        return implement(this).handler(handler);
    }
}
export function job<const N extends string>(
    name: N,
    options: { version?: number } = {},
): JobContract<void, void, UnspecifiedOutput, unknown, N> {
    if (!name || name.includes(':'))
        throw new TypeError('Job names must be nonempty and cannot contain colons');
    const version = options.version ?? 1;
    if (!Number.isSafeInteger(version) || version < 1)
        throw new TypeError('Version must be a positive safe integer');
    return new JobContract(constructionToken, { name, version, defaults: {} });
}
declare const middlewareContext: unique symbol;
/** Opaque continuation. Middleware must return the exact value produced by next(). */
export interface MiddlewareResult<C> {
    readonly [middlewareContext]: C;
}
export type MiddlewareArgs<I, C> = HandlerArgs<I, C> & {
    readonly next: <Added extends object = Record<never, never>>(options?: {
        context: Added;
    }) => Promise<MiddlewareResult<Added>>;
};
type Middleware = (
    args: MiddlewareArgs<unknown, unknown>,
) => Promise<MiddlewareResult<object>> | MiddlewareResult<object>;
export class ImplementationBuilder<
    I,
    P,
    O,
    H,
    N extends string,
    Required = Record<never, never>,
    C = Required,
    Used extends boolean = false,
> {
    declare readonly '~middlewareUsed': Used;
    constructor(
        _token: typeof constructionToken,
        readonly contract: JobContract<I, P, O, H, N>,
        private readonly middleware: readonly Middleware[] = [],
        private readonly settings: WorkerSettings = {},
    ) {
        this.settings = Object.freeze({
            ...settings,
            ...(settings.limiter ? { limiter: Object.freeze({ ...settings.limiter }) } : {}),
        });
    }
    $context<Context extends object>(
        this: Used extends false ? ImplementationBuilder<I, P, O, H, N, Required, C, Used> : never,
    ): ImplementationBuilder<I, P, O, H, N, Context, Context> {
        if (this.middleware.length) throw new TypeError('$context must be called before use');
        return new ImplementationBuilder(
            constructionToken,
            this.contract,
            this.middleware,
            this.settings,
        );
    }
    use<Added extends object>(
        middleware: (
            args: MiddlewareArgs<P, C>,
        ) => MiddlewareResult<Added> | Promise<MiddlewareResult<Added>>,
    ): ImplementationBuilder<I, P, O, H, N, Required, Omit<C, keyof Added> & Added, true> {
        return new ImplementationBuilder(
            constructionToken,
            this.contract,
            [...this.middleware, middleware as Middleware],
            this.settings,
        );
    }
    worker(settings: WorkerSettings): ImplementationBuilder<I, P, O, H, N, Required, C, Used> {
        validateWorker(settings);
        return new ImplementationBuilder(constructionToken, this.contract, this.middleware, {
            ...this.settings,
            ...settings,
        });
    }
    handler<R extends H>(
        handler: (args: HandlerArgs<P, C>) => R | Promise<R>,
    ): JobImplementation<I, P, O extends UnspecifiedOutput ? Awaited<R> : O, H, N, Required> {
        const execute = async (args: HandlerArgs<unknown, unknown>): Promise<unknown> => {
            const dispatch = async (index: number, context: unknown): Promise<unknown> => {
                const middleware = this.middleware[index];
                if (!middleware) return handler({ ...args, context } as HandlerArgs<P, C>);
                let called = false;
                let active = true;
                let middlewareFailed = false;
                let nextWork: Promise<object> | undefined;
                let continuation: object | undefined;
                let continuationResult: unknown;
                try {
                    const result = await middleware({
                        ...args,
                        context,
                        next: <Added extends object = Record<never, never>>(options?: {
                            context: Added;
                        }) => {
                            if (!active)
                                throw new Error(
                                    'Middleware next() cannot run after middleware settles',
                                );
                            if (called)
                                throw new Error('Middleware next() may only be called once');
                            called = true;
                            nextWork = dispatch(index + 1, {
                                ...(context as object),
                                ...options?.context,
                            }).then((value) => {
                                continuationResult = value;
                                continuation = Object.freeze(Object.create(null) as object);
                                return continuation;
                            });
                            // Attach immediately: detached next failures must never become unhandled rejections.
                            void nextWork.catch(() => {});
                            return nextWork as Promise<MiddlewareResult<Added>>;
                        },
                    });
                    if (!called) throw new Error('Middleware must call next() exactly once');
                    if (result !== continuation || continuation === undefined)
                        throw new Error(
                            'Middleware must return the exact continuation produced by next()',
                        );
                    return continuationResult;
                } catch (error) {
                    middlewareFailed = true;
                    throw error;
                } finally {
                    active = false;
                    // Retain the worker slot until every started handler has settled, even if middleware throws.
                    await nextWork?.catch((error: unknown) => {
                        if (!middlewareFailed) throw error;
                    });
                }
            };
            return dispatch(0, args.context);
        };
        return new JobImplementation(
            constructionToken,
            this.contract.definition,
            this.settings,
            execute,
        );
    }
}
export class JobImplementation<I, P, O, H, N extends string, C> implements AnyImplementation {
    declare readonly [contractBrand]: true;
    declare readonly '~types': ContractTypes<I, P, O, H, N>;
    declare readonly '~context': C;
    readonly workerOptions: WorkerSettings;
    constructor(
        _token: typeof constructionToken,
        readonly definition: JobDefinition,
        settings: WorkerSettings,
        readonly execute: (args: HandlerArgs<unknown, unknown>) => Promise<unknown>,
    ) {
        this.workerOptions = Object.freeze({
            ...settings,
            ...(settings.limiter ? { limiter: Object.freeze({ ...settings.limiter }) } : {}),
        });
    }
}
export function implement<I, P, O, H, N extends string>(
    contract: JobContract<I, P, O, H, N>,
): ImplementationBuilder<I, P, O, H, N> {
    return new ImplementationBuilder(constructionToken, contract);
}
export async function executeImplementation(
    implementation: AnyImplementation,
    args: HandlerArgs<unknown, unknown>,
): Promise<unknown> {
    return implementation.execute(args);
}
export async function parseSchema<S extends StandardSchemaV1>(
    schema: S,
    value: unknown,
    details: { phase: ValidationPhase; queue?: string },
): Promise<StandardSchemaV1.InferOutput<S>> {
    const result = await schema['~standard'].validate(value);
    if (result.issues) throw new ValidationError(details.phase, result.issues, details.queue);
    return result.value;
}

function positiveInteger(value: number | undefined, name: string, allowZero = false): void {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)))
        throw new TypeError(
            `${name} must be a ${allowZero ? 'nonnegative' : 'positive'} safe integer`,
        );
}
export function validateDefaults(defaults: JobDefaults): void {
    positiveInteger(defaults.attempts, 'attempts');
    positiveInteger(defaults.priority, 'priority', true);
    if (defaults.priority !== undefined && defaults.priority > 2 ** 21)
        throw new TypeError('priority must be at most 2097152');
    if (defaults.lifo !== undefined && typeof defaults.lifo !== 'boolean')
        throw new TypeError('lifo must be boolean');
    positiveInteger(defaults.stackTraceLimit, 'stackTraceLimit', true);
    positiveInteger(defaults.sizeLimit, 'sizeLimit');
    const backoff = defaults.backoff;
    if (typeof backoff === 'number') positiveInteger(backoff, 'backoff', true);
    else if (backoff) {
        if (backoff.type !== 'fixed' && backoff.type !== 'exponential')
            throw new TypeError('backoff.type must be fixed or exponential');
        positiveInteger(backoff.delay, 'backoff.delay', true);
        if (
            backoff.jitter !== undefined &&
            (!Number.isFinite(backoff.jitter) || backoff.jitter < 0 || backoff.jitter > 1)
        )
            throw new TypeError('backoff.jitter must be between zero and one');
    }
    for (const retention of [defaults.removeOnComplete, defaults.removeOnFail]) {
        if (typeof retention === 'number') positiveInteger(retention, 'retention', true);
        else if (retention !== null && typeof retention === 'object') {
            positiveInteger('age' in retention ? retention.age : undefined, 'retention.age', true);
            positiveInteger(retention.count, 'retention.count', true);
        } else if (retention !== undefined && typeof retention !== 'boolean')
            throw new TypeError('retention must be boolean, a count, or an age/count object');
    }
}
export function validateTimeout(value: number, name: string): void {
    positiveInteger(value, name);
    if (value > 2_147_483_647) throw new TypeError(`${name} must be at most 2147483647ms`);
}
function validateWorker(settings: WorkerSettings): void {
    positiveInteger(settings.concurrency, 'concurrency');
    if (settings.timeoutMs !== undefined) validateTimeout(settings.timeoutMs, 'timeoutMs');
    if (settings.limiter) {
        positiveInteger(settings.limiter.max, 'limiter.max');
        positiveInteger(settings.limiter.duration, 'limiter.duration');
    }
}
