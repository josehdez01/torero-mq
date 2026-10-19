/** Runtime-free contract entry point for shared packages. */
export { job, implement } from './definition.js';
export type {
    JobContract,
    JobImplementation,
    JobDefinition,
    JobDefaults,
    WorkerSettings,
    JobContext,
    JsonValue,
    HandlerArgs,
    InferInputs,
    InferOutputs,
    InputOf,
    OutputOf,
    MiddlewareArgs,
    MiddlewareResult,
} from './definition.js';
export { ValidationError, JobError } from './errors.js';
