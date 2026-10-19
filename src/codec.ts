import superjson from 'superjson';
import { CodecError } from './errors.js';

// Validate before serialization: JSON would silently drop or alter these values.
function assertSupported(value: unknown, seen = new Set<object>()): void {
    if (typeof value === 'function' || typeof value === 'symbol')
        throw new CodecError(`Unsupported value: ${typeof value}`);
    if (value === null || typeof value !== 'object') return;
    if (seen.has(value)) return;
    seen.add(value);
    if (Object.getOwnPropertySymbols(value).length)
        throw new CodecError('Symbol properties cannot be serialized');
    const prototype: unknown = Object.getPrototypeOf(value);
    const builtin =
        value instanceof Date ||
        value instanceof RegExp ||
        value instanceof Map ||
        value instanceof Set;
    if (builtin) {
        const expectedPrototype =
            value instanceof Date
                ? Date.prototype
                : value instanceof RegExp
                  ? RegExp.prototype
                  : value instanceof Map
                    ? Map.prototype
                    : Set.prototype;
        if (prototype !== expectedPrototype) throw new CodecError('Unsupported built-in subclass');
        const allowedKeys = value instanceof RegExp ? ['lastIndex'] : [];
        if (Object.getOwnPropertyNames(value).some((key) => !allowedKeys.includes(key)))
            throw new CodecError('Custom built-in properties cannot be serialized');
    }
    if (value instanceof Date) {
        if (Number.isNaN(value.getTime()))
            throw new CodecError('Invalid dates cannot be serialized');
        return;
    }
    if (value instanceof RegExp) {
        if (value.lastIndex !== 0)
            throw new CodecError('Stateful regular expressions cannot be serialized');
        return;
    }
    if (value instanceof Map) {
        for (const [key, item] of value) {
            assertSupported(key, seen);
            assertSupported(item, seen);
        }
        return;
    }
    if (value instanceof Set) {
        for (const item of value) assertSupported(item, seen);
        return;
    }
    if (Array.isArray(value) && prototype !== Array.prototype)
        throw new CodecError('Unsupported array subclass');
    if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null)
        throw new CodecError('Unsupported class instance');
    const keys = Reflect.ownKeys(value);
    if (Array.isArray(value)) {
        for (let index = 0; index < value.length; index++)
            if (!Object.hasOwn(value, index))
                throw new CodecError('Sparse arrays cannot be serialized');
    }
    for (const key of keys) {
        if (Array.isArray(value) && key === 'length') continue;
        if (Array.isArray(value) && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key)))
            throw new CodecError('Custom array properties cannot be serialized');
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (!descriptor || !('value' in descriptor))
            throw new CodecError('Accessor properties cannot be serialized');
        if (!descriptor.enumerable)
            throw new CodecError('Non-enumerable properties cannot be serialized');
        assertSupported(descriptor.value, seen);
    }
}
export function encode(value: unknown): string {
    try {
        assertSupported(value);
        return JSON.stringify({ version: 1, payload: superjson.serialize(value) });
    } catch (cause) {
        if (cause instanceof CodecError) throw cause;
        throw new CodecError('Could not encode job value', { cause });
    }
}
export function decode(value: string): unknown {
    try {
        const envelope: unknown = JSON.parse(value);
        if (
            !envelope ||
            typeof envelope !== 'object' ||
            !('version' in envelope) ||
            envelope.version !== 1 ||
            !('payload' in envelope) ||
            !envelope.payload ||
            typeof envelope.payload !== 'object' ||
            !('json' in envelope.payload)
        )
            throw new CodecError('Invalid or unsupported codec envelope');
        return superjson.deserialize(
            envelope.payload as Parameters<typeof superjson.deserialize>[0],
        );
    } catch (cause) {
        if (cause instanceof CodecError) throw cause;
        throw new CodecError('Could not decode job value', { cause });
    }
}
