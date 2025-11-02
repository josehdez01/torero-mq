import { describe, it, expect } from 'vitest';
import { placeholder } from './index.js';

describe('placeholder', () => {
    it('returns ok', () => {
        expect(placeholder()).toBe('ok');
    });
});
