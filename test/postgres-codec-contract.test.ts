import { describe, expect, test } from 'bun:test';
import {
  decodePostgresValue,
  encodePostgresValue,
} from '../src/infrastructure/persistence/postgres/codec';

describe('PostgreSQL value codec contract', () => {
  test('normalizes nested negative zero with public JSON semantics', () => {
    const original = {
      topLevel: -0,
      nestedObject: { value: -0 },
      nestedArray: [0, -0],
    };
    const decoded = decodePostgresValue(
      encodePostgresValue(original),
      null,
      'nested-negative-zero'
    );

    expect(Object.is(original.topLevel, -0)).toBe(true);
    expect(decoded).toEqual(JSON.parse(JSON.stringify(original)));
    expect(Object.is(decoded?.topLevel, 0)).toBe(true);
    expect(Object.is(decoded?.nestedObject.value, 0)).toBe(true);
    expect(Object.is(decoded?.nestedArray[1], 0)).toBe(true);
  });
});
