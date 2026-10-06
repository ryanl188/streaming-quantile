import { test } from 'node:test';
import assert from 'node:assert/strict';

import { StreamingQuantile } from '../src/core.js';

// Deterministic small-stream check: with few values and a big buffer, no
// compression happens, so the estimator should be exact.
test('exact when buffer never compresses', () => {
  const sq = new StreamingQuantile({ bufferSize: 16 });
  for (let i = 1; i <= 10; i++) sq.add(i);
  assert.equal(sq.count(), 10);
  assert.equal(sq.query(0), 1);
  assert.equal(sq.query(1), 10);
  assert.equal(sq.query(0.5), 5.5);
});

// After a collapse, the stored count halves and invariants still hold.
test('collapses when buffer fills', () => {
  const sq = new StreamingQuantile({ bufferSize: 4 }); // capacity 8
  for (let i = 1; i <= 8; i++) sq.add(i);
  // First collapse: 8 stored -> 4 stored, count stays 8.
  assert.equal(sq.count(), 8);
  // Stored live count is exactly b after a collapse.
  assert.equal(sq._n, 4);
  // min/max clamp to actual buffer contents.
  assert.equal(sq.query(0), 2);
  assert.equal(sq.query(1), 8);
});

// query must reject out-of-range q with RangeError, not silently clamp.
test('query rejects q outside [0,1]', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  sq.add(1);
  assert.throws(() => sq.query(-0.01), RangeError);
  assert.throws(() => sq.query(1.01), RangeError);
  assert.throws(() => sq.query(NaN), RangeError);
});

// add must reject NaN since it breaks sorted order.
test('add rejects NaN', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  assert.throws(() => sq.add(NaN), TypeError);
});

// Empty estimator has nothing to report.
test('query on empty throws', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  assert.throws(() => sq.query(0.5), /no observations/);
});

// Constructor validates bufferSize.
test('bufferSize must be integer >= 2', () => {
  assert.throws(() => new StreamingQuantile({ bufferSize: 0 }), RangeError);
  assert.throws(() => new StreamingQuantile({ bufferSize: 1 }), RangeError);
  assert.throws(() => new StreamingQuantile({ bufferSize: 1.5 }), RangeError);
  assert.doesNotThrow(() => new StreamingQuantile({ bufferSize: 2 }));
});

// addAll accepts any iterable, not just arrays.
test('addAll accepts a generator', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  function* gen() { yield 1; yield 3; yield 2; }
  sq.addAll(gen());
  assert.equal(sq.median(), 2);
});

// Streaming a long sorted sequence still tracks min/max at the right end.
test('long sorted stream keeps max near the true max', () => {
  const sq = new StreamingQuantile({ bufferSize: 16 });
  for (let i = 0; i < 1000; i++) sq.add(i);
  // Max should be exactly 999 because we always keep index 1,3,5,... of each
  // sorted block, and the last element of the last block is the global max.
  assert.equal(sq.query(1), 999);
});

// reset clears state.
test('reset clears observations', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  sq.add(5); sq.add(10);
  assert.equal(sq.count(), 2);
  sq.reset();
  assert.equal(sq.count(), 0);
  assert.throws(() => sq.query(0.5), /no observations/);
});

// Deterministic seeded pseudo-random stream: exact correctness is not
// asserted, but the p50 must sit within [min,max] and be stable on repeat
test('random stream: median within [min,max] and stable', () => {
  const sq = new StreamingQuantile({ bufferSize: 32 });
  // Simple LCG for determinism.
  let state = 12345;
  for (let i = 0; i < 10000; i++) {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    sq.add(state % 1000);
  }
  const min = sq.query(0);
  const med = sq.query(0.5);
  const max = sq.query(1);
  assert.ok(min <= med && med <= max, `expected ${min} <= ${med} <= ${max}`);
  assert.equal(sq.query(0.5), med, 'repeatable');
});

// The quantile function is monotonic in q for a fixed buffer.
test('query is monotonic in q', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  for (let i = 1; i <= 16; i++) sq.add(i); // triggers collapses
  let prev = -Infinity;
  for (let k = 0; k <= 100; k++) {
    const v = sq.query(k / 100);
    assert.ok(v >= prev, `q=${k/100} gave ${v} < prev ${prev}`);
    prev = v;
  }
});

// Interpolation between stored ranks: single-element median is exact.
test('single odd element median exact', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  sq.addAll([7, 3, 5, 1, 9]); // odd count, no collapse
  assert.equal(sq.median(), 5);
});

// Bulk insert followed by exact quantile on even count.
test('even count interpolates median', () => {
  const sq = new StreamingQuantile({ bufferSize: 8 });
  sq.addAll([10, 20, 30, 40]); // even, no collapse
  assert.equal(sq.median(), 25);
});
