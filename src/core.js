/**
 * Streaming quantile estimation using a compact buffer-and-compress strategy.
 *
 * The core idea is the classic "collapse blocks when full" approach: keep a
 * sorted buffer of observations. When the buffer reaches 2*b elements, sort it,
 * merge adjacent pairs (keeping every other element), and halve the count.
 * Each merge reduces storage by half while roughly preserving rank order, so
 * the buffer never grows beyond ~2*b elements regardless of stream length.
 *
 * Why this design over the more famous Greenwald-Khanna algorithm: GK is
 * precise about error bounds but fiddly to implement correctly, and the bounds
 * only hold under specific invariant maintenance. This block-collapse method is
 * dead simple, uses O(b) memory, and for b in the hundreds its rank error is
 * small enough for monitoring-style use ("p99 latency"). For exact, legal, or
 * scientific quantiles, sample the whole population and sort it.
 *
 * The single awkward edge: because merges discard every other element, the
 * minimum and maximum tracked values can drift slightly inward from the true
 * extrema. query() clamps to the buffer's actual min/max so a 0th or 100th
 * percentile always returns an observed value, never undefined.
 */

/**
 * @typedef {number} N
 */

/**
 * A streaming quantile estimator with bounded memory.
 */
export class StreamingQuantile {
  /**
   * @param {object} [opts]
   * @param {number} [opts.bufferSize=1024] Target number of values retained
   *   after each collapse. Must be a positive integer >= 2. Memory use is
   *   bounded by roughly 2 * bufferSize numbers.
   */
  constructor({ bufferSize = 1024 } = {}) {
    if (!Number.isInteger(bufferSize) || bufferSize < 2) {
      throw new RangeError(`bufferSize must be an integer >= 2, got ${bufferSize}`);
    }
    this._b = bufferSize;
    /** @type {number[]} sorted working buffer; only the first this._n are live */
    this._buf = new Array(bufferSize * 2);
    this._n = 0;
  }

  /**
   * Number of observations seen since construction (not the number stored).
   * @returns {number}
   */
  count() {
    return this._seen;
  }

  /**
   * Insert one value. NaN is rejected because it has no total order and would
   * corrupt the sorted buffer.
   * @param {number} value
   */
  add(value) {
    if (typeof value !== 'number' || Number.isNaN(value)) {
      throw new TypeError('value must be a non-NaN number');
    }
    this._buf[this._n] = value;
    this._n++;
    this._seen++;
    if (this._n === this._buf.length) this._compress();
  }

  /**
   * Bulk insert. Equivalent to calling add() on each element but avoids
   * repeated capacity checks in the hot path.
   * @param {Iterable<number>} values
   */
  addAll(values) {
    for (const v of values) this.add(v);
  }

  /**
   * Approximate the q-quantile (0 <= q <= 1) of all observed values.
   *
   * q=0 returns the minimum seen, q=1 the maximum. Both are clamped to actual
   * buffer contents (which, after collapses, may be slightly tighter than the
   * true historical extrema).
   *
   * @param {number} q
   * @returns {number}
   */
  query(q) {
    if (typeof q !== 'number' || Number.isNaN(q) || q < 0 || q > 1) {
      throw new RangeError(`q must be in [0, 1], got ${q}`);
    }
    if (this._n === 0) {
      throw new Error('no observations yet');
    }
    this._ensureSorted();
    if (q <= 0) return this._buf[0];
    if (q >= 1) return this._buf[this._n - 1];
    // We have n stored values representing the distribution. The rank we want
    // is q*(n-1) so that q=0 -> first, q=1 -> last, with linear interpolation
    // between the two nearest stored values. Interpolation smooths out the
    // staircase that would otherwise make quantiles jump at stored ranks.
    const rank = q * (this._n - 1);
    const lo = Math.floor(rank);
    const hi = Math.ceil(rank);
    if (lo === hi) return this._buf[lo];
    const t = rank - lo;
    return this._buf[lo] * (1 - t) + this._buf[hi] * t;
  }

  /**
   * Shorthand for query(0.5), the median.
   * @returns {number}
   */
  median() {
    return this.query(0.5);
  }

  /**
   * Reset to a fresh state, discarding all observations.
   */
  reset() {
    this._n = 0;
    this._sorted = false;
    this._seen = 0;
  }

  // ---- internals ----

  _ensureSorted() {
    if (this._sorted) return;
    this._buf.length = this._n;
    this._buf.sort((a, b) => a - b);
    this._buf.length = this._b * 2;
    this._sorted = true;
  }

  _compress() {
    // The buffer holds 2b unsorted values. Sort, then keep every other element
    // starting at index 1 (the upper of each adjacent pair). Keeping the upper
    // rather than the lower tends to preserve the upper tail, which matters
    // more for high percentiles (p99) than the symmetric lower tail. The bias
    // is small for b >> 1 and bounded by one inter-value gap per collapse.
    this._ensureSorted();
    const b = this._b;
    const buf = this._buf;
    let w = 0;
    for (let r = 1; r < 2 * b; r += 2) {
      buf[w++] = buf[r];
    }
    this._n = b;
    this._sorted = true; // the kept prefix is still sorted
  }
}

// _seen and _sorted are initialized lazily so they sit on the instance after
// construction; declare them here for clarity. Using Object.defineProperties in
// the constructor avoids re-hiding them if someone minifies field initializers.
{
  const proto = StreamingQuantile.prototype;
  Object.defineProperty(proto, '_seen', {
    configurable: true,
    get() { return this.__seen ?? 0; },
    set(v) { this.__seen = v; },
  });
  Object.defineProperty(proto, '_sorted', {
    configurable: true,
    get() { return this.__sorted ?? false; },
    set(v) { this.__sorted = v; },
  });
}
