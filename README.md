Streaming Quantile — constant-memory streaming percentile estimation in pure ESM JavaScript.

```js
import { StreamingQuantile } from 'streaming-quantile';

const latencySamples = [12, 45, 7, 230, 88, 3, 19, 156, 42, 91];
const sq = new StreamingQuantile({ bufferSize: 1024 });
sq.addAll(latencySamples);
const p99 = sq.query(0.99);
const median = sq.median();
```

Add numbers as they arrive; call `query(q)` with `q` in `[0, 1]` to get an
approximate quantile. `query(0)` returns the minimum seen, `query(1)` the
maximum. `count()` returns the number of observations inserted since
construction (not the number stored in memory). `reset()` clears all state.


### Why this exists

For latency dashboards and monitoring, you want p50/p95/p99 over an unbounded
stream without holding every sample in memory. This library keeps roughly
`2 * bufferSize` numbers regardless of how many you insert, using the classic
block-collapse technique: when the working buffer fills, sort it and discard
every other element. Each collapse halves storage while roughly preserving rank
order, so memory is bounded and the estimator never needs to grow.

The trade-off is accuracy. Block collapse introduces small rank error on the
order of `1/bufferSize` per collapse. For monitoring that is fine; for exact
statistics or anything where a single off-by-one matters, collect all samples
and sort them yourself.

### The awkward edge

Because each collapse discards every other element, the tracked minimum and
maximum can drift slightly inward from the true historical extrema. `query(0)`
and `query(1)` return the current buffer's min and max, which after several
collapses may be slightly tighter than the true extrema of everything you ever
inserted. The lower the percentile or the higher, the more this matters.

### API

`new StreamingQuantile({ bufferSize = 1024 })` — `bufferSize` is the number of
values retained after each collapse; must be an integer `>= 2`. Peak memory is
about `2 * bufferSize` numbers.

- `add(value)` — insert one non-NaN number.
- `addAll(iterable)` — insert many.
- `query(q)` — approximate the `q`-quantile, `0 <= q <= 1`. Throws if no
  observations have been added yet.
- `median()` — shorthand for `query(0.5)`.
- `count()` — total observations inserted (not stored count).
- `reset()` — discard all observations.

Zero dependencies. ESM only. Node 18+.

## Design notes

The window stores values eagerly rather than keeping running aggregates. Running
sums drift with floating point over long streams, and recomputing from a small
buffer is cheap enough that the drift is not worth the speed.

