## 2026-01-04 - [HUDI-PERF-01] Default Index Type `SIMPLE` Causes Slow Upserts

**Learning:**
- The default index type for Spark is `SIMPLE` (O(N) scan), which scales poorly compared to `BLOOM`.
- Benchmarking with 100k records showed:
  - **SIMPLE**: Insert 4900 ms, Upsert 6615 ms
  - **BLOOM_DEFAULT** (FPP 1e-9, Metadata OFF): Insert 4944 ms, Upsert 5388 ms
  - **BLOOM_TUNED** (FPP 1e-6, Metadata ON): Insert 5347 ms, Upsert 6758 ms
- **Insight:** Switching from `SIMPLE` to `BLOOM` (even with default conservative settings) improves upsert performance by **~18.5%** (6615ms -> 5388ms) for this workload.
- **Counter-Intuitive Finding:** Enabling Metadata Table for Bloom Index (`BLOOM_TUNED`) actually *degraded* performance for this small dataset (100k records), likely due to the overhead of maintaining the metadata table being higher than the gain from faster index lookups when file I/O is minimal.

**Action:**
- Recommend changing default index type to `BLOOM` for non-small tables.
- Investigate Metadata Table overhead for small batches/tables.
- Future benchmarks should use larger datasets to demonstrate Metadata Table benefits.
