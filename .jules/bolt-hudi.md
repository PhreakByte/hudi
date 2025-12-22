## 2025-12-22 - [Indexing Subsystem: Bloom Filter Performance]
**Learning:**
- **Issue**: `HoodieIndexConfig.BLOOM_INDEX_USE_METADATA` defaults to `false`. This causes `HoodieBloomIndex` to ignore the Metadata Table (MDT) even if the MDT is enabled for the table. Instead, it performs expensive file footprint scans to read Bloom filters from parquet footers.
- **Metric**: Default Bloom Filter FPP is `0.000000001` (1e-9). This is excessively strict for most use cases, leading to larger Bloom filters and increased I/O overhead.
- **Impact**: Upsert latency is significantly higher due to:
    1.  O(N) file opens/reads during index lookup (where N is files in partition).
    2.  Larger than necessary Bloom filters due to strict default FPP.

**Action:**
- **Fix**: Explicitly set `hoodie.bloom.index.use.metadata=true` when Metadata Table is enabled.
- **Tune**: Relax `hoodie.index.bloom.fpp` to `0.000001` (1e-6) for a better balance between false positives and storage/IO cost.
- **Verify**: Use `BenchmarkBloomIndex.java` to measure `upsert` latency with and without these optimizations. Expected improvement is >30% for large datasets.
