## 2025-12-25 - [Bloom Filter Size & Config Bottlenecks]
**Learning:** Default Bloom Filter FPP (1e-9) creates ~430KB filters per file, while 1e-6 (industry standard) uses ~287KB (-33%).
**Learning:** `BLOOM_INDEX_USE_METADATA` defaults to `false`, causing `HoodieBloomIndex` to ignore Metadata Table Column Stats even when available, forcing footer reads (latency spike).
**Action:** Tune `hoodie.index.bloom.fpp` to `0.000001` and enable `hoodie.bloom.index.use.metadata`.

## 2025-12-25 - [Write Performance Defaults]
**Learning:** Default `INDEX_TYPE` is `SIMPLE` (O(N) file scan). Users managing >100GB datasets experience severe upsert latency without switching to `BLOOM` or `RECORD_INDEX`.
**Action:** Recommend `BLOOM` index + MDT for general use cases.
