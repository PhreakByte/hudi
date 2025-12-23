## 2024-05-23 - Bloom Filter False Positive Probability
**Learning:** Default Bloom Filter FPP is 1e-9 (0.000000001) which is extremely aggressive and causes "Bloom filter size bloat". The recommended value is 1e-6 (0.000001).
**Action:** Detect: Check `hoodie.index.bloom.fpp` in `HoodieStorageConfig`. Fix: Tune to 1e-6 to save ~30-40% storage on bloom filters.

## 2024-05-23 - Index Type Default
**Learning:** Default Index Type is SIMPLE for Spark/Java, which is O(N) scan.
**Action:** Detect: Check `hoodie.index.type`. Fix: Use BLOOM or RECORD_INDEX for large tables.
