## 2024-05-23 - [Bloom Filter FPP Tuning]
**Learning:** Default Bloom Filter False Positive Probability (FPP) of 1e-9 is excessively strict, resulting in 50% larger bloom filters and 2x slower creation time compared to standard 1e-6.
**Action:** Documented performance win. Recommend tuning `hoodie.index.bloom.fpp` to `0.000001` and enabling metadata table bloom index.
