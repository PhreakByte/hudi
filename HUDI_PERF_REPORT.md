# ⚡ HUDI PERFORMANCE: Bloom Filter Size Bloat & Index Defaults

## [HUDI-PERF-001] Aggressive Default Bloom Filter FPP

**Module**: `hudi-common`
**File**: `hudi-common/src/main/java/org/apache/hudi/common/config/HoodieStorageConfig.java:689`
**Hudi Version**: 1.x
**Table Type**: CoW / MoR
**Impact Level**: Medium

### PERFORMANCE PROBLEM:
The default False Positive Probability (FPP) for Bloom Filters is set to `0.000000001` (1e-9).
This extremely low probability requires significantly more bits per key (approx 43 bits/key) compared to a standard optimized value like `0.000001` (1e-6, approx 29 bits/key).
This results in **Bloom filter size bloat** in the Parquet footer, increasing storage overhead and I/O during index lookups.

### REPRODUCTION BENCHMARK:
1. Create a Hudi table with `COPY_ON_WRITE`.
2. Insert 50,000 records.
3. Compare file sizes with `hoodie.index.bloom.fpp=0.000000001` (default) vs `0.000001`.

```java
// See reproduction test in: hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/client/functional/TestBloomFilterPerformance.java
```

### BASELINE METRICS (Estimated):
- **Records per file**: 50,000
- **Bloom Bits per Key (1e-9)**: ~43 bits
- **Bloom Filter Size (1e-9)**: ~270 KB per file
- **Total Parquet Overhead**: Higher due to larger footer.

### ROOT CAUSE ANALYSIS:
`HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE` defaults to `0.000000001`.
While this minimizes false positives, the trade-off for storage is suboptimal for most workloads. 1e-6 is generally sufficient for 1M+ records.

### OPTIMIZED APPROACH:
Tune the default FPP to `0.000001` (1e-6).

### OPTIMIZED METRICS (Expected):
- **Bloom Bits per Key (1e-6)**: ~29 bits
- **Bloom Filter Size (1e-6)**: ~180 KB per file
- **Size Reduction**: ~33% reduction in Bloom Filter size.
- **I/O Savings**: Faster footer reads during index lookup.

### HUDI OPERATIONS AFFECTED:
- Upserts (Bloom Index Lookup)
- Compaction (if using Bloom Index)

### PROOF OF CONCEPT:
The provided reproduction test `TestBloomFilterPerformance.java` demonstrates the file size difference.

### CONFIGURATION RECOMMENDATIONS:
```properties
hoodie.index.bloom.fpp=0.000001
hoodie.metadata.index.bloom.filter.enable=true
```

---

## [HUDI-PERF-002] Default Index Type is SIMPLE (Slow for Large Tables)

**Module**: `hudi-client-common`
**File**: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieIndexConfig.java:374`
**Hudi Version**: 1.x
**Table Type**: CoW / MoR
**Impact Level**: High

### PERFORMANCE PROBLEM:
The default index type is `SIMPLE` for Spark and Java engines.
`SIMPLE` index performs a join against all existing base files to find updates. This is **O(N)** complexity where N is the total data size.
For large tables (>100GB), this results in excessive write latency.

### ROOT CAUSE ANALYSIS:
`HoodieIndexConfig.getDefaultIndexType` hardcodes `SIMPLE` as default.

### OPTIMIZED APPROACH:
Use `BLOOM` index (with tuned FPP) or `RECORD_INDEX` (Metadata Table based) for large tables.

### CONFIGURATION RECOMMENDATIONS:
```properties
hoodie.index.type=BLOOM
# OR
hoodie.index.type=RECORD_INDEX
```

### RISK ASSESSMENT:
- **Transaction Safety**: Maintained.
- **Backward Compatibility**: `SIMPLE` is safe but slow. Changing default requires user awareness.
