# ⚡ BOLT PERFORMANCE REPORT: Hudi Write & Indexing Bottlenecks

## Executive Summary
Analysis of the `hudi-client` and `hudi-common` modules revealed two critical performance bottlenecks affecting **write latency** and **storage efficiency** for default Hudi configurations. These issues impact all table types (CoW/MoR) but are most pronounced in upsert-heavy workloads.

## 1. [HUDI-PERF-001] Bloated Bloom Filters in Parquet Footers

**Module:** `hudi-common`
**File:** `HoodieStorageConfig.java`
**Impact Level:** **High** (Storage & IO Overhead)

### Problem
The default False Positive Probability (FPP) for Bloom Filters is set to `0.000000001` (1e-9). This is aggressively low and results in significantly larger bloom filters than necessary for most data lake workloads.

**Metric:**
For `60,000` entries (default `BLOOM_FILTER_NUM_ENTRIES_VALUE`):
- **Default (1e-9):** ~431 KB per file
- **Optimized (1e-6):** ~287 KB per file
- **Bloat:** **~33% larger**

This extra size is written to *every* Parquet file footer (since `hoodie.parquet.bloom.filter.enabled` defaults to `true`), increasing storage costs and footer read latency during index lookups.

### Recommendation
Change default FPP to `0.000001` (1e-6). This is the industry standard (e.g., HBase, Cassandra) and provides a sufficient trade-off for Hudi's file sizing.

## 2. [HUDI-PERF-002] Bloom Index Ignores Metadata Table by Default

**Module:** `hudi-client`
**File:** `HoodieIndexConfig.java`
**Impact Level:** **High** (Write Latency)

### Problem
The configuration `hoodie.bloom.index.use.metadata` defaults to `false`.
Even if a user enables the Metadata Table (`hoodie.metadata.enable=true`) and Column Stats Index, the `HoodieBloomIndex` implementation **will not use it** by default. It falls back to reading file footers from cloud storage/HDFS to get column ranges, defeating the purpose of the Metadata Table for pruning.

### Recommendation
Change `BLOOM_INDEX_USE_METADATA` default to `true` (conditional on MDT availability) OR documentation must explicitly warn users. Code analysis shows `HoodieBloomIndex` already checks for MDT partition existence, so defaulting to `true` should be safe if the user has MDT enabled.

## 3. [HUDI-PERF-003] Default "SIMPLE" Index Scalability

**Module:** `hudi-client`
**File:** `HoodieIndexConfig.java`
**Impact Level:** **Critical** (Scalability)

### Problem
The default `INDEX_TYPE` is `SIMPLE`. This index performs a join against the incoming batch and the on-disk parquet files. For large tables, this O(N) scan becomes the dominant factor in write latency.

### Recommendation
Users should be guided to `BLOOM` or `RECORD_INDEX` for production workloads > 10GB.

---

## Action Plan
1.  **Code Change:** Optimize `BLOOM_FILTER_FPP_VALUE` default in `HoodieStorageConfig`.
2.  **Code Change:** Consider updating `BLOOM_INDEX_USE_METADATA` default.
3.  **Docs:** Update performance tuning guide to highlight `BLOOM` vs `SIMPLE`.

## Verification
A reproduction test `ReproduceBloomBottleneck.java` confirmed the 33% size difference in bloom filters.
