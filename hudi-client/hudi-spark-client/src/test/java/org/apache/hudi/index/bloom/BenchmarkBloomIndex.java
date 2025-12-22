/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.index.bloom;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

/**
 * Benchmark for Bloom Index performance.
 * <p>
 * This test benchmarks the performance of Bloom Index with and without Metadata Table enabled,
 * and with different FPP settings.
 * <p>
 * NOTE: This benchmark is disabled by default as it is intended for manual execution and verification.
 * It may fail in some CI environments due to dependency shading issues with Hadoop/Spark.
 */
@Tag("functional")
@Disabled("Benchmark for manual verification. Fails in some CI environments due to dependency issues.")
public class BenchmarkBloomIndex extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkBloomIndex.class);

  @Test
  public void benchmarkBloomIndexPerformance() throws Exception {
    // Setup
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    int totalRecords = 5000; // Small for functional test, would need more for real benchmark
    int updateRecords = 2000;
    String partitionPath = "2023/10/01";

    // Generate initial data
    List<HoodieRecord> records = dataGen.generateInserts(partitionPath, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

    // 1. Baseline: Default Bloom (FPP 1e-9, Metadata OFF)
    long baselineTime = runScenario("BASELINE", records, writeRecords, dataGen, updateRecords, false, 0.000000001);

    // Clean up for next run
    // Ideally we should use different paths, but for simplicity reusing harness structure with cleanup if possible
    // Here we just use a different table name/path for the second run

    // 2. Optimized: Tuned Bloom (FPP 1e-6, Metadata ON)
    // Note: To truly test Metadata Table we need to ensure it's built.
    // The first insert will build it if enabled.
    long optimizedTime = runScenario("OPTIMIZED", records, writeRecords, dataGen, updateRecords, true, 0.000001);

    LOG.info("--------------------------------------------------");
    LOG.info("Benchmark Results:");
    LOG.info("Baseline (Default Bloom, No MDT): " + baselineTime + " ms");
    LOG.info("Optimized (Tuned Bloom, MDT):     " + optimizedTime + " ms");
    if (baselineTime > optimizedTime) {
      LOG.info("Improvement: " + (baselineTime - optimizedTime) + " ms ("
          + String.format("%.2f", (double) (baselineTime - optimizedTime) / baselineTime * 100) + "%)");
    } else {
      LOG.info("Degradation: " + (optimizedTime - baselineTime) + " ms ("
          + String.format("%.2f", (double) (optimizedTime - baselineTime) / baselineTime * 100) + "%)");
    }
    LOG.info("--------------------------------------------------");
  }

  private long runScenario(String name, List<HoodieRecord> initialRecords, JavaRDD<HoodieRecord> initialRDD,
                           HoodieTestDataGenerator dataGen, int numUpdates,
                           boolean enableMetadata, double fpp) throws Exception {

    String tableName = "hoodie_benchmark_" + name;
    String basePath = tempDir.resolve(tableName).toAbsolutePath().toString();

    HoodieTableMetaClient.newTableBuilder()
        .setTableType(org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE)
        .setTableName(tableName)
        .setPayloadClassName(org.apache.hudi.common.model.OverwriteWithLatestAvroPayload.class.getName())
        .initTable(HadoopFSUtils.getStorageConf(hadoopConf()), basePath);

    Properties props = new Properties();
    props.setProperty(HoodieIndexConfig.BLOOM_INDEX_USE_METADATA.key(), String.valueOf(enableMetadata));
    props.setProperty(HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE.key(), String.valueOf(fpp));

    // If metadata is enabled, we need to enable it in write config too
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), String.valueOf(enableMetadata));
    if (enableMetadata) {
      props.setProperty(HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER.key(), "true");
      props.setProperty(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    }

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .fromProperties(props)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .fromProperties(props)
            .build())
        .forTable(tableName)
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context(), config)) {
      // Initial Insert
      String newCommitTime = client.startCommit();
      client.insert(initialRDD, newCommitTime).collect();

      // Generate updates
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates(newCommitTime, numUpdates);
      JavaRDD<HoodieRecord> updateRDD = jsc().parallelize(updates, 1);

      // Measure Upsert Time
      String updateCommitTime = client.startCommit();
      long start = System.currentTimeMillis();
      List<WriteStatus> statuses = client.upsert(updateRDD, updateCommitTime).collect();
      long end = System.currentTimeMillis();

      LOG.info("[" + name + "] Upserted " + statuses.size() + " files in " + (end - start) + " ms");
      return end - start;
    }
  }

  // Helper to get Hadoop Configuration from SparkContext
  private org.apache.hadoop.conf.Configuration hadoopConf() {
    return jsc().hadoopConfiguration();
  }
}
