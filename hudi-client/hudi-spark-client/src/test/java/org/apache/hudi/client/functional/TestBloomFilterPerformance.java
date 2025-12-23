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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Bloom Filter FPP tuning.
 * This test demonstrates the file size difference between default FPP (1e-9) and tuned FPP (1e-6).
 */
public class TestBloomFilterPerformance extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestBloomFilterPerformance.class);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testBloomFilterFPPImpact() throws IOException {
    // 1. Write with default FPP (1e-9)
    String tablePathDefault = basePath + "/default_fpp";
    long sizeDefault = writeAndGetSize(tablePathDefault, "0.000000001");

    // 2. Write with tuned FPP (1e-6)
    String tablePathTuned = basePath + "/tuned_fpp";
    long sizeTuned = writeAndGetSize(tablePathTuned, "0.000001");

    LOG.info("Total Size (Default FPP 1e-9): " + sizeDefault);
    LOG.info("Total Size (Tuned FPP 1e-6): " + sizeTuned);

    long diff = sizeDefault - sizeTuned;
    LOG.info("Difference: " + diff + " bytes");

    // We expect the default FPP to produce larger files due to larger bloom filters
    assertTrue(sizeDefault > sizeTuned, "Default FPP should produce larger files");
  }

  private long writeAndGetSize(String tablePath, String fpp) throws IOException {
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), tablePath);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(tablePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .bloomFilterFPP(Double.parseDouble(fpp))
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .withBloomFilterFpp(Double.parseDouble(fpp))
            .build())
        .withMetadataConfig(org.apache.hudi.common.config.HoodieMetadataConfig.newBuilder().enable(false).build()) // Disable metadata table to isolate file size
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
      String newCommitTime = client.startCommit();

      // Generate enough records to make bloom filter significant
      // 50k records
      HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 50000);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
    }

    // Calculate total size of parquet files
    try (Stream<Path> paths = Files.walk(Paths.get(tablePath))) {
      return paths
            .filter(Files::isRegularFile)
            .filter(p -> p.toString().endsWith(".parquet"))
            .mapToLong(p -> {
              try {
                return Files.size(p);
              } catch (IOException e) {
                return 0L;
              }
            })
            .sum();
    }
  }
}
