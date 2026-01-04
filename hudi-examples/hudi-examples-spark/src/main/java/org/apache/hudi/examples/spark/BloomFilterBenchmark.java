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

package org.apache.hudi.examples.spark;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.examples.common.HoodieExampleSparkUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class BloomFilterBenchmark {

  private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: BloomFilterBenchmark <tablePath> <tableName> <scenario>");
      System.err.println("Scenario: SIMPLE, BLOOM_DEFAULT, BLOOM_TUNED");
      System.exit(1);
    }
    String tablePath = args[0];
    String tableName = args[1];
    String scenario = args[2];

    System.out.println("Running benchmark for scenario: " + scenario);

    SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-perf-benchmark");
    // Ensure we have enough memory for the test
    sparkConf.set("spark.driver.memory", "4g");
    sparkConf.set("spark.executor.memory", "4g");

    try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
      HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

      // Initialize table
      Path path = new Path(tablePath);
      FileSystem fs = HadoopFSUtils.getFs(tablePath, jsc.hadoopConfiguration());
      if (fs.exists(path)) {
        fs.delete(path, true);
      }

      HoodieTableMetaClient.newTableBuilder()
          .setTableType(tableType)
          .setTableName(tableName)
          .setPayloadClass(HoodieAvroPayload.class)
          .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tablePath);

      // Configure based on scenario
      HoodieWriteConfig.Builder configBuilder = HoodieWriteConfig.newBuilder()
          .withPath(tablePath)
          .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .withParallelism(4, 4)
          .withDeleteParallelism(4)
          .forTable(tableName);

      if ("SIMPLE".equals(scenario)) {
        configBuilder.withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.SIMPLE)
            .build());
        configBuilder.withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build());
      } else if ("BLOOM_DEFAULT".equals(scenario)) {
        configBuilder.withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .bloomFilterFPP(0.000000001) // Default
            .bloomIndexUseMetadata(false) // Default
            .build());
        configBuilder.withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build());
      } else if ("BLOOM_TUNED".equals(scenario)) {
        configBuilder.withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .bloomFilterFPP(0.000001) // Tuned
            .bloomIndexUseMetadata(true) // Enable metadata index
            .build());
        configBuilder.withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexBloomFilter(true) // Enable bloom filter in metadata
            .build());
      }

      HoodieWriteConfig cfg = configBuilder.build();

      try (SparkRDDWriteClient<HoodieAvroPayload> client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg)) {

        // 1. Initial Insert (Seed the table)
        // 100000 records
        String newCommitTime = client.startCommit();
        System.out.println("Starting insert commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 100000);
        JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 4);

        long startInsert = System.currentTimeMillis();
        client.insert(writeRecords, newCommitTime);
        long endInsert = System.currentTimeMillis();
        System.out.println("Insert time: " + (endInsert - startInsert) + " ms");

        // 2. Upsert (Update 50000 existing, Insert 50000 new)
        // This triggers index lookup
        newCommitTime = client.startCommit();
        System.out.println("Starting upsert commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> updates = dataGen.generateUpdates(newCommitTime, 50000);
        List<HoodieRecord<HoodieAvroPayload>> inserts = dataGen.generateInserts(newCommitTime, 50000);
        List<HoodieRecord<HoodieAvroPayload>> upsertRecords = new ArrayList<>(updates);
        upsertRecords.addAll(inserts);

        JavaRDD<HoodieRecord<HoodieAvroPayload>> upsertRDD = jsc.parallelize(upsertRecords, 4);

        long startUpsert = System.currentTimeMillis();
        client.upsert(upsertRDD, newCommitTime);
        long endUpsert = System.currentTimeMillis();
        System.out.println("Upsert time (" + scenario + "): " + (endUpsert - startUpsert) + " ms");
      }
    }
  }
}
