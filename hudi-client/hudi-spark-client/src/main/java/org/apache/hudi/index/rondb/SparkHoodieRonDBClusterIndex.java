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

package org.apache.hudi.index.rondb;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.DateTime;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBClusterIndex<T extends HoodieRecordPayload>
    extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBClusterIndex.class);
  private static SessionFactory entitySessionFactory;
  private static transient Thread shutdownThread;

  private final String recordKey = "record_key";
  private final String commitTimestamp = "commit_ts";
  private final String partition = "partition_path";
  private final String fileName = "file_name";

  private final String indexRecordKey = "idx_record_key";

  private final String tableName = "index_cluster_record";

  public SparkHoodieRonDBClusterIndex(HoodieWriteConfig config) {
    super(config);
    init();
  }

  private void init() {
    if (entitySessionFactory == null) {
      setUpEnvironment();
      entitySessionFactory = ClusterJHelper.getSessionFactory(config.getRonDBIndexCLUSTERJ());
      addShutDownHook();
      init();
    }
  }

  private void setUpEnvironment() {
    try (Statement stmt = getRonDBConnection().createStatement()) {
      String sqlTemplate = "CREATE TABLE IF NOT EXISTS %1$s (\n"
          + "  %2$s VARBINARY(255) NOT NULL, \n"
          + "  %3$s BIGINT NOT NULL, \n"
          + "  %4$s VARCHAR(255) NOT NULL, \n"
          + "  %5$s VARCHAR(38) NOT NULL, \n"
          + "  PRIMARY KEY (%2$s, %3$s), \n"
          + "  INDEX %6$s (%2$s) \n"
          + ") ENGINE=NDBCLUSTER";
      String sql = String.format(sqlTemplate, tableName, recordKey, commitTimestamp, partition, fileName, indexRecordKey);
      stmt.execute(sql);
      LOG.debug("Table created");
    } catch (SQLException e) {
      throw new HoodieIndexException(e.getMessage());
    }
  }

  private Connection getRonDBConnection() {
    try {
      Class.forName(config.getRonDBIndexJDBCDriver());
      return DriverManager.getConnection(config.getRonDBIndexJDBCURL(), config.getRonDBIndexJDBC());
    } catch (SQLException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              "url: " + config.getRonDBIndexJDBCURL(), e);
    }  catch (ClassNotFoundException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              "bad driver: " + config.getRonDBIndexJDBCDriver(), e);
    }
  }

  /**
   * Since we are sharing the RonDBConnection across tasks in a JVM, make sure the entitySessionFactory is closed when JVM
   * exits.
   */
  private void addShutDownHook() {
    if (null == shutdownThread) {
      shutdownThread = new Thread(() -> {
        try {
          entitySessionFactory.close();
        } catch (Exception e) {
          LOG.error("Problem closing RonDB connection");
        } finally {
          entitySessionFactory = null;
        }
      });
      Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty() && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  /**
   * Ensure that any resources used for indexing are released here.
   */
  @Override
  public void close() {
    LOG.info("No resources to release from RonDB index");
  }

  @Override
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records,
                                                 HoodieEngineContext context, HoodieTable hoodieTable) {
    return HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(records)
        .mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true));
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
      HoodieTableMetaClient metaClient) {

    return (partitionNum, hoodieRecordIterator) -> {

      final long startTimeForPutsTask = DateTime.now().getMillis();

      synchronized (SparkHoodieRonDBIndex.class) {
        init();
      }

      Map<String, HoodieRecord> recordMap = new HashMap<>();

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();

      try (Session session = entitySessionFactory.getSession()) {

        QueryBuilder builder = session.getQueryBuilder();
        QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
        domain.where(domain.get("recordKey").in(domain.param("recordKeys")));

        // Do the tagging.
        while (hoodieRecordIterator.hasNext()) {
          HoodieRecord record = hoodieRecordIterator.next();
          recordMap.put(record.getRecordKey(), record);
          // iterator till we reach batch size
          if (hoodieRecordIterator.hasNext() && recordMap.size() < config.getRonDBIndexBatchSize()) {
            continue;
          }
          //for each batch (later do another for each to fill tagged records)
          Query<HudiRecord> query = session.createQuery(domain);
          query.setParameter("recordKeys",
                  recordMap.keySet().stream().map(v -> v.getBytes()).toArray());
          query.setOrdering(Query.Ordering.DESCENDING, "recordKey", "commitTs");
          // can't do inner joins to return only the latest record, therefore if update of partition is enabled more
          // records will be returned then needed

          List<HudiRecord> results = query.getResultList();

          for (HudiRecord resultRecord : results) {
            String resultKey = new String(resultRecord.getRecordKey());
            String resultCommitTimestamp = Long.toString(resultRecord.getCommitTs());
            String resultFileName = resultRecord.getFileName();
            String resultPartition = resultRecord.getPartitionPath();

            HoodieRecord currentRecord = recordMap.remove(resultKey);
            if (currentRecord == null) {
              // the latest value already processed for this record, therefore ignore
              continue;
            }

            if (!checkIfValidCommit(metaClient, resultCommitTimestamp)) {
              // if commit is invalid, treat this as a new taggedRecord
              taggedRecords.add(currentRecord);
              continue;
            }

            // check whether to do partition change processing
            if (config.getRonDBIndexUpdatePartitionPath() && !resultPartition.equals(currentRecord.getPartitionPath())) {
              // delete partition old data record
              HoodieRecord emptyRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), resultPartition),
                      new EmptyHoodieRecordPayload());
              emptyRecord.unseal();
              emptyRecord.setCurrentLocation(new HoodieRecordLocation(resultCommitTimestamp, resultFileName));
              emptyRecord.seal();
              // insert partition new data record
              currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), currentRecord.getPartitionPath()),
                      currentRecord.getData());
              taggedRecords.add(emptyRecord);
              taggedRecords.add(currentRecord);
            } else {
              currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), resultPartition),
                      currentRecord.getData());
              currentRecord.unseal();
              currentRecord.setCurrentLocation(new HoodieRecordLocation(resultCommitTimestamp, resultFileName));
              currentRecord.seal();
              taggedRecords.add(currentRecord);
              // the key from Result and the key being processed should be same
              assert (currentRecord.getRecordKey().contentEquals(resultKey));
            }
          }

          for (HoodieRecord currentRecord : recordMap.values()) {
            taggedRecords.add(currentRecord);
          }

          recordMap.clear();
        }
      }

      final long endPutsTime = DateTime.now().getMillis();
      LOG.debug("rondb puts task time for this task: " + (endPutsTime - startTimeForPutsTask));
      return taggedRecords.iterator();
    };
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context,
                                                HoodieTable  hoodieTable) {
    JavaRDD<WriteStatus> writeStatusRDD = HoodieJavaRDD.getJavaRDD(writeStatus);
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(SparkMemoryUtils.getWriteStatusStorageLevel(config.getProps()));
    return HoodieJavaRDD.of(writeStatusJavaRDD);
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {

    return (partition, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();

      final long startTimeForPutsTask = DateTime.now().getMillis();
      LOG.debug("startTimeForPutsTask for this task: " + startTimeForPutsTask);

      synchronized (SparkHoodieRonDBIndex.class) {
        init();
      }

      try (Session session = entitySessionFactory.getSession()) {

        QueryBuilder builder = session.getQueryBuilder();
        QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
        domain.where(domain.get("recordKey").equal(domain.param("recordKey")));

        List<HudiRecord> mutations = new ArrayList<>();

        while (statusIterator.hasNext()) {
          WriteStatus writeStatus = statusIterator.next();

          try {
            long numOfInserts = writeStatus.getStat().getNumInserts();
            LOG.debug("Num of inserts in this WriteStatus: " + numOfInserts);

            for (HoodieRecord currentRecord : writeStatus.getWrittenRecords()) {
              if (!writeStatus.isErrored(currentRecord.getKey())) {
                Option<HoodieRecordLocation> loc = currentRecord.getNewLocation();
                if (loc.isPresent()) {
                  if (currentRecord.getCurrentLocation() != null) {
                    // This is an update, no need to update index
                    continue;
                  }

                  HudiRecord hudiRecord = session.newInstance(HudiRecord.class);
                  hudiRecord.setRecordKey(currentRecord.getRecordKey().getBytes());
                  hudiRecord.setCommitTs(Long.parseLong(loc.get().getInstantTime()));
                  hudiRecord.setPartitionPath(currentRecord.getPartitionPath());
                  hudiRecord.setFileName(loc.get().getFileId());

                  mutations.add(hudiRecord);
                } else {
                  Query<HudiRecord> query = session.createQuery(domain);
                  query.setParameter("recordKey", currentRecord.getRecordKey().getBytes());
                  query.deletePersistentAll();
                }
              }
              if (mutations.size() < config.getRonDBIndexBatchSize()) {
                continue;
              }
              session.savePersistentAll(mutations);
              mutations.clear();
            }
            session.savePersistentAll(mutations);
          } catch (Exception e) {
            Exception we = new Exception("Error updating index for " + writeStatus, e);
            LOG.error(we);
            writeStatus.setGlobalError(we);
          }
          writeStatusList.add(writeStatus);
        }
      }

      final long endPutsTime = DateTime.now().getMillis();
      LOG.debug("rondb puts task time for this task: " + (endPutsTime - startTimeForPutsTask));
      return writeStatusList.iterator();
    };
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    if (!config.getRonDBIndexRollbackSync()) {
      // Default Rollback in RonDBIndex is managed via method {@link #checkIfValidCommit()}
      return true;
    }

    synchronized (SparkHoodieRonDBIndex.class) {
      init();
    }

    try (Session session = entitySessionFactory.getSession()) {
      QueryBuilder builder = session.getQueryBuilder();
      QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
      domain.where(domain.get("commitTs").greaterThan(domain.param("commitTs")));

      Query<HudiRecord> query = session.createQuery(domain);
      query.setParameter("commitTs", Long.parseLong(instantTime));
      query.deletePersistentAll();
    } catch (Exception e) {
      LOG.error("rondb index roll back failed", e);
      return false;
    }
    return true;

  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in RonDB already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}