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

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBAdvancedIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBAdvancedIndex.class);
  private static SessionFactory entitySessionFactory;
  private static transient Thread shutdownThread;

  private final String recordKey = "record_key";
  private final String commitTimestamp = "commit_ts";
  private final String partition = "partition_path";
  private final String fileName = "file_name";

  private final String indexRecordKey = "idx_record_key";

  private final String tableName;
  private final String databaseName;

  public SparkHoodieRonDBAdvancedIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = "hudi_record";
    this.databaseName = "hudi";
    init();
  }

  private void init() {
    if (entitySessionFactory == null || entitySessionFactory.getSession().isClosed()) {
      setUpEnvironment();
      entitySessionFactory = ClusterJHelper.getSessionFactory(config.getProps());
      addShutDownHook();
    }
  }

  private void setUpEnvironment() {
    try {
      Statement stmt = getRonDBConnection().createStatement();

      String sqlTemplate = "CREATE DATABASE IF NOT EXISTS %1$s";
      String sql = String.format(sqlTemplate, databaseName);
      stmt.execute(sql);

      sqlTemplate = "USE %1$s";
      sql = String.format(sqlTemplate, databaseName);
      stmt.execute(sql);

      sqlTemplate = "CREATE TABLE IF NOT EXISTS %1$s (\n"
              + "  %2$s VARBINARY(255) NOT NULL, \n"
              + "  %3$s TIMESTAMP NOT NULL, \n"
              + "  %4$s VARCHAR(255) NOT NULL, \n"
              + "  %5$s VARCHAR(255) NOT NULL, \n"
              + "  KEY %6$s (%2$s), \n"
              + "  PRIMARY KEY (%2$s, %3$s) \n"
              + ") ENGINE=NDBCLUSTER";
      sql = String.format(sqlTemplate, tableName, recordKey, commitTimestamp, partition, fileName, indexRecordKey);
      stmt.execute(sql);

      stmt.close();
    } catch (SQLException e) {
      throw new HoodieIndexException(e.getMessage());
    }
  }

  private Connection getRonDBConnection() {
    try {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver());
      return DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "");
    } catch (SQLException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
          "jdbc:mysql://localhost:3306");
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
          LOG.info("Problem closing RonDB connection");
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
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
                                              HoodieEngineContext context,
                                              HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    return recordRDD.mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true);
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
          HoodieTableMetaClient metaClient) {

    return (partitionNum, hoodieRecordIterator) -> {

      boolean updatePartitionPath = config.getRonDBIndexUpdatePartitionPath();

      Session session;
      synchronized (SparkHoodieRonDBIndex.class) {
        init();
        session = entitySessionFactory.getSession();
      }

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      // Do the tagging.
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord currentRecord = hoodieRecordIterator.next();

        QueryBuilder builder = session.getQueryBuilder();
        QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
        domain.where(domain.get("recordKey").equal(domain.param("recordKey")));

        Query<HudiRecord> query = session.createQuery(domain);
        query.setParameter("recordKey", currentRecord.getRecordKey().getBytes());
        query.setOrdering(Query.Ordering.DESCENDING, "commitTs");
        query.setLimits(0, 1);
        List<HudiRecord> results = query.getResultList();

        HudiRecord record;
        if (!results.isEmpty()) {
          record = results.get(0);
        } else {
          taggedRecords.add(currentRecord);
          continue;
        }

        String keyFromResult = new String(record.getRecordKey());
        String commitTs = HoodieActiveTimeline.COMMIT_FORMATTER.format(record.getCommitTs());
        String fileId = record.getFileName();
        String partitionPath = record.getPartitionPath();

        if (!checkIfValidCommit(metaClient, commitTs)) {
          // if commit is invalid, treat this as a new taggedRecord
          taggedRecords.add(currentRecord);
          continue;
        }

        // check whether to do partition change processing
        if (updatePartitionPath && !partitionPath.equals(currentRecord.getPartitionPath())) {
          // delete partition old data record
          HoodieRecord emptyRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                  new EmptyHoodieRecordPayload());
          emptyRecord.unseal();
          emptyRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
          emptyRecord.seal();
          // insert partition new data record
          currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), currentRecord.getPartitionPath()),
                  currentRecord.getData());
          taggedRecords.add(emptyRecord);
          taggedRecords.add(currentRecord);
        } else {
          currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                  currentRecord.getData());
          currentRecord.unseal();
          currentRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
          currentRecord.seal();
          taggedRecords.add(currentRecord);
          // the key from Result and the key being processed should be same
          assert (currentRecord.getRecordKey().contentEquals(keyFromResult));
        }
      }
      return taggedRecords.iterator();
    };
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context,
                                             HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>,
                                                     JavaRDD<WriteStatus>> hoodieTable) {
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(SparkMemoryUtils.getWriteStatusStorageLevel(config.getProps()));
    return writeStatusJavaRDD;
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {

    return (partition, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();

      Session session;
      synchronized (SparkHoodieRonDBIndex.class) {
        init();
        session = entitySessionFactory.getSession();
      }

      final long startTimeForPutsTask = DateTime.now().getMillis();
      LOG.info("startTimeForPutsTask for this task: " + startTimeForPutsTask);

      while (statusIterator.hasNext()) {
        WriteStatus writeStatus = statusIterator.next();
        List<HudiRecord> mutations = new ArrayList<>();

        // Start transaction
        Transaction transaction = session.currentTransaction();
        transaction.begin();

        try {
          long numOfInserts = writeStatus.getStat().getNumInserts();
          LOG.info("Num of inserts in this WriteStatus: " + numOfInserts);

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
                hudiRecord.setCommitTs(HoodieActiveTimeline.COMMIT_FORMATTER.parse(loc.get().getInstantTime()).getTime());
                hudiRecord.setPartitionPath(currentRecord.getPartitionPath());
                hudiRecord.setFileName(loc.get().getFileId());

                mutations.add(hudiRecord);
              } else {
                QueryBuilder builder = session.getQueryBuilder();
                QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
                domain.where(domain.get("recordKey").equal(domain.param("recordKey")));

                Query<HudiRecord> query = session.createQuery(domain);
                query.setParameter("recordKey", currentRecord.getRecordKey().getBytes());
                query.deletePersistentAll();
              }
            }
          }
          session.makePersistentAll(mutations);
          transaction.commit();
        } catch (Exception e) {
          Exception we = new Exception("Error updating index for " + writeStatus, e);
          LOG.error(we);
          writeStatus.setGlobalError(we);
          transaction.rollback();
        }
        writeStatusList.add(writeStatus);
      }

      final long endPutsTime = DateTime.now().getMillis();
      LOG.info("rondb puts task time for this task: " + (endPutsTime - startTimeForPutsTask));
      return writeStatusList.iterator();
    };
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    if (!config.getRonDBIndexRollbackSync()) {
      // Default Rollback in RonDBIndex is managed via method {@link #checkIfValidCommit()}
      return true;
    }

    Session session;
    synchronized (SparkHoodieRonDBIndex.class) {
      init();
      session = entitySessionFactory.getSession();
    }

    // Start transaction
    Transaction transaction = session.currentTransaction();
    transaction.begin();

    try {
      Date date = HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime);

      QueryBuilder builder = session.getQueryBuilder();
      QueryDomainType<HudiRecord> domain = builder.createQueryDefinition(HudiRecord.class);
      domain.where(domain.get("commitTs").greaterThan(domain.param("commitTs")));

      Query<HudiRecord> query = session.createQuery(domain);
      query.setParameter("commitTs", date);
      query.deletePersistentAll();

      transaction.commit();
    } catch (Exception e) {
      transaction.rollback();
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
