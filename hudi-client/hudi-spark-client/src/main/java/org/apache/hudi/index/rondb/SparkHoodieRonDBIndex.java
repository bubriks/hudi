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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RateLimiter;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBIndex.class);
  private static Connection rondbConnection = null;
  private static transient Thread shutdownThread;

  private final String recordKey = "record_key";
  private final String recordCommitTimestamp = "commit_ts";
  private final String recordPartitionPath = "partition_path";
  private final String recordFileId = "file_id";
  private final String tableName;

  public SparkHoodieRonDBIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getRonDBTable();
    init();
    addShutDownHook();
  }

  private void init() {
    rondbConnection = getRonDBConnection();
    try {
      setUpEnvironment();
      rondbConnection.setCatalog(config.getRonDBDatabase());
    } catch (SQLException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              "problem initializing RonDB: " + e.getMessage());
    }
  }

  private void setUpEnvironment() throws SQLException {
    Statement stmt = rondbConnection.createStatement();

    String sqlTemplate = "CREATE DATABASE IF NOT EXISTS %1$s";
    String sql = String.format(sqlTemplate, config.getRonDBDatabase());
    stmt.execute(sql);

    sqlTemplate = "USE %1$s";
    sql = String.format(sqlTemplate, config.getRonDBDatabase());
    stmt.execute(sql);

    sqlTemplate = "CREATE TABLE IF NOT EXISTS %1$s (\n"
                + "  %2$s VARBINARY(255) NOT NULL, \n"
                + "  %3$s TIMESTAMP NOT NULL, \n"
                + "  %4$s VARCHAR(255) NOT NULL, \n"
                + "  %5$s VARCHAR(255) NOT NULL, \n"
                + "  PRIMARY KEY (%2$s, %3$s) \n"
                + ")";
    sql = String.format(sqlTemplate, tableName, recordKey, recordCommitTimestamp, recordPartitionPath, recordFileId);
    stmt.execute(sql);

    stmt.close();
  }

  private Connection getRonDBConnection() {
    try {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver());
      return DriverManager.getConnection(config.getRonDBUrl(), config.getRonDBUsername(), config.getRonDBPassword());
    } catch (SQLException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              config.getRonDBUrl());
    }
  }

  /**
   * Since we are sharing the RonDBConnection across tasks in a JVM, make sure the RonDBConnection is closed when JVM
   * exits.
   */
  private void addShutDownHook() {
    if (null == shutdownThread) {
      shutdownThread = new Thread(() -> {
        try {
          rondbConnection.close();
        } catch (Exception e) {
          LOG.info("Problem closing RonDB connection");
        }
      });
      Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
  }

  private PreparedStatement generateGetStatement(String key) throws SQLException {
    String sqlTemplate = "SELECT * "
            + "FROM %1$s s1 "
            + "JOIN ( "
            + "  SELECT %2$s, MAX(%3$s) AS %3$s "
            + "  FROM %1$s "
            + "  GROUP BY %2$s) AS s2 "
            + "  ON s1.%2$s = s2.%2$s AND s1.%3$s = s2.%3$s "
            + "WHERE s1.%2$s = ?";
    String sql = String.format(sqlTemplate, tableName, recordKey, recordCommitTimestamp, recordPartitionPath, recordFileId);

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setBytes(1, key.getBytes());
    return p;
  }

  private PreparedStatement generateInsertStatement(String key, String partitionPath, String fileName, String commitTs)
          throws SQLException, ParseException {
    String sqlTemplate = "INSERT INTO %1$s (%2$s, %3$s, %4$s, %5$s) VALUES (?, ?, ?, ?)";
    String sql = String.format(sqlTemplate, tableName, recordKey, recordCommitTimestamp, recordPartitionPath, recordFileId);

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setBytes(1, key.getBytes());
    p.setTimestamp(2, new Timestamp(HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTs).getTime()));
    p.setString(3, partitionPath);
    p.setString(4, fileName);
    return p;
  }

  private PreparedStatement generateDeleteStatement(String key)
          throws SQLException {
    String sqlTemplate = "DELETE FROM %1$s WHERE %2$s = ?";
    String sql = String.format(sqlTemplate, tableName, recordKey, recordCommitTimestamp, recordPartitionPath, recordFileId);

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setBytes(1, key.getBytes());
    return p;
  }

  private PreparedStatement generateRoleBackStatement(String instantTime)
          throws SQLException, ParseException {
    String sqlTemplate = "DELETE FROM %1$s WHERE %3$s > ?";
    String sql = String.format(sqlTemplate, tableName, recordKey, recordCommitTimestamp, recordPartitionPath, recordFileId);

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setTimestamp(1, new Timestamp(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime()));
    return p;
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

    // `multiGetBatchSize` is intended to be a batch per 100ms. To create a rate limiter that measures
    // operations per second, we need to multiply `multiGetBatchSize` by 10.
    Integer multiGetBatchSize = config.getRonDBIndexGetBatchSize();
    return (partitionNum, hoodieRecordIterator) -> {

      boolean updatePartitionPath = config.getRonDBIndexUpdatePartitionPath();
      RateLimiter limiter = RateLimiter.create(multiGetBatchSize * 10, TimeUnit.SECONDS);
      // Grab the global RonDB connection
      synchronized (SparkHoodieRonDBIndex.class) {
        if (rondbConnection == null || rondbConnection.isClosed()) {
          rondbConnection = getRonDBConnection();
          rondbConnection.setCatalog(config.getRonDBDatabase());
        }
      }

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      List<PreparedStatement> statements = new ArrayList<>();
      List<HoodieRecord> currentBatchOfRecords = new LinkedList<>();
      // Do the tagging.
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        statements.add(generateGetStatement(rec.getRecordKey()));
        currentBatchOfRecords.add(rec);
        // iterator till we reach batch size
        if (hoodieRecordIterator.hasNext() && statements.size() < multiGetBatchSize) {
          continue;
        }
        // get results for batch from RonDB
        List<ResultSet> results = executeQuery(statements, limiter);
        for (ResultSet result : results) {
          // first, attempt to grab location from RonDB
          HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
          if (!result.next()) {
            taggedRecords.add(currentRecord);
            continue;
          }

          // get info
          String key = new String(result.getBytes(recordKey));
          String commitTs = HoodieActiveTimeline.COMMIT_FORMATTER.format(result.getTimestamp(recordCommitTimestamp));
          String fileId = result.getString(recordFileId);
          String partitionPath = result.getString(recordPartitionPath);
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
            assert (currentRecord.getRecordKey().contentEquals(key));
          }
        }
      }
      return taggedRecords.iterator();
    };
  }

  private List<ResultSet> executeQuery(List<PreparedStatement> statements, RateLimiter limiter) throws SQLException {
    List<ResultSet> results = new ArrayList();
    if (statements.size() > 0) {
      limiter.tryAcquire(statements.size());
      for (PreparedStatement statement : statements) {
        ResultSet resultSet = statement.executeQuery();
        results.add(resultSet);
      }
    }
    // clear statements to be GC'd
    statements.clear();
    return results;
  }

  private void execute(List<PreparedStatement> statements, RateLimiter limiter) throws SQLException {
    rondbConnection.setAutoCommit(false);
    if (statements.size() > 0) {
      limiter.tryAcquire(statements.size());
      for (PreparedStatement statement : statements) {
        statement.execute();
      }
    }
    rondbConnection.commit();
    statements.clear();
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

    Integer multiPutBatchSize = config.getRonDBIndexGetBatchSize();
    return (partitionNum, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();
      // Grab the global RonDB connection
      synchronized (SparkHoodieRonDBIndex.class) {
        if (rondbConnection == null || rondbConnection.isClosed()) {
          rondbConnection = getRonDBConnection();
          rondbConnection.setCatalog(config.getRonDBDatabase());
        }
      }
      final long startTimeForPutsTask = DateTime.now().getMillis();
      LOG.info("startTimeForPutsTask for this task: " + startTimeForPutsTask);

      final RateLimiter limiter = RateLimiter.create(multiPutBatchSize, TimeUnit.SECONDS);
      while (statusIterator.hasNext()) {
        WriteStatus writeStatus = statusIterator.next();
        List<PreparedStatement> mutations = new ArrayList<>();
        try {
          long numOfInserts = writeStatus.getStat().getNumInserts();
          LOG.info("Num of inserts in this WriteStatus: " + numOfInserts);
          LOG.info("multiPutBatchSize for this job: " + multiPutBatchSize);
          // Create a rate limiter that allows `multiPutBatchSize` operations per second
          // Any calls beyond `multiPutBatchSize` within a second will be rate limited
          for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
            if (!writeStatus.isErrored(rec.getKey())) {
              Option<HoodieRecordLocation> loc = rec.getNewLocation();
              if (loc.isPresent()) {
                if (rec.getCurrentLocation() != null) {
                  // This is an update, no need to update index
                  continue;
                }

                PreparedStatement statement = generateInsertStatement(rec.getRecordKey(), rec.getPartitionPath(),
                        loc.get().getFileId(), loc.get().getInstantTime());
                mutations.add(statement);
              } else {
                // Delete existing index for a deleted record
                PreparedStatement statement = generateDeleteStatement(rec.getRecordKey());
                mutations.add(statement);
              }
            }
            if (mutations.size() < multiPutBatchSize) {
              continue;
            }
            execute(mutations, limiter);
          }
          // process remaining puts and deletes, if any
          execute(mutations, limiter);
        } catch (Exception e) {
          Exception we = new Exception("Error updating index for " + writeStatus, e);
          LOG.error(we);
          writeStatus.setGlobalError(we);
          throw we;
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
    boolean rollbackSync = config.getRonDBIndexRollbackSync();

    if (!rollbackSync) {
      // Default Rollback in RonDBIndex is managed via method {@link #checkIfValidCommit()}
      return true;
    }

    try {
      synchronized (SparkHoodieRonDBIndex.class) {
        if (rondbConnection == null || rondbConnection.isClosed()) {
          rondbConnection = getRonDBConnection();
          rondbConnection.setCatalog(config.getRonDBDatabase());
        }
      }
      PreparedStatement ps = generateRoleBackStatement(instantTime);
      ps.execute();
    } catch (SQLException | ParseException e) {
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
