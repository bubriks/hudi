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
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkMemoryUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBAdvancedIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBAdvancedIndex.class);
  private static Connection rondbConnection = null;
  private static transient Thread shutdownThread;

  private final String recordKey = "record_key";
  private final String commitTimestamp = "commit_ts";
  private final String partition = "partition_path";
  private final String fileName = "file_name";

  private final String tableName;

  public SparkHoodieRonDBAdvancedIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getRonDBTable();
    init();
    addShutDownHook();

    System.setProperty("java.library.path", "/srv/hops/mysql-21.04.2/lib/");

    Properties properties = new Properties();
    properties.put("com.mysql.clusterj.connectstring", "localhost:1186");
    properties.put("com.mysql.clusterj.database", "hudi");
    SessionFactory sessionFactory = ClusterJHelper.getSessionFactory(properties);
    Session session = sessionFactory.getSession();

    List<HudiIndex> insertInstances = new ArrayList<HudiIndex>();
    HudiIndex hudiIndex = session.newInstance(HudiIndex.class);
    hudiIndex.setRecordKey("1");
    hudiIndex.setCommitTs("2");
    hudiIndex.setPartitionPath("3");
    hudiIndex.setFileName("4");
    insertInstances.add(hudiIndex);
    session.makePersistentAll(insertInstances);
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

    String query = "CREATE DATABASE IF NOT EXISTS " + config.getRonDBDatabase();
    stmt.execute(query);

    query = "USE " + config.getRonDBDatabase();
    stmt.execute(query);

    query = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
            + "  `" + recordKey + "` varchar(50)  NOT NULL, \n"
            + "  `" + commitTimestamp + "` varchar(14)  NOT NULL, \n"
            + "  `" + partition + "` varchar(50) NOT NULL, \n"
            + "  `" + fileName + "` varchar(50) NOT NULL, \n"
            + "   PRIMARY KEY (" + recordKey + ") \n"
            + ")";
    stmt.execute(query);

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
    String sql = "SELECT * FROM " + tableName + " WHERE " + recordKey + " = ?";

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setString(1, key);
    return p;
  }

  private PreparedStatement generateUpsertStatement(String key, String partitionPath, String fileId, String commitTs)
          throws SQLException {
    String sql = "REPLACE INTO " + tableName + " (" + recordKey + ", " + partition + ", " + fileName + ", " + commitTimestamp + ")"
            + "VALUES (?, ?, ?, ?)";

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setString(1, key);
    p.setString(2, partitionPath);
    p.setString(3, fileId);
    p.setString(4, commitTs);
    return p;
  }

  private PreparedStatement generateDeleteStatement(String key)
          throws SQLException {
    String sql = "DELETE FROM " + tableName + " WHERE " + recordKey + " = ?";

    PreparedStatement p = rondbConnection.prepareStatement(sql);
    p.setString(1, key);
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

    return (partitionNum, hoodieRecordIterator) -> {

      // Grab the global RonDB connection
      synchronized (SparkHoodieRonDBAdvancedIndex.class) {
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
        if (hoodieRecordIterator.hasNext()) {
          continue;
        }

        // get results for batch from RonDB
        List<ResultSet> results = executeQuery(statements);

        // clear statements
        statements.clear();

        for (ResultSet result : results) {
          // first, attempt to grab location from RonDB
          HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
          if (!result.next()) {
            taggedRecords.add(currentRecord);
            continue;
          }

          // get info
          String keyFromResult = result.getString(recordKey);
          String commitTs = result.getString(commitTimestamp);
          String fileId = result.getString(fileName);
          String partitionPath = result.getString(partition);

          if (!checkIfValidCommit(metaClient, commitTs)) {
            // if commit is invalid, treat this as a new taggedRecord
            taggedRecords.add(currentRecord);
            continue;
          }

          // tag record
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

  private List<ResultSet> executeQuery(List<PreparedStatement> statements) throws SQLException {
    List<ResultSet> results = new ArrayList();
    for (PreparedStatement statement : statements) {
      ResultSet resultSet = statement.executeQuery();
      results.add(resultSet);
    }
    return results;
  }

  private void execute(List<PreparedStatement> statements) throws SQLException {
    for (PreparedStatement statement : statements) {
      statement.execute();
    }
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

    return (partitionNum, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();

      // Grab the global RonDB connection
      synchronized (SparkHoodieRonDBAdvancedIndex.class) {
        if (rondbConnection == null || rondbConnection.isClosed()) {
          rondbConnection = getRonDBConnection();
          rondbConnection.setCatalog(config.getRonDBDatabase());
        }
      }

      final long startTimeForPutsTask = DateTime.now().getMillis();
      LOG.info("startTimeForPutsTask for this task: " + startTimeForPutsTask);

      while (statusIterator.hasNext()) {

        WriteStatus writeStatus = statusIterator.next();
        List<PreparedStatement> mutations = new ArrayList<>();

        try {
          long numOfInserts = writeStatus.getStat().getNumInserts();
          LOG.info("Num of inserts in this WriteStatus: " + numOfInserts);

          for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
            if (!writeStatus.isErrored(rec.getKey())) {
              Option<HoodieRecordLocation> loc = rec.getNewLocation();
              if (loc.isPresent()) {
                if (rec.getCurrentLocation() != null) {
                  // This is an update, no need to update index
                  continue;
                }

                PreparedStatement statement = generateUpsertStatement(rec.getRecordKey(), rec.getPartitionPath(),
                        loc.get().getFileId(), loc.get().getInstantTime());
                mutations.add(statement);
              } else {
                // Delete existing index for a deleted record
                PreparedStatement statement = generateDeleteStatement(rec.getRecordKey());
                mutations.add(statement);
              }
            }
          }

          // process puts and deletes, if any
          rondbConnection.setAutoCommit(false);
          execute(mutations);
          rondbConnection.commit();
        } catch (Exception e) {
          Exception we = new Exception("Error updating index for " + writeStatus, e);
          LOG.error(we);
          writeStatus.setGlobalError(we);
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
    return false;
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
