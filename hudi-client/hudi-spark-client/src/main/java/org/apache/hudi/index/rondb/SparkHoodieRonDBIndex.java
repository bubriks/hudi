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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.index.HoodieIndex;
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
import java.sql.Timestamp;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;
import java.util.Date;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBIndex<T extends HoodieRecordPayload>
        extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBIndex.class);
  private static boolean initialized = false;

  //region SQL queries

  // Columns
  private final String recordKey = "record_key";
  private final String recordTimestamp = "commit_ts";
  private final String recordFile = "file_name";
  private final String recordPartition = "partition_path";
  private final String recordFileID = "file_id";
  private final String recordPartitionID = "partition_id";

  // Tables
  private final String recordTableName = "index_record";
  private final String recordFileTableName = "index_record_file";
  private final String recordPartitionTableName = "index_record_partition";

  private String fillQuery(String sqlTemplate) {
    return String.format(sqlTemplate,
            recordTableName, recordFileTableName, recordPartitionTableName,
            recordKey, recordTimestamp, recordFile, recordPartition, recordFileID, recordPartitionID);
  }

  // Templates
  String createRecordTableTemplate = "CREATE TABLE IF NOT EXISTS %1$s (\n"
          + "  %4$s VARBINARY(255) NOT NULL, \n"
          + "  %5$s TIMESTAMP(3) NOT NULL, \n"
          + "  %8$s INT NOT NULL, \n"
          + "  PRIMARY KEY (%4$s, %5$s), \n"
          + "  INDEX (%4$s), \n"
          + "  FOREIGN KEY (%8$s) REFERENCES %2$s(%8$s) \n"
          + ") ENGINE=NDBCLUSTER";

  String createFileTableTemplate = "CREATE TABLE IF NOT EXISTS %2$s (\n"
          + "  %8$s INT NOT NULL AUTO_INCREMENT, \n"
          + "  %6$s VARCHAR(38) NOT NULL, \n"
          + "  %9$s INT NOT NULL, \n"
          + "  PRIMARY KEY (%8$s), \n"
          + "  UNIQUE INDEX (%6$s), \n"
          + "  FOREIGN KEY (%9$s) REFERENCES %3$s(%9$s) \n"
          + ") ENGINE=NDBCLUSTER";

  String createPartitionTableTemplate = "CREATE TABLE IF NOT EXISTS %3$s (\n"
          + "  %9$s INT NOT NULL AUTO_INCREMENT, \n"
          + "  %7$s VARCHAR(255) NOT NULL, \n"
          + "  PRIMARY KEY (%9$s), \n"
          + "  UNIQUE INDEX (%7$s) \n"
          + ") ENGINE=NDBCLUSTER";

  String selectAllBatchTemplate = "SELECT r.%4$s, r.%5$s, f.%6$s, p.%7$s \n"
          + "FROM %1$s r \n"
          + "INNER JOIN %2$s f ON r.%8$s = f.%8$s \n"
          + "INNER JOIN %3$s p ON f.%9$s = p.%9$s \n"
          + "WHERE r.%4$s in (?) and \n"
          + "r.%5$s IN ( \n"
          + "  SELECT max(r1.%5$s) \n"
          + "  FROM %1$s r1 \n"
          + "  WHERE r.%4$s = r1.%4$s \n"
          + ")";

  String selectFileTemplate = "SELECT f.%8$s \n"
          + "FROM %2$s f \n"
          + "WHERE f.%6$s = ? \n"
          + "LIMIT 1";

  String selectPartitionTemplate = "SELECT p.%9$s \n"
          + "FROM %3$s p \n"
          + "WHERE p.%7$s = ? \n"
          + "LIMIT 1";

  String insertRecordTemplate = "INSERT INTO %1$s (%4$s, %5$s, %8$s) VALUES (?, ?, ?)";

  String insertFileTemplate = "INSERT IGNORE INTO %2$s (%6$s, %9$s) VALUES (?, ?)";

  String insertPartitionTemplate = "INSERT IGNORE INTO %3$s (%7$s) VALUES (?)";

  String deleteRecordTemplate = "DELETE \n"
          + "FROM %1$s r \n"
          + "WHERE r.%4$s = ?";

  String deleteRecordsTemplate = "DELETE \n"
          + "FROM %1$s r \n"
          + "WHERE r.%5$s > ?";
  //endregion

  public SparkHoodieRonDBIndex(HoodieWriteConfig config) {
    super(config);
    if (!initialized) {
      init();
      this.initialized = true;
    }
  }

  private void init() {
    try (Connection connection = getRonDBConnection()) {
      setUpEnvironment(connection);
    } catch (SQLException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              "Problem setting up databases for indexing: " + config.getRonDBIndexJDBCURL(), e);
    }
  }

  private Connection getRonDBConnection() throws SQLException {
    try {
      Class.forName(config.getRonDBIndexJDBCDriver());
      return DriverManager.getConnection(config.getRonDBIndexJDBCURL(), config.getRonDBIndexJDBC());
    }  catch (ClassNotFoundException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.RONDB,
              "Bad driver: " + config.getRonDBIndexJDBCDriver(), e);
    }
  }

  private void setUpEnvironment(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      //setup partition
      stmt.execute(fillQuery(createPartitionTableTemplate));

      //setup file
      stmt.execute(fillQuery(createFileTableTemplate));

      //setup record
      stmt.execute(fillQuery(createRecordTableTemplate));
    }
    LOG.debug("rondb create tables done");
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
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records, HoodieEngineContext context,
                                                 HoodieTable hoodieTable) {
    return HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(records)
            .mapPartitionsWithIndex(locationTagFunction(hoodieTable.getMetaClient()), true));
  }

  private PreparedStatement batchQueryPreparedStatement(Connection connection, List<String> keys, String query)
          throws SQLException {
    query = query.replaceAll("\\?", String.join(",", Collections.nCopies(keys.size(), "?")));
    PreparedStatement batchStatement = connection.prepareStatement(query);
    for (int i = 0; i < keys.size(); i++) {
      batchStatement.setBytes(i + 1, keys.get(i).getBytes());
    }
    return batchStatement;
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  private Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> locationTagFunction(
          HoodieTableMetaClient metaClient) {

    return (partitionNum, hoodieRecordIterator) -> {

      Map<String, HoodieRecord> recordMap = new HashMap<>();

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();

      final long startTimeForPutsTask = DateTime.now().getMillis();

      try (Connection connection = getRonDBConnection()) {

        // Do the tagging.
        while (hoodieRecordIterator.hasNext()) {
          HoodieRecord record = hoodieRecordIterator.next();
          recordMap.put(record.getRecordKey(), record);
          // iterator till we reach batch size
          if (hoodieRecordIterator.hasNext() && recordMap.size() < config.getRonDBIndexBatchSize()) {
            continue;
          }

          //for each batch
          try (PreparedStatement selectAllStatement = batchQueryPreparedStatement(
                  connection, recordMap.keySet().stream().collect(Collectors.toList()), fillQuery(selectAllBatchTemplate))) {
            ResultSet resultSet = selectAllStatement.executeQuery();

            while (resultSet.next()) {
              String resultKey = new String(resultSet.getBytes(recordKey));
              String resultCommitTimestamp = HoodieActiveTimeline.formatDate(new Date(resultSet.getTimestamp(recordTimestamp).getTime()));
              String resultFileName = resultSet.getString(recordFile);
              String resultPartition = resultSet.getString(recordPartition);

              HoodieRecord currentRecord = recordMap.remove(resultKey);

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
          }

          for (HoodieRecord currentRecord : recordMap.values()) {
            taggedRecords.add(currentRecord);
          }
          recordMap.clear();
        }
      }

      final long endPutsTime = DateTime.now().getMillis();
      LOG.debug("rondb tag task time: " + (endPutsTime - startTimeForPutsTask));
      return taggedRecords.iterator();
    };
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context,
                                                HoodieTable hoodieTable) {
    JavaRDD<WriteStatus> writeStatusRDD = HoodieJavaRDD.getJavaRDD(writeStatus);
    JavaRDD<WriteStatus> writeStatusJavaRDD = writeStatusRDD.mapPartitionsWithIndex(updateLocationFunction(), true);
    // caching the index updated status RDD
    writeStatusJavaRDD = writeStatusJavaRDD.persist(SparkMemoryUtils.getWriteStatusStorageLevel(config.getProps()));
    return HoodieJavaRDD.of(writeStatusJavaRDD);
  }

  private void insertRecordPartition(Connection connection, String partition) throws SQLException {
    try (PreparedStatement insertPartitionStatement = connection.prepareStatement(fillQuery(insertPartitionTemplate))) {
      insertPartitionStatement.setString(1, partition);
      insertPartitionStatement.execute();
    }
  }

  private int getIDByPartition(Connection connection, String partition) throws SQLException {
    try (PreparedStatement selectPartitionStatement = connection.prepareStatement(fillQuery(selectPartitionTemplate))) {
      selectPartitionStatement.setString(1, partition);
      ResultSet selectPartitionResultSet = selectPartitionStatement.executeQuery();

      if (!selectPartitionResultSet.next()) {
        insertRecordPartition(connection, partition);
        return getIDByPartition(connection, partition);
      } else {
        return selectPartitionResultSet.getInt(recordPartitionID);
      }
    }
  }

  private void insertRecordFile(Connection connection, String file, String partition)
          throws SQLException {
    try (PreparedStatement insertFileStatement = connection.prepareStatement(fillQuery(insertFileTemplate))) {
      int partitionId = getIDByPartition(connection, partition);

      insertFileStatement.setString(1, file);
      insertFileStatement.setInt(2, partitionId);
      insertFileStatement.execute();
    }
  }

  private int getIDByFile(Connection connection, String file, String partition) throws SQLException {
    try (PreparedStatement selectFileStatement = connection.prepareStatement(fillQuery(selectFileTemplate))) {
      selectFileStatement.setString(1, file);
      ResultSet selectFileResultSet = selectFileStatement.executeQuery();

      if (!selectFileResultSet.next()) {
        insertRecordFile(connection, file, partition);
        return getIDByFile(connection, file, partition);
      } else {
        return selectFileResultSet.getInt(recordFileID);
      }
    }
  }

  private Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> updateLocationFunction() {

    return (partitionNum, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();

      // to reduce need for communicating with the database
      Map<String, Integer> recordFileIdMap = new HashMap<>();

      final long startTimeForPutsTask = DateTime.now().getMillis();

      try (Connection connection = getRonDBConnection()) {

        try (PreparedStatement deleteRecordStatement = connection.prepareStatement(fillQuery(deleteRecordTemplate));
            PreparedStatement insertRecordStatement = connection.prepareStatement(fillQuery(insertRecordTemplate))) {

          int recordsInserted = 0;
          int recordsDeleted = 0;

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

                    int fileId;
                    if (recordFileIdMap.containsKey(loc.get().getFileId())) {
                      fileId = recordFileIdMap.get(loc.get().getFileId());
                    } else {
                      fileId = getIDByFile(connection, loc.get().getFileId(), currentRecord.getPartitionPath());
                      recordFileIdMap.put(loc.get().getFileId(), fileId);
                    }

                    insertRecordStatement.setBytes(1, currentRecord.getRecordKey().getBytes());
                    insertRecordStatement.setTimestamp(2,
                            new Timestamp(HoodieActiveTimeline.parseDateFromInstantTime(loc.get().getInstantTime()).getTime()));
                    insertRecordStatement.setInt(3, fileId);
                    insertRecordStatement.addBatch();

                    recordsInserted++;
                    if (recordsInserted >= config.getRonDBIndexBatchSize()) {
                      insertRecordStatement.executeBatch();
                      recordsInserted = 0;
                    }
                  } else {
                    deleteRecordStatement.setBytes(1, currentRecord.getRecordKey().getBytes());
                    deleteRecordStatement.addBatch();
                    recordsDeleted++;
                    if (recordsDeleted >= config.getRonDBIndexBatchSize()) {
                      deleteRecordStatement.executeBatch();
                      recordsDeleted = 0;
                    }
                  }
                }
              }
              //for remaining batch
              insertRecordStatement.executeBatch();
              deleteRecordStatement.executeBatch();
            } catch (Exception e) {
              Exception we = new Exception("Error updating index for " + writeStatus, e);
              LOG.error(we);
              writeStatus.setGlobalError(we);
            }

            recordFileIdMap.clear();
            writeStatusList.add(writeStatus);
          }
        }
      }

      final long endPutsTime = DateTime.now().getMillis();
      LOG.debug("rondb updated task time: " + (endPutsTime - startTimeForPutsTask));
      return writeStatusList.iterator();
    };
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    if (!config.getRonDBIndexRollbackSync()) {
      // Default Rollback in RonDBIndex is managed via method {@link #checkIfValidCommit()}
      return true;
    }

    try (Connection connection = getRonDBConnection()) {
      try (PreparedStatement deleteRecordsStatement = connection.prepareStatement(fillQuery(deleteRecordsTemplate))) {
        deleteRecordsStatement.setTimestamp(1,
                new Timestamp(HoodieActiveTimeline.parseDateFromInstantTime(instantTime).getTime()));
        deleteRecordsStatement.execute();
      }
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