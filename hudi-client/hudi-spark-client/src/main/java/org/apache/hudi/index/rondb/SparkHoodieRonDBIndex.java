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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.DateTime;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Hoodie Index implementation backed by RonDB.
 */
public class SparkHoodieRonDBIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieRonDBIndex.class);
  private static transient Thread shutdownThread;
  private static EntityManagerFactory entityManagerFactory;

  public SparkHoodieRonDBIndex(HoodieWriteConfig config) {
    super(config);
    init();
  }

  private void init() {
    if (entityManagerFactory == null || !entityManagerFactory.isOpen()) {
      entityManagerFactory = Persistence.createEntityManagerFactory("index", config.getProps());
      addShutDownHook();
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
          entityManagerFactory.close();
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

      EntityManager entityManager;
      synchronized (SparkHoodieRonDBIndex.class) {
        init();
        entityManager = entityManagerFactory.createEntityManager();
      }

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      // Do the tagging.
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord currentRecord = hoodieRecordIterator.next();

        IndexRecord record;
        try {
          record = entityManager.createNamedQuery("IndexRecord.findByKey", IndexRecord.class)
                  .setParameter("key", currentRecord.getRecordKey()).getSingleResult();
        } catch (NoResultException noResultException) {
          taggedRecords.add(currentRecord);
          continue;
        }

        // get info
        String key = record.id.getKey();
        String commitTs = record.id.getCommitTimeString();
        String fileId = record.getFileId();
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
          assert (currentRecord.getRecordKey().contentEquals(key));
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

    Integer multiPutBatchSize = config.getRonDBIndexGetBatchSize();
    return (partitionNum, statusIterator) -> {

      List<WriteStatus> writeStatusList = new ArrayList<>();

      EntityManager entityManager;
      synchronized (SparkHoodieRonDBIndex.class) {
        init();
        entityManager = entityManagerFactory.createEntityManager();
      }

      // Start transaction
      EntityTransaction entityTransaction = entityManager.getTransaction();
      entityTransaction.begin();

      final long startTimeForPutsTask = DateTime.now().getMillis();
      LOG.info("startTimeForPutsTask for this task: " + startTimeForPutsTask);

      while (statusIterator.hasNext()) {
        WriteStatus writeStatus = statusIterator.next();
        try {
          long numOfInserts = writeStatus.getStat().getNumInserts();
          LOG.info("Num of inserts in this WriteStatus: " + numOfInserts);
          LOG.info("multiPutBatchSize for this job: " + multiPutBatchSize);

          for (HoodieRecord currentRecord : writeStatus.getWrittenRecords()) {
            if (!writeStatus.isErrored(currentRecord.getKey())) {
              Option<HoodieRecordLocation> loc = currentRecord.getNewLocation();
              if (loc.isPresent()) {
                if (currentRecord.getCurrentLocation() != null) {
                  // This is an update, no need to update index
                  continue;
                }

                // Create and set values for new customer
                IndexRecord record = new IndexRecord();
                record.id.setKey(currentRecord.getRecordKey());
                record.id.setCommitTime(loc.get().getInstantTime());
                record.setFileId(loc.get().getFileId());
                record.setPartitionPath(currentRecord.getPartitionPath());

                entityManager.persist(record);
              } else {
                // Delete existing index for a deleted record
                entityManager.createNamedQuery("IndexRecord.removeByKey", IndexRecord.class)
                        .setParameter("key", currentRecord.getRecordKey()).executeUpdate();
              }
            }
          }
        } catch (Exception e) {
          entityTransaction.rollback();

          Exception we = new Exception("Error updating index for " + writeStatus, e);
          LOG.error(we);
          writeStatus.setGlobalError(we);
          throw we;
        }
        writeStatusList.add(writeStatus);
      }
      entityTransaction.commit();
      entityManager.close();

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

    EntityManager entityManager;
    synchronized (SparkHoodieRonDBIndex.class) {
      init();
      entityManager = entityManagerFactory.createEntityManager();
    }

    // Start transaction
    EntityTransaction entityTransaction = entityManager.getTransaction();
    entityTransaction.begin();

    try {
      Date date = HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime);
      entityManager.createNamedQuery("IndexRecord.removeByTimestamp", IndexRecord.class)
              .setParameter("timestamp", date).executeUpdate();

      entityTransaction.commit();
    } catch (Exception e) {
      entityTransaction.rollback();
      LOG.error("rondb index roll back failed", e);
      return false;
    } finally {
      entityManager.close();
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
