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

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

@Entity
@Table(name = "index_record",
        indexes = @Index(name = "record_index", columnList = "key_id"))
@NamedQueries({
        @NamedQuery(name = "Record.findByKey",
                query = "SELECT record FROM IndexRecord record WHERE record.recordKey.key = :key ORDER BY record.commitTimestamp DESC"),
        @NamedQuery(name = "Record.removeByTimestamp",
                query = "DELETE FROM IndexRecord record WHERE record.commitTimestamp > :timestamp")})
public class IndexRecord implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Long id;

  @Column(name = "commit_ts")
  @Temporal(TemporalType.TIMESTAMP)
  private Date commitTimestamp;

  @JoinColumn(name = "key_id", referencedColumnName = "id", nullable = false)
  @ManyToOne(cascade = CascadeType.PERSIST)
  private IndexRecordKey recordKey;

  @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false)
  @ManyToOne(cascade = CascadeType.PERSIST)
  private IndexRecordFile recordFile;

  public IndexRecord() {}

  public IndexRecord(String commitTimestamp, IndexRecordKey recordKey, IndexRecordFile recordFile)
          throws ParseException {
    setCommitTime(commitTimestamp);
    setRecordKey(recordKey);
    setRecordFile(recordFile);
  }

  public Date getCommitTime() {
    return commitTimestamp;
  }

  public String getCommitTimeString() {
    return HoodieActiveTimeline.COMMIT_FORMATTER.format(commitTimestamp);
  }

  public void setCommitTime(Date commitTimestamp) {
    this.commitTimestamp = commitTimestamp;
  }

  public void setCommitTime(String commitTimestamp) throws ParseException {
    this.commitTimestamp = HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTimestamp);
  }

  public IndexRecordKey getRecordKey() {
    return recordKey;
  }

  public void setRecordKey(IndexRecordKey recordKey) {
    this.recordKey = recordKey;
  }

  public IndexRecordFile getRecordFile() {
    return recordFile;
  }

  public void setRecordFile(IndexRecordFile recordFile) {
    this.recordFile = recordFile;
  }
}