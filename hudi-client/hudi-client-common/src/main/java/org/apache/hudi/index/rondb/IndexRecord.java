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

import javax.persistence.CascadeType;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.io.Serializable;
import java.text.ParseException;

@Entity
@Table(name = "index_record",
        indexes = @Index(name = "record_index", columnList = "key_id"))
@NamedQueries({
        @NamedQuery(name = "Record.findByKey",
                query = "SELECT record FROM IndexRecord record WHERE record.recordKey.key = :key ORDER BY record.id.commitTimestamp DESC"),
        @NamedQuery(name = "Record.removeByTimestamp",
                query = "DELETE FROM IndexRecord record WHERE record.id.commitTimestamp > :timestamp")})
public class IndexRecord implements Serializable {

  @EmbeddedId
  IndexRecordId id = new IndexRecordId();

  @JoinColumn(name = "key_id", referencedColumnName = "id", nullable = false)
  @ManyToOne(cascade = CascadeType.PERSIST)
  private IndexRecordKey recordKey;

  @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false)
  @ManyToOne(cascade = CascadeType.PERSIST)
  private IndexRecordFile recordFile;

  public IndexRecord() {}

  public IndexRecord(String commitTimestamp, IndexRecordKey recordKey, IndexRecordFile recordFile)
          throws ParseException {
    id.setCommitTime(commitTimestamp);
    setRecordKey(recordKey);
    setRecordFile(recordFile);
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