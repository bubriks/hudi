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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Collection;

@Entity
@Table(name = "index_record_file",
        indexes = @Index(columnList = "file_id"))
@NamedQueries({
        @NamedQuery(name = "RecordFile.getByFileIdAndPartition",
                query = "SELECT file FROM IndexRecordFile file WHERE file.fileId = :fileId AND file.recordPartition = :partition")})
public class IndexRecordFile implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Long id;

  @Column(name = "file_id", nullable = false, length = 38, unique = true)
  private String fileId;

  @JoinColumn(name = "partition_id", referencedColumnName = "id", nullable = false)
  @ManyToOne(cascade = CascadeType.PERSIST)
  private IndexRecordPartition recordPartition;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "recordFile")
  private Collection<IndexRecord> records;

  public IndexRecordFile() {}

  public IndexRecordFile(String fileId, IndexRecordPartition recordPartition) {
    setFileId(fileId);
    setRecordPartition(recordPartition);
  }

  public Long getId() {
    return id;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public IndexRecordPartition getRecordPartition() {
    return recordPartition;
  }

  public void setRecordPartition(IndexRecordPartition recordPartition) {
    this.recordPartition = recordPartition;
  }

  public Collection<IndexRecord> getRecords() {
    return records;
  }

  public void setRecords(Collection<IndexRecord> records) {
    this.records = records;
  }
}
