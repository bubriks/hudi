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

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.io.Serializable;
import java.util.Collection;

@Entity
@Table(name = "index_record_location", uniqueConstraints = {
  @UniqueConstraint(columnNames = {"file_id", "partition_path"})
})
@NamedQueries({
  @NamedQuery(name = "IndexRecord.findByFileIdAndPartitionPath",
    query = "SELECT location FROM IndexRecordLocation location WHERE "
      + "location.fileId = :fileId AND location.partitionPath = :partitionPath")})
public class IndexRecordLocation implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Basic(optional = false)
  @Column(name = "id")
  private Long id;

  @Column(name = "file_id")
  @Basic(optional = false)
  private String fileId;

  @Column(name = "partition_path")
  @Basic(optional = false)
  private String partitionPath;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "indexRecordLocation")
  private Collection<IndexRecord> records;

  public Long getId() {
    return id;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public Collection<IndexRecord> getIndexRecords() {
    return records;
  }

  public void setIndexRecords(Collection<IndexRecord> records) {
    this.records = records;
  }
}
