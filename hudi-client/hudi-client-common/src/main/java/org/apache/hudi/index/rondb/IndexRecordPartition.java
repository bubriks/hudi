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
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.io.Serializable;
import java.util.Collection;

@Entity
@Table(name = "index_record_partition",
        indexes = @Index(name = "record_partition_index", columnList = "partition_path"),
        uniqueConstraints = @UniqueConstraint(columnNames = {"partition_path"}))
public class IndexRecordPartition implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Long id;

  @Column(name = "partition_path", nullable = false)
  private String partitionPath;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "recordPartition")
  private Collection<IndexRecordFile> files;

  public Long getId() {
    return id;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public Collection<IndexRecordFile> getFiles() {
    return files;
  }

  public void setFiles(Collection<IndexRecordFile> files) {
    this.files = files;
  }
}
