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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Collection;

@Entity
@Table(name = "index_record_partition")
@NamedQueries({
    @NamedQuery(name = "RecordPartition.getByPath",
        query = "SELECT recordPartition FROM IndexRecordPartition recordPartition WHERE recordPartition.path = :path")})
public class IndexRecordPartition implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Long id;

  @Column(name = "path", nullable = false, unique = true)
  private String path;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "recordPartition")
  private Collection<IndexRecordFile> files;

  public IndexRecordPartition() {}

  public IndexRecordPartition(String path) {
    setPath(path);
  }

  public Long getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Collection<IndexRecordFile> getFiles() {
    return files;
  }

  public void setFiles(Collection<IndexRecordFile> files) {
    this.files = files;
  }
}