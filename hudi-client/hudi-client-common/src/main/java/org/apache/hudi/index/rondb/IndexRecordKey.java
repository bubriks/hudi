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
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Collection;

@Entity
@Table(name = "record_key",
       indexes = @Index(name = "record_key_index", columnList = "key"))
@NamedQueries({
        @NamedQuery(name = "RecordKey.getByValue",
                query = "SELECT key FROM IndexRecordKey key WHERE key.value = :value"),
        @NamedQuery(name = "RecordKey.removeByValue",
                query = "DELETE FROM IndexRecordKey key WHERE key.value = :value")})
public class IndexRecordKey implements Serializable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Long id;

  @Column(name = "value", columnDefinition = "VARBINARY(255)", nullable = false)
  private byte[] value;

  @OneToMany(cascade = CascadeType.ALL, mappedBy = "key")
  private Collection<IndexRecord> records;

  public IndexRecordKey() {}

  public IndexRecordKey(String value) {
    setValueString(value);
  }

  public Long getId() {
    return id;
  }

  public String getValueString() {
    return new String(value);
  }

  public void setValueString(String value) {
    this.value = value.getBytes();
  }

  public Collection<IndexRecord> getRecords() {
    return records;
  }

  public void setRecords(Collection<IndexRecord> records) {
    this.records = records;
  }
}
