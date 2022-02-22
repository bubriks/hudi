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

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "hudi_index")
@NamedQueries({
        @NamedQuery(name = "IndexRecord.findByKey", query = "SELECT record FROM IndexRecord record WHERE record.id.key = :key ORDER BY record.id.commitTimestamp DESC"),
        @NamedQuery(name = "IndexRecord.removeByKey", query = "DELETE FROM IndexRecord record WHERE record.id.key = :key"),
        @NamedQuery(name = "IndexRecord.removeByTimestamp", query = "DELETE FROM IndexRecord record WHERE record.id.commitTimestamp > :timestamp")})
public class IndexRecord implements Serializable {

  @EmbeddedId
  IndexRecordId id = new IndexRecordId();

  @Column(name = "file_id", nullable = false)
  private String fileId;
  //todo split fileid and partition path into separate table

  @Column(name = "partition_path", nullable = false)
  private String partitionPath;

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
}
