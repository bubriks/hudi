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

import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Column;

@PersistenceCapable(table = "hudi_index")
public interface HudiIndex {

  @PrimaryKey
  @Column(name = "record_key")
  String getRecordKey();

  void setRecordKey(String recordKey);

  @Column(name = "commit_ts")
  String getCommitTs();

  void setCommitTs(String commitTs);

  @Column(name = "partition_path")
  String getPartitionPath();

  void setPartitionPath(String partitionPath);

  @Column(name = "file_name")
  String getFileName();

  void setFileName(String fileName);
}