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

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

@Embeddable
public class IndexRecordId implements Serializable {

  @Column(name = "key_id", insertable = false, updatable = false)
  private Long keyId;

  @Column(name = "commit_ts")
  @Temporal(TemporalType.TIMESTAMP)
  private Date commitTimestamp;

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
}