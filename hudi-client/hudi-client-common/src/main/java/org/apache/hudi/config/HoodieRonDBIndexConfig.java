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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@ConfigClassProperty(name = "RonDB Index Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control indexing behavior "
        + "(when RonDB based indexing is enabled), which tags incoming "
        + "records as either inserts or updates to older records.")
public class HoodieRonDBIndexConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> UPDATE_PARTITION_PATH_ENABLE = ConfigProperty
      .key("hoodie.rondb.index.update.partition.path")
      .defaultValue(false)
      .withDocumentation("Only applies if index type is RONDB. "
          + "When an already existing record is upserted to a new partition compared to whats in storage, "
          + "this config when set, will delete old record in old partition "
          + "and will insert it as new record in new partition.");

  public static final ConfigProperty<Boolean> ROLLBACK_SYNC_ENABLE = ConfigProperty
      .key("hoodie.rondb.hbase.rollback.sync")
      .defaultValue(false)
      .withDocumentation("When set to true, the rollback method will delete the last failed task index. "
          + "The default value is false. Because deleting the index will add extra load on the RonDB cluster for each rollback");

  /**
   * @deprecated Use {@link #UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String RONDB_INDEX_UPDATE_PARTITION_PATH = UPDATE_PARTITION_PATH_ENABLE.key();
  /**
   * @deprecated Use {@link #UPDATE_PARTITION_PATH_ENABLE} and its methods instead
   */
  @Deprecated
  public static final Boolean DEFAULT_RONDB_INDEX_UPDATE_PARTITION_PATH = UPDATE_PARTITION_PATH_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #ROLLBACK_SYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final String RONDB_INDEX_ROLLBACK_SYNC = ROLLBACK_SYNC_ENABLE.key();
  /**
   * @deprecated Use {@link #ROLLBACK_SYNC_ENABLE} and its methods instead
   */
  @Deprecated
  public static final Boolean DEFAULT_RONDB_INDEX_ROLLBACK_SYNC = ROLLBACK_SYNC_ENABLE.defaultValue();

  private HoodieRonDBIndexConfig() {
    super();
  }

  public static HoodieRonDBIndexConfig.Builder newBuilder() {
    return new HoodieRonDBIndexConfig.Builder();
  }

  public static class Builder {

    private final HoodieRonDBIndexConfig rondDBIndexConfig = new HoodieRonDBIndexConfig();

    public HoodieRonDBIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.rondDBIndexConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieRonDBIndexConfig.Builder fromProperties(Properties props) {
      this.rondDBIndexConfig.getProps().putAll(props);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexUpdatePartitionPath(boolean updatePartitionPath) {
      rondDBIndexConfig.setValue(UPDATE_PARTITION_PATH_ENABLE, String.valueOf(updatePartitionPath));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexRollbackSync(boolean rollbackSync) {
      rondDBIndexConfig.setValue(ROLLBACK_SYNC_ENABLE, String.valueOf(rollbackSync));
      return this;
    }

    public HoodieRonDBIndexConfig build() {
      rondDBIndexConfig.setDefaults(HoodieRonDBIndexConfig.class.getName());
      return rondDBIndexConfig;
    }

  }
}