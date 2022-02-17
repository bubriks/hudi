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

import org.apache.hudi.common.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HoodieRonDBIndexConfig extends DefaultHoodieConfig {

  // storage

  public static final String RONDB_TABLE_PROP = "hoodie.index.rondb.table";
  public static final String DEFAULT_RONDB_TABLE_PROP = "hudi_index";

  public static final String RONDB_DATABASE_PROP = "hoodie.index.rondb.database";
  public static final String DEFAULT_RONDB_DATABASE_PROP = "hudi";

  // connection

  public static final String RONDB_URL_PROP = "hoodie.index.rondb.url";
  public static final String DEFAULT_RONDB_URL_PROP = "jdbc:mysql://localhost:3306";

  // authentication

  public static final String RONDB_AUTH_USERNAME_PROP = "hoodie.index.rondb.auth.username";
  public static final String DEFAULT_RONDB_AUTH_USERNAME_PROP = "root";

  public static final String RONDB_AUTH_PASSWORD_PROP = "hoodie.index.rondb.auth.password";
  public static final String DEFAULT_RONDB_AUTH_PASSWORD_PROP = "";

  // CRUD config

  public static final String RONDB_GET_BATCH_SIZE_PROP = "hoodie.index.rondb.get.batch.size";
  public static final String RONDB_PUT_BATCH_SIZE_PROP = "hoodie.index.rondb.put.batch.size";
  public static final int DEFAULT_HBASE_BATCH_SIZE = 100;

  /**
   * Only applies if index type is RonDB.
   * <p>
   * When set to true, an update to a record with a different partition from its existing one
   * will insert the record to the new partition and delete it from the old partition.
   * <p>
   * When set to false, a record will be updated to the old partition.
   */
  public static final String RONDB_INDEX_UPDATE_PARTITION_PATH = "hoodie.rondb.index.update.partition.path";
  public static final Boolean DEFAULT_RONDB_INDEX_UPDATE_PARTITION_PATH = false;

  /**
   * When set to true, the rollback method will delete the last failed task index.
   * The default value is false. Because deleting the index will add extra load on the RonDB cluster for each rollback.
   */
  public static final String RONDB_INDEX_ROLLBACK_SYNC = "hoodie.index.rondb.rollback.sync";
  public static final Boolean DEFAULT_RONDB_INDEX_ROLLBACK_SYNC = false;

  public HoodieRonDBIndexConfig(final Properties props) {
    super(props);
  }

  public static HoodieRonDBIndexConfig.Builder newBuilder() {
    return new HoodieRonDBIndexConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieRonDBIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieRonDBIndexConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbTableName(String tableName) {
      props.setProperty(RONDB_TABLE_PROP, tableName);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbDatabaseName(String databaseName) {
      props.setProperty(RONDB_DATABASE_PROP, databaseName);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbUrlName(String url) {
      props.setProperty(RONDB_URL_PROP, url);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbUsername(String username) {
      props.setProperty(RONDB_AUTH_USERNAME_PROP, username);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbPassword(String password) {
      props.setProperty(RONDB_AUTH_PASSWORD_PROP, password);
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexGetBatchSize(int getBatchSize) {
      props.setProperty(RONDB_GET_BATCH_SIZE_PROP, String.valueOf(getBatchSize));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexPutBatchSize(int getBatchSize) {
      props.setProperty(RONDB_PUT_BATCH_SIZE_PROP, String.valueOf(getBatchSize));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexUpdatePartitionPath(boolean updatePartitionPath) {
      props.setProperty(RONDB_INDEX_UPDATE_PARTITION_PATH, String.valueOf(updatePartitionPath));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexRollbackSync(boolean rollbackSync) {
      props.setProperty(RONDB_INDEX_ROLLBACK_SYNC, String.valueOf(rollbackSync));
      return this;
    }

    public HoodieRonDBIndexConfig build() {
      HoodieRonDBIndexConfig config = new HoodieRonDBIndexConfig(props);
      setDefaultOnCondition(props, !props.containsKey(RONDB_TABLE_PROP), RONDB_TABLE_PROP,
              String.valueOf(DEFAULT_RONDB_TABLE_PROP));
      setDefaultOnCondition(props, !props.containsKey(RONDB_DATABASE_PROP), RONDB_DATABASE_PROP,
              String.valueOf(DEFAULT_RONDB_DATABASE_PROP));
      setDefaultOnCondition(props, !props.containsKey(RONDB_URL_PROP), RONDB_URL_PROP,
              String.valueOf(DEFAULT_RONDB_URL_PROP));
      setDefaultOnCondition(props, !props.containsKey(RONDB_AUTH_USERNAME_PROP), RONDB_AUTH_USERNAME_PROP,
              String.valueOf(DEFAULT_RONDB_AUTH_USERNAME_PROP));
      setDefaultOnCondition(props, !props.containsKey(RONDB_AUTH_PASSWORD_PROP), RONDB_AUTH_PASSWORD_PROP,
              String.valueOf(DEFAULT_RONDB_AUTH_PASSWORD_PROP));
      setDefaultOnCondition(props, !props.containsKey(RONDB_GET_BATCH_SIZE_PROP), RONDB_GET_BATCH_SIZE_PROP,
              String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(RONDB_PUT_BATCH_SIZE_PROP), RONDB_PUT_BATCH_SIZE_PROP,
              String.valueOf(DEFAULT_HBASE_BATCH_SIZE));
      setDefaultOnCondition(props, !props.containsKey(RONDB_INDEX_UPDATE_PARTITION_PATH), RONDB_INDEX_UPDATE_PARTITION_PATH,
              String.valueOf(DEFAULT_RONDB_INDEX_UPDATE_PARTITION_PATH));
      setDefaultOnCondition(props, !props.containsKey(RONDB_INDEX_ROLLBACK_SYNC), RONDB_INDEX_ROLLBACK_SYNC,
              String.valueOf(DEFAULT_RONDB_INDEX_ROLLBACK_SYNC));
      return config;
    }

  }
}
