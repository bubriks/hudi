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
      .key("hoodie.index.rondb.update.partition.path")
      .defaultValue(false)
      .withDocumentation("Only applies if index type is RONDB. "
          + "When an already existing record is upserted to a new partition compared to whats in storage, "
          + "this config when set, will delete old record in old partition "
          + "and will insert it as new record in new partition.");

  public static final ConfigProperty<Boolean> ROLLBACK_SYNC_ENABLE = ConfigProperty
      .key("hoodie.index.rondb.rollback.sync")
      .defaultValue(false)
      .withDocumentation("When set to true, the rollback method will delete the last failed task index. "
          + "The default value is false. Because deleting the index will add extra load on the RonDB cluster for each rollback");

  public static final ConfigProperty<Integer> BATCH_SIZE = ConfigProperty
      .key("hoodie.index.rondb.batch.size")
      .defaultValue(100)
      .withDocumentation("Controls the batch size for performing operations against RonDB. "
          + "Batching improves throughput, by saving round trips.");

  public static final ConfigProperty<String> JDBC_DRIVER = ConfigProperty
          .key("hoodie.index.rondb.jdbc.driver")
          .defaultValue("com.mysql.jdbc.Driver")
          .withDocumentation("Sets driver to use when requiring JDBC connection");

  public static final ConfigProperty<String> JDBC_URL = ConfigProperty
      .key("hoodie.index.rondb.jdbc.url")
      .defaultValue("jdbc:mysql://localhost:3306/hudi")
      .withDocumentation("JDBC url to use when connecting");

  public static final ConfigProperty<Properties> JPA = ConfigProperty
      .key("hoodie.index.rondb.jpa")
      .defaultValue(getJPAProperties())
      .withDocumentation("JPA properties");

  private static Properties getJPAProperties() {
    Properties properties = new Properties();
    properties.put("javax.persistence.jdbc.url", "jdbc:mysql://localhost:3306/hudi?createDatabaseIfNotExist=true");
    properties.put("javax.persistence.jdbc.user", "root");
    properties.put("javax.persistence.jdbc.password", "");
    properties.put("javax.persistence.schema-generation.database.action", "create");
    return properties;
  }

  public static final ConfigProperty<Properties> JDBC = ConfigProperty
      .key("hoodie.index.rondb.jdbc")
      .defaultValue(getJDBCProperties())
      .withDocumentation("JDBC properties");

  private static Properties getJDBCProperties() {
    Properties properties = new Properties();
    properties.put("createDatabaseIfNotExist", "true");
    properties.put("user", "root");
    properties.put("password", "");
    return properties;
  }

  public static final ConfigProperty<Properties> CLUSTERJ = ConfigProperty
      .key("hoodie.index.rondb.clusterj")
      .defaultValue(getCLUSTERJProperties())
      .withDocumentation("CLUSTERJ properties");

  private static Properties getCLUSTERJProperties() {
    Properties properties = new Properties();
    properties.put("com.mysql.clusterj.connectstring", "10.0.2.15:1186");
    properties.put("com.mysql.clusterj.database", "hudi");
    return properties;
  }

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

    public HoodieRonDBIndexConfig.Builder rondbIndexBatchSize(int batchSize) {
      rondDBIndexConfig.setValue(BATCH_SIZE, String.valueOf(batchSize));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexJDBCDriver(String driver) {
      rondDBIndexConfig.setValue(JDBC_DRIVER, String.valueOf(driver));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexJDBCURL(String url) {
      rondDBIndexConfig.setValue(JDBC_URL, String.valueOf(url));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexJPA(Properties properties) {
      rondDBIndexConfig.setValue(JPA, String.valueOf(properties));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexJDBC(Properties properties) {
      rondDBIndexConfig.setValue(JDBC, String.valueOf(properties));
      return this;
    }

    public HoodieRonDBIndexConfig.Builder rondbIndexCLUSTERJ(Properties properties) {
      rondDBIndexConfig.setValue(CLUSTERJ, String.valueOf(properties));
      return this;
    }

    public HoodieRonDBIndexConfig build() {
      rondDBIndexConfig.setDefaults(HoodieRonDBIndexConfig.class.getName());
      return rondDBIndexConfig;
    }

  }
}