/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.openworkflow.persistence.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Objects;

/** Creates the bounded application pool shared by HTTP repositories and projections. */
public final class PostgresqlDataSources {
  private PostgresqlDataSources() {}

  public static HikariDataSource pooled(
      String jdbcUrl, String username, String password, int maximumPoolSize) {
    return new HikariDataSource(configuration(jdbcUrl, username, password, maximumPoolSize));
  }

  static HikariConfig configuration(
      String jdbcUrl, String username, String password, int maximumPoolSize) {
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      throw new IllegalArgumentException("jdbcUrl must not be blank");
    }
    if (maximumPoolSize < 1) {
      throw new IllegalArgumentException("maximumPoolSize must be positive");
    }
    var config = new HikariConfig();
    config.setPoolName("openworkflow-application");
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(Objects.requireNonNullElse(username, ""));
    config.setPassword(Objects.requireNonNullElse(password, ""));
    config.setMaximumPoolSize(maximumPoolSize);
    config.setMinimumIdle(1);
    config.setConnectionTimeout(5_000);
    config.setValidationTimeout(2_000);
    config.setInitializationFailTimeout(-1);
    return config;
  }
}
