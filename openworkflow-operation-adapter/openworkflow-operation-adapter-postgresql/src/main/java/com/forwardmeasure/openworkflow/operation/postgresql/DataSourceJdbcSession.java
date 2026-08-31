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
package com.forwardmeasure.openworkflow.operation.postgresql;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.pekko.japi.function.Function;
import org.apache.pekko.projection.jdbc.JdbcSession;

/**
 * The offset-store/management tables live inside each tenant's own Postgres schema, same as the
 * actor journal - {@code schema} routes this session's connection there, mirroring {@code
 * SpringSchemaConnectionProvider}'s {@code Connection#setSchema(...)} pattern from {@code
 * forwardmeasure-jpa-spring}.
 */
final class DataSourceJdbcSession implements JdbcSession {
  private final Connection connection;

  DataSourceJdbcSession(DataSource dataSource, TenantSchema schema) {
    try {
      connection = Objects.requireNonNull(dataSource).getConnection();
      connection.setSchema(Objects.requireNonNull(schema, "schema").value());
      connection.setAutoCommit(false);
    } catch (SQLException failure) {
      throw new IllegalStateException(failure);
    }
  }

  @Override
  public <Result> Result withConnection(Function<Connection, Result> function) throws Exception {
    return function.apply(connection);
  }

  @Override
  public void commit() throws SQLException {
    connection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    connection.rollback();
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }
}
