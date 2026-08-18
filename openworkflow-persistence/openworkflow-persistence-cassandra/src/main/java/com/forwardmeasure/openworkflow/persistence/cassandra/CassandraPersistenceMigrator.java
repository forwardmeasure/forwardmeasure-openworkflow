/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.persistence.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Restart-safe installer for the Pekko Cassandra journal and snapshot keyspaces. */
public final class CassandraPersistenceMigrator {
  private static final String SCHEMA = "/db/migration/cassandra/V001__openworkflow.cql";

  private CassandraPersistenceMigrator() {}

  public static void migrate(CqlSession session) {
    migrate(session, "openworkflow_definition");
  }

  public static void migrate(CqlSession session, String applicationKeyspace) {
    Objects.requireNonNull(session, "session");
    if (!Objects.requireNonNull(applicationKeyspace, "applicationKeyspace")
        .matches("[a-z][a-z0-9_]{0,47}")) {
      throw new IllegalArgumentException("Invalid Cassandra application keyspace");
    }
    for (String statement :
        schema().replace("${applicationKeyspace}", applicationKeyspace).split(";")) {
      String cql = statement.strip();
      if (!cql.isEmpty()) {
        session.execute(cql);
      }
    }
  }

  private static String schema() {
    try (var input = CassandraPersistenceMigrator.class.getResourceAsStream(SCHEMA)) {
      if (input == null) {
        throw new IllegalStateException("Missing Cassandra persistence schema " + SCHEMA);
      }
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }
}
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
