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
package com.forwardmeasure.openworkflow.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

class OpenWorkflowCassandraMigrationTest {
  @Test
  void installsOnlyCurrentPekkoAndApplicationSchemasRestartSafely() {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      try (CqlSession session = session(cassandra)) {
        OpenWorkflowCassandraMigrator.migrate(session);
        OpenWorkflowCassandraMigrator.migrate(session);

        assertEquals(
            List.of(
                "openworkflow_application",
                "openworkflow_journal",
                "openworkflow_projection",
                "openworkflow_snapshot"),
            session
                .execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name IN"
                        + " ('openworkflow_application','openworkflow_journal',"
                        + " 'openworkflow_projection','openworkflow_snapshot')")
                .all()
                .stream()
                .map(row -> row.getString("keyspace_name"))
                .sorted()
                .toList());
        assertEquals(
            List.of("openworkflow_event_subscription"),
            session
                .execute(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name ="
                        + " 'openworkflow_application'")
                .all()
                .stream()
                .map(row -> row.getString("table_name"))
                .sorted()
                .toList());
      }
    }
  }

  @Test
  void rejectsUntrustedApplicationKeyspaceNamesBeforeExecutingCql() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OpenWorkflowCassandraMigrator.validateKeyspace("openworkflow; DROP KEYSPACE x"));
  }

  private static CqlSession session(CassandraContainer cassandra) {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .build();
  }
}
