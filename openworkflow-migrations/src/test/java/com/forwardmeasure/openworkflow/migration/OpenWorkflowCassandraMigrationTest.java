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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

class OpenWorkflowCassandraMigrationTest {
  @Test
  void installsOnlyCurrentPekkoAndApplicationSchemasRestartSafely() {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      CassandraMigrationTarget target = target(cassandra);
      try (CqlSession session = session(cassandra)) {
        OpenWorkflowCassandraMigrator.migrate(target);
        OpenWorkflowCassandraMigrator.migrate(target);

        assertEquals(
            List.of(
                "openworkflow_application",
                "openworkflow_journal",
                "openworkflow_migrations",
                "openworkflow_projection",
                "openworkflow_snapshot"),
            session
                .execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name IN"
                        + " ('openworkflow_application','openworkflow_journal','openworkflow_migrations',"
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
        assertEquals(
            14L,
            session
                .execute("SELECT COUNT(*) FROM openworkflow_migrations.databasechangelog")
                .one()
                .getLong(0));
        assertEquals(
            1L,
            session
                .execute("SELECT COUNT(*) FROM openworkflow_migrations.databasechangeloglock")
                .one()
                .getLong(0));
        assertTrue(
            session
                .execute(
                    "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name ="
                        + " 'openworkflow_journal'")
                .one()
                .getMap("replication", String.class, String.class)
                .containsKey("datacenter1"));
      }
    }
  }

  @Test
  void serializesConcurrentMigrationJobsWithTheLiquibaseLock() throws Exception {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      Process first = migrationProcess(cassandra, "first");
      Process second = migrationProcess(cassandra, "second");
      assertTrue(first.waitFor(2, TimeUnit.MINUTES), "first migration process timed out");
      assertTrue(second.waitFor(2, TimeUnit.MINUTES), "second migration process timed out");
      assertEquals(0, first.exitValue(), processOutput("first"));
      assertEquals(0, second.exitValue(), processOutput("second"));

      try (CqlSession session = session(cassandra)) {
        assertEquals(
            14L,
            session
                .execute("SELECT COUNT(*) FROM openworkflow_parallel_migrations.databasechangelog")
                .one()
                .getLong(0));
      }
    }
  }

  private static Process migrationProcess(CassandraContainer cassandra, String identity)
      throws Exception {
    Path output = processOutputPath(identity);
    return new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("surefire.test.class.path"),
            CassandraMigrationProcessMain.class.getName(),
            cassandra.getHost(),
            Integer.toString(cassandra.getMappedPort(9042)),
            cassandra.getLocalDatacenter(),
            "openworkflow_parallel_migrations",
            "openworkflow_parallel_app")
        .redirectErrorStream(true)
        .redirectOutput(output.toFile())
        .start();
  }

  private static String processOutput(String identity) throws Exception {
    return Files.readString(processOutputPath(identity));
  }

  private static Path processOutputPath(String identity) {
    return Path.of("target", "cassandra-migration-" + identity + ".out");
  }

  @Test
  void upgradesAnEarlierTrackedSchemaAndRejectsChecksumDrift() {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      CassandraMigrationTarget target =
          target(cassandra, "openworkflow_upgrade_migrations", "openworkflow_upgrade_app");

      OpenWorkflowCassandraMigrator.migrate(
          target, "db/changelog/cassandra-test/openworkflow-cassandra-baseline.xml");
      OpenWorkflowCassandraMigrator.migrate(target);

      try (CqlSession session = session(cassandra)) {
        assertEquals(
            14L,
            session
                .execute("SELECT COUNT(*) FROM openworkflow_upgrade_migrations.databasechangelog")
                .one()
                .getLong(0));
      }
      assertThrows(
          IllegalStateException.class,
          () ->
              OpenWorkflowCassandraMigrator.migrate(
                  target, "db/changelog/cassandra-test/openworkflow-cassandra-drift.xml"));
    }
  }

  @Test
  void rejectsUntrustedApplicationKeyspaceNamesBeforeExecutingCql() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CassandraMigrationTarget(
                List.of(InetSocketAddress.createUnresolved("localhost", 9042)),
                "datacenter1",
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                "openworkflow_migrations",
                "openworkflow; DROP KEYSPACE x",
                1));
  }

  private static CassandraMigrationTarget target(CassandraContainer cassandra) {
    return CassandraMigrationTarget.unauthenticated(
        new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)),
        cassandra.getLocalDatacenter());
  }

  private static CassandraMigrationTarget target(
      CassandraContainer cassandra, String migrationKeyspace, String applicationKeyspace) {
    return new CassandraMigrationTarget(
        List.of(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042))),
        cassandra.getLocalDatacenter(),
        Optional.empty(),
        Optional.empty(),
        migrationKeyspace,
        applicationKeyspace,
        1);
  }

  private static CqlSession session(CassandraContainer cassandra) {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
        .withLocalDatacenter(cassandra.getLocalDatacenter())
        .build();
  }
}
