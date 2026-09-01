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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link TenantPersistencePlugins} gives every tenant schema its own {@code pekko-persistence-jdbc}
 * plugin id and config, overlaid onto the actor system's own already-resolved {@code jdbc-journal}/
 * {@code jdbc-snapshot-store}/{@code jdbc-read-journal} config. This module (openworkflow-pekko-
 * runtime) does not itself depend on {@code pekko-persistence-jdbc} - that library only arrives on
 * the classpath of the actual deployable, combined with openworkflow-pekko-persistence-postgresql.
 * These tests therefore install a minimal, realistic stand-in for that base config directly on a
 * test {@link ActorTestKit}'s actor system, matching the shape the real library's reference.conf
 * would provide in production, and then assert on {@link TenantPersistencePlugins}' own overlay
 * logic against it.
 */
class TenantPersistencePluginsTest {
  private static final String BASE_CONFIG =
      """
      jdbc-journal {
        class = "org.apache.pekko.persistence.jdbc.journal.JdbcAsyncWriteJournal"
        use-shared-db = "slick"
        tables {
          event_journal { schemaName = "" }
          event_tag { schemaName = "" }
        }
        slick {
          profile = "slick.jdbc.H2Profile$"
          db {
            url = "jdbc:h2:mem:shared"
            user = "shared-user"
            password = "shared-password"
            driver = "org.h2.Driver"
            numThreads = 5
            maxConnections = 5
            minConnections = 1
          }
        }
      }
      jdbc-snapshot-store {
        class = "org.apache.pekko.persistence.jdbc.snapshot.JdbcSnapshotStore"
        use-shared-db = "slick"
        tables {
          snapshot { schemaName = "" }
        }
        slick {
          profile = "slick.jdbc.H2Profile$"
          db {
            url = "jdbc:h2:mem:shared"
            user = "shared-user"
            password = "shared-password"
            driver = "org.h2.Driver"
            numThreads = 5
            maxConnections = 5
            minConnections = 1
          }
        }
      }
      jdbc-read-journal {
        class = "org.apache.pekko.persistence.jdbc.query.JdbcReadJournalProvider"
        use-shared-db = "slick"
        tables {
          event_journal { schemaName = "" }
          event_tag { schemaName = "" }
        }
        slick {
          profile = "slick.jdbc.H2Profile$"
          db {
            url = "jdbc:h2:mem:shared"
            user = "shared-user"
            password = "shared-password"
            driver = "org.h2.Driver"
            numThreads = 5
            maxConnections = 5
            minConnections = 1
          }
        }
      }
      """;

  private static final TenantSchema TENANT_A = schema("a".repeat(32));
  private static final TenantSchema TENANT_B = schema("b".repeat(32));
  private static final PostgresConnectionSettings CONNECTION =
      new PostgresConnectionSettings(
          "jdbc:postgresql://tenant-host:5432/openworkflow", "tenant-user", "tenant-password");

  private static ActorTestKit actors;

  @BeforeAll
  static void start() {
    actors = ActorTestKit.create(ConfigFactory.parseString(BASE_CONFIG));
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  private static TenantSchema schema(String hex32) {
    return new TenantSchema("t_" + hex32);
  }

  @Test
  void pluginIdsAreTenantQualifiedAndDistinctPerTenant() {
    assertEquals(
        "jdbc-journal-" + TENANT_A.value(), TenantPersistencePlugins.journalPluginId(TENANT_A));
    assertEquals(
        "jdbc-snapshot-store-" + TENANT_A.value(),
        TenantPersistencePlugins.snapshotPluginId(TENANT_A));
    assertEquals(
        "jdbc-read-journal-" + TENANT_A.value(),
        TenantPersistencePlugins.readJournalPluginId(TENANT_A));
    assertNotEquals(
        TenantPersistencePlugins.journalPluginId(TENANT_A),
        TenantPersistencePlugins.journalPluginId(TENANT_B));
  }

  @Test
  void journalPluginConfigSetsSchemaNameOnBothEventJournalAndEventTagTables() {
    Config overlay =
        TenantPersistencePlugins.journalPluginConfig(actors.system(), TENANT_A, CONNECTION)
            .orElseThrow();
    Config tenant = overlay.getConfig(TenantPersistencePlugins.journalPluginId(TENANT_A));

    // Regression coverage: an earlier version of this class only set
    // tables.event_journal.schemaName. event_tag lives in its own companion table and needs the
    // same tenant schema, or event_tag reads/writes silently fall back to the default schema - the
    // exact bug that caused a live k3s failure.
    assertEquals(TENANT_A.value(), tenant.getString("tables.event_journal.schemaName"));
    assertEquals(TENANT_A.value(), tenant.getString("tables.event_tag.schemaName"));
  }

  @Test
  void journalPluginConfigOverlaysConnectionSettingsAndPoolingOntoTheResolvedBaseConfig() {
    Config overlay =
        TenantPersistencePlugins.journalPluginConfig(actors.system(), TENANT_A, CONNECTION)
            .orElseThrow();
    Config tenant = overlay.getConfig(TenantPersistencePlugins.journalPluginId(TENANT_A));

    assertEquals(CONNECTION.baseUrl(), tenant.getString("slick.db.url"));
    assertEquals(CONNECTION.username(), tenant.getString("slick.db.user"));
    assertEquals(CONNECTION.password(), tenant.getString("slick.db.password"));
    assertEquals("org.postgresql.Driver", tenant.getString("slick.db.driver"));
    assertEquals("slick.jdbc.PostgresProfile$", tenant.getString("slick.profile"));
    assertEquals("HikariCP", tenant.getString("slick.db.connectionPool"));
    assertEquals(3, tenant.getInt("slick.db.numThreads"));
    assertEquals(3, tenant.getInt("slick.db.maxConnections"));
    assertEquals(0, tenant.getInt("slick.db.minConnections"));
    // The base plugin config's own library defaults (e.g. the journal implementation class) come
    // along unmodified from the actor system's already-resolved config.
    assertEquals(
        "org.apache.pekko.persistence.jdbc.journal.JdbcAsyncWriteJournal",
        tenant.getString("class"));
    assertFalse(
        tenant.hasPath("use-shared-db"),
        "use-shared-db must be stripped so each tenant gets its own connection pool");
  }

  @Test
  void snapshotPluginConfigSetsSchemaNameOnTheSnapshotTableAndOverlaysConnectionSettings() {
    Config overlay =
        TenantPersistencePlugins.snapshotPluginConfig(actors.system(), TENANT_A, CONNECTION)
            .orElseThrow();
    Config tenant = overlay.getConfig(TenantPersistencePlugins.snapshotPluginId(TENANT_A));

    assertEquals(TENANT_A.value(), tenant.getString("tables.snapshot.schemaName"));
    assertEquals(CONNECTION.baseUrl(), tenant.getString("slick.db.url"));
    assertEquals(CONNECTION.username(), tenant.getString("slick.db.user"));
    assertEquals(CONNECTION.password(), tenant.getString("slick.db.password"));
    assertEquals(
        "org.apache.pekko.persistence.jdbc.snapshot.JdbcSnapshotStore", tenant.getString("class"));
  }

  @Test
  void readJournalPluginConfigSetsBothSchemaNamesAndPointsWritePluginAtItsOwnTenantJournal() {
    Config overlay =
        TenantPersistencePlugins.readJournalPluginConfig(actors.system(), TENANT_A, CONNECTION);
    String pluginId = TenantPersistencePlugins.readJournalPluginId(TENANT_A);
    Config tenant = overlay.getConfig(pluginId);

    assertEquals(TENANT_A.value(), tenant.getString("tables.event_journal.schemaName"));
    assertEquals(TENANT_A.value(), tenant.getString("tables.event_tag.schemaName"));
    assertEquals(
        TenantPersistencePlugins.journalPluginId(TENANT_A),
        overlay.getString(pluginId + ".write-plugin"));
  }

  @Test
  void differentTenantsProduceIndependentNonCollidingPluginConfigs() {
    Config overlayA =
        TenantPersistencePlugins.journalPluginConfig(actors.system(), TENANT_A, CONNECTION)
            .orElseThrow();
    Config overlayB =
        TenantPersistencePlugins.journalPluginConfig(actors.system(), TENANT_B, CONNECTION)
            .orElseThrow();

    assertTrue(overlayA.hasPath(TenantPersistencePlugins.journalPluginId(TENANT_A)));
    assertFalse(overlayA.hasPath(TenantPersistencePlugins.journalPluginId(TENANT_B)));
    assertEquals(
        TENANT_B.value(),
        overlayB
            .getConfig(TenantPersistencePlugins.journalPluginId(TENANT_B))
            .getString("tables.event_journal.schemaName"));
  }
}
