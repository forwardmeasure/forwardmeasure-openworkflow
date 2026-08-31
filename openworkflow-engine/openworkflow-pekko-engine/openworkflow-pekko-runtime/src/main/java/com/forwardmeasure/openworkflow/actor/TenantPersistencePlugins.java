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

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.Objects;
import java.util.Optional;
import org.apache.pekko.actor.typed.ActorSystem;

/**
 * Builds per-tenant {@code pekko-persistence-jdbc} journal/snapshot-store/read-journal plugin ids
 * and configs from one shared, schema-less {@link PostgresConnectionSettings}.
 *
 * <p>Each tenant's actor journal and snapshot store live in that tenant's own Postgres schema
 * (created by {@code OpenWorkflowTenantMigrator}), but {@code pekko-persistence-jdbc}'s
 * shared-database mechanism only supports one static, pre-registered connection per plugin id.
 * Apache Pekko's {@code Persistence.pluginHolderFor} caches plugin instances keyed solely by the
 * plugin-id string, and resolves a never-before-seen plugin id entirely from the config supplied at
 * that call site - no pre-registration in {@code reference.conf}/{@code application.conf} and no
 * actor-system restart needed. Giving each tenant schema its own plugin-id string therefore gives
 * each tenant its own independently-cached plugin instance and connection pool, resolved lazily the
 * first time that tenant is actually used.
 *
 * <p>Every builder here starts from the actor system's own already-resolved {@code jdbc-journal}/
 * {@code jdbc-snapshot-store}/{@code jdbc-read-journal} config (not a hand-assembled config), so
 * {@code class} and every other library default come along unmodified, and any HOCON
 * self-referencing substitution (e.g. {@code jdbc-read-journal.tables.event_journal =
 * ${jdbc-journal.tables.event_journal}}) is already resolved to a concrete value before it gets
 * copied under the tenant's own plugin-id path.
 */
public final class TenantPersistencePlugins {
  private static final int TENANT_POOL_MAX_CONNECTIONS = 3;
  private static final String POSTGRES_PROFILE = "slick.jdbc.PostgresProfile$";
  private static final String POSTGRES_DRIVER = "org.postgresql.Driver";

  private TenantPersistencePlugins() {}

  public static String journalPluginId(TenantSchema schema) {
    return "jdbc-journal-" + schema.value();
  }

  public static String snapshotPluginId(TenantSchema schema) {
    return "jdbc-snapshot-store-" + schema.value();
  }

  public static String readJournalPluginId(TenantSchema schema) {
    return "jdbc-read-journal-" + schema.value();
  }

  /** Event tags live in their own companion table to event_journal - both need the schema set. */
  private static final String[] JOURNAL_SCHEMA_PATHS = {
    "tables.event_journal.schemaName", "tables.event_tag.schemaName"
  };

  private static final String[] SNAPSHOT_SCHEMA_PATHS = {"tables.snapshot.schemaName"};

  public static Optional<Config> journalPluginConfig(
      ActorSystem<?> system, TenantSchema schema, PostgresConnectionSettings connection) {
    return Optional.of(
        tenantOverlay(
            system,
            "jdbc-journal",
            journalPluginId(schema),
            schema,
            connection,
            JOURNAL_SCHEMA_PATHS));
  }

  public static Optional<Config> snapshotPluginConfig(
      ActorSystem<?> system, TenantSchema schema, PostgresConnectionSettings connection) {
    return Optional.of(
        tenantOverlay(
            system,
            "jdbc-snapshot-store",
            snapshotPluginId(schema),
            schema,
            connection,
            SNAPSHOT_SCHEMA_PATHS));
  }

  /** Also points {@code write-plugin} at this tenant's own journal id, not the global default. */
  public static Config readJournalPluginConfig(
      ActorSystem<?> system, TenantSchema schema, PostgresConnectionSettings connection) {
    String pluginId = readJournalPluginId(schema);
    Config overlay =
        tenantOverlay(
            system, "jdbc-read-journal", pluginId, schema, connection, JOURNAL_SCHEMA_PATHS);
    return overlay.withValue(
        pluginId + ".write-plugin", ConfigValueFactory.fromAnyRef(journalPluginId(schema)));
  }

  private static Config tenantOverlay(
      ActorSystem<?> system,
      String basePluginPath,
      String pluginId,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      String[] schemaNameConfigPaths) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(connection, "connection");
    Config base = system.settings().config().getConfig(basePluginPath);
    for (String schemaNameConfigPath : schemaNameConfigPaths) {
      base = base.withValue(schemaNameConfigPath, ConfigValueFactory.fromAnyRef(schema.value()));
    }
    Config tenant =
        base.withoutPath("use-shared-db")
            .withValue("slick.profile", ConfigValueFactory.fromAnyRef(POSTGRES_PROFILE))
            .withValue("slick.db.connectionPool", ConfigValueFactory.fromAnyRef("HikariCP"))
            .withValue("slick.db.url", ConfigValueFactory.fromAnyRef(connection.baseUrl()))
            .withValue("slick.db.user", ConfigValueFactory.fromAnyRef(connection.username()))
            .withValue("slick.db.password", ConfigValueFactory.fromAnyRef(connection.password()))
            .withValue("slick.db.driver", ConfigValueFactory.fromAnyRef(POSTGRES_DRIVER))
            .withValue(
                "slick.db.numThreads", ConfigValueFactory.fromAnyRef(TENANT_POOL_MAX_CONNECTIONS))
            .withValue(
                "slick.db.maxConnections",
                ConfigValueFactory.fromAnyRef(TENANT_POOL_MAX_CONNECTIONS))
            .withValue("slick.db.minConnections", ConfigValueFactory.fromAnyRef(0));
    return ConfigFactory.empty().withValue(pluginId, tenant.root());
  }
}
