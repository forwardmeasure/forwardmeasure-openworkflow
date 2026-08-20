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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/** Applies the versioned Cassandra schema through the Liquibase Cassandra extension. */
public final class OpenWorkflowCassandraMigrator {
  public static final String DEFAULT_APPLICATION_KEYSPACE = "openworkflow_application";
  public static final String DEFAULT_MIGRATION_KEYSPACE = "openworkflow_migrations";

  private static final String CHANGELOG =
      "db/changelog/cassandra/openworkflow-cassandra-master.xml";
  private static final int BOOTSTRAP_ATTEMPTS = 20;
  private static final Duration BOOTSTRAP_RETRY_DELAY = Duration.ofMillis(500);

  private OpenWorkflowCassandraMigrator() {}

  public static void migrate(CassandraMigrationTarget target) {
    migrate(target, CHANGELOG);
  }

  static void migrate(CassandraMigrationTarget target, String changelog) {
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(changelog, "changelog");
    bootstrapLiquibaseMetadata(target);
    Properties credentials = new Properties();
    target.username().ifPresent(value -> credentials.setProperty("user", value));
    target.password().ifPresent(value -> credentials.setProperty("password", value));
    try {
      Class.forName("com.ing.data.cassandra.jdbc.CassandraDriver");
      try (Connection connection = DriverManager.getConnection(target.jdbcUrl(), credentials)) {
        Database database =
            DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection));
        try (Liquibase liquibase =
            new Liquibase(
                changelog,
                new ClassLoaderResourceAccessor(
                    OpenWorkflowCassandraMigrator.class.getClassLoader()),
                database)) {
          liquibase.setChangeLogParameter("applicationKeyspace", target.applicationKeyspace());
          liquibase.setChangeLogParameter("replicationDatacenter", target.localDatacenter());
          liquibase.setChangeLogParameter(
              "replicationFactor", Integer.toString(target.replicationFactor()));
          liquibase.update(new Contexts(), new LabelExpression());
        }
      }
    } catch (ReflectiveOperationException | java.sql.SQLException | LiquibaseException failure) {
      throw new IllegalStateException("Cassandra migration failed", failure);
    }
  }

  /**
   * Creates Liquibase's own metadata boundary before Liquibase starts.
   *
   * <p>The Cassandra extension creates these tables lazily, before its distributed lock exists. Two
   * first-time migration Jobs can consequently race on schema creation. Idempotent CQL plus
   * schema-agreement retries close that cold-start race; every application schema change remains a
   * versioned Liquibase changeset.
   */
  private static void bootstrapLiquibaseMetadata(CassandraMigrationTarget target) {
    CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter(target.localDatacenter());
    target.contactPoints().forEach(builder::addContactPoint);
    target
        .username()
        .ifPresent(
            username -> builder.withAuthCredentials(username, target.password().orElseThrow()));
    try (CqlSession session = builder.build()) {
      executeBootstrapStatement(
          session,
          "CREATE KEYSPACE IF NOT EXISTS "
              + target.migrationKeyspace()
              + " WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '"
              + target.localDatacenter()
              + "': "
              + target.replicationFactor()
              + "}");
      String metadataKeyspace = target.migrationKeyspace();
      executeBootstrapStatement(
          session,
          "CREATE TABLE IF NOT EXISTS "
              + metadataKeyspace
              + ".databasechangelog (ID TEXT, AUTHOR TEXT, FILENAME TEXT, DATEEXECUTED TIMESTAMP,"
              + " ORDEREXECUTED INT, EXECTYPE TEXT, MD5SUM TEXT, DESCRIPTION TEXT, COMMENTS TEXT,"
              + " TAG TEXT, LIQUIBASE TEXT, CONTEXTS TEXT, LABELS TEXT, DEPLOYMENT_ID TEXT, PRIMARY"
              + " KEY (ID, AUTHOR, FILENAME))");
      executeBootstrapStatement(
          session,
          "CREATE TABLE IF NOT EXISTS "
              + metadataKeyspace
              + ".databasechangeloglock (ID INT, LOCKED BOOLEAN, LOCKGRANTED TIMESTAMP, LOCKEDBY"
              + " TEXT, PRIMARY KEY (ID))");
      executeBootstrapStatement(
          session,
          "INSERT INTO "
              + metadataKeyspace
              + ".databasechangeloglock (ID, LOCKED) VALUES (1, FALSE) IF NOT EXISTS");
    }
  }

  private static void executeBootstrapStatement(CqlSession session, String cql) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= BOOTSTRAP_ATTEMPTS; attempt++) {
      try {
        session.execute(cql);
        if (session.checkSchemaAgreement()) {
          return;
        }
      } catch (RuntimeException failure) {
        lastFailure = failure;
      }
      LockSupport.parkNanos(BOOTSTRAP_RETRY_DELAY.toNanos());
      if (Thread.currentThread().isInterrupted()) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Cassandra migration bootstrap was interrupted", lastFailure);
      }
    }
    throw new IllegalStateException(
        "Cassandra migration metadata did not reach schema agreement", lastFailure);
  }
}
