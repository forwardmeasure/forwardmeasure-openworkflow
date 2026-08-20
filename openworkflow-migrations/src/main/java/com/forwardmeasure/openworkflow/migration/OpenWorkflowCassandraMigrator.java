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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Installs the shared Pekko Cassandra stores and the OpenWorkflow application projections. */
public final class OpenWorkflowCassandraMigrator {
  public static final String DEFAULT_APPLICATION_KEYSPACE = "openworkflow_application";

  private static final String CHANGELOG = "/db/changelog/openworkflow-cassandra.cql";
  private static final String APPLICATION_KEYSPACE_TOKEN = "${applicationKeyspace}";

  private OpenWorkflowCassandraMigrator() {}

  public static void migrate(CqlSession session) {
    migrate(session, DEFAULT_APPLICATION_KEYSPACE);
  }

  public static void migrate(CqlSession session, String applicationKeyspace) {
    Objects.requireNonNull(session, "session");
    String validatedKeyspace = validateKeyspace(applicationKeyspace);
    for (String statement :
        changelog().replace(APPLICATION_KEYSPACE_TOKEN, validatedKeyspace).split(";")) {
      String cql = statement.strip();
      if (!cql.isEmpty()) {
        session.execute(cql);
      }
    }
  }

  static String validateKeyspace(String keyspace) {
    String value = Objects.requireNonNull(keyspace, "applicationKeyspace");
    if (!value.matches("[a-z][a-z0-9_]{0,47}")) {
      throw new IllegalArgumentException("Invalid Cassandra application keyspace");
    }
    return value;
  }

  private static String changelog() {
    try (var input = OpenWorkflowCassandraMigrator.class.getResourceAsStream(CHANGELOG)) {
      if (input == null) {
        throw new IllegalStateException("Missing Cassandra changelog " + CHANGELOG);
      }
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }
}
