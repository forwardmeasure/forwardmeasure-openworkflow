package com.forwardmeasure.openworkflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

class PersistenceConfigLoaderTest {
  @Test
  void appliesCanonicalPostgresqlConnectionSettings() {
    var configured =
        PersistenceConfigLoader.withConnection(
            ConfigFactory.parseString(
                """
                pekko-persistence-jdbc.shared-databases.openworkflow.db.url = old
                """),
            PersistenceProfile.POSTGRESQL,
            "jdbc:postgresql://db/workflow",
            "workflow",
            "secret",
            "unused");

    assertEquals(
        "jdbc:postgresql://db/workflow",
        configured.getString("pekko-persistence-jdbc.shared-databases.openworkflow.db.url"));
    assertEquals(
        "workflow",
        configured.getString("pekko-persistence-jdbc.shared-databases.openworkflow.db.user"));
    assertEquals(
        "secret",
        configured.getString("pekko-persistence-jdbc.shared-databases.openworkflow.db.password"));
  }

  @Test
  void appliesCanonicalCassandraConnectionAndAuthenticationSettings() {
    var configured =
        PersistenceConfigLoader.withConnection(
            ConfigFactory.empty(),
            PersistenceProfile.CASSANDRA,
            "cassandra:9042",
            "workflow",
            "secret",
            "dc1");

    assertEquals(
        List.of("cassandra:9042"),
        configured.getStringList("datastax-java-driver.basic.contact-points"));
    assertEquals(
        "dc1",
        configured.getString("datastax-java-driver.basic.load-balancing-policy.local-datacenter"));
    assertEquals(
        "PlainTextAuthProvider",
        configured.getString("datastax-java-driver.advanced.auth-provider.class"));
    assertEquals(
        "workflow", configured.getString("datastax-java-driver.advanced.auth-provider.username"));
  }

  @Test
  void rejectsIncompleteCassandraCredentials() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PersistenceConfigLoader.withConnection(
                ConfigFactory.empty(),
                PersistenceProfile.CASSANDRA,
                "cassandra:9042",
                "workflow",
                "",
                "dc1"));
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
