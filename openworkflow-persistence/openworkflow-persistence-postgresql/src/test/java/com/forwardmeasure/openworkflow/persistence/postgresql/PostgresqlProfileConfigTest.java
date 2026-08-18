package com.forwardmeasure.openworkflow.persistence.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.openworkflow.persistence.PersistenceConfigLoader;
import com.forwardmeasure.openworkflow.persistence.PersistenceProfile;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class PostgresqlProfileConfigTest {
  @Test
  void selectsJdbcJournalSnapshotAndSharedPostgresDatabase() {
    var config =
        PersistenceConfigLoader.select(ConfigFactory.load(), PersistenceProfile.POSTGRESQL);

    assertEquals("jdbc-journal", config.getString("pekko.persistence.journal.plugin"));
    assertEquals(
        "jdbc-snapshot-store", config.getString("pekko.persistence.snapshot-store.plugin"));
    assertEquals(
        "slick.jdbc.PostgresProfile$",
        config.getString("pekko-persistence-jdbc.shared-databases.openworkflow.profile"));
    assertEquals("openworkflow", config.getString("jdbc-read-journal.use-shared-db"));
    assertEquals("postgres-dialect", config.getString("pekko.projection.jdbc.dialect"));
    assertEquals(
        8, config.getInt("pekko-persistence-jdbc.shared-databases.openworkflow.db.maxConnections"));
    assertEquals(
        8, config.getInt("pekko-persistence-jdbc.shared-databases.openworkflow.db.numThreads"));
    assertEquals(
        20,
        config.getInt(
            "pekko.projection.jdbc.blocking-jdbc-dispatcher"
                + ".thread-pool-executor.fixed-pool-size"));
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
