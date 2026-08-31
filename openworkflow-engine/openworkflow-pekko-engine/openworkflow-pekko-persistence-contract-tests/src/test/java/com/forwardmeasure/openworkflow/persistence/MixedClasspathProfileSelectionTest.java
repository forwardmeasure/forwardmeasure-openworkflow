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
package com.forwardmeasure.openworkflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class MixedClasspathProfileSelectionTest {
  @Test
  void selectsPostgresqlDeterministicallyWhenBothPluginsArePresent() {
    var selected =
        PersistenceConfigLoader.select(ConfigFactory.load(), PersistenceProfile.POSTGRESQL);
    assertEquals("jdbc-journal", selected.getString("pekko.persistence.journal.plugin"));
    assertEquals(
        "jdbc-snapshot-store", selected.getString("pekko.persistence.snapshot-store.plugin"));
  }

  @Test
  void selectsCassandraDeterministicallyWhenBothPluginsArePresent() {
    var selected =
        PersistenceConfigLoader.select(ConfigFactory.load(), PersistenceProfile.CASSANDRA);
    assertEquals(
        "pekko.persistence.cassandra.journal",
        selected.getString("pekko.persistence.journal.plugin"));
    assertEquals(
        "pekko.persistence.cassandra.snapshot",
        selected.getString("pekko.persistence.snapshot-store.plugin"));
  }
}
