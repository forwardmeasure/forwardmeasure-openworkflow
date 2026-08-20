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
package com.forwardmeasure.openworkflow.persistence.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PostgresqlDataSourcesTest {
  @Test
  void boundsAndNamesTheSharedApplicationPool() {
    var config =
        PostgresqlDataSources.configuration(
            "jdbc:postgresql://database/openworkflow", "user", "secret", 8);

    assertEquals("openworkflow-application", config.getPoolName());
    assertEquals(8, config.getMaximumPoolSize());
    assertEquals(1, config.getMinimumIdle());
    assertEquals(5_000, config.getConnectionTimeout());
    assertEquals(-1, config.getInitializationFailTimeout());
  }

  @Test
  void rejectsAnUnboundedOrInvalidPool() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresqlDataSources.configuration("jdbc:postgresql://database/db", "", "", 0));
    assertThrows(
        IllegalArgumentException.class, () -> PostgresqlDataSources.configuration(" ", "", "", 8));
  }
}
