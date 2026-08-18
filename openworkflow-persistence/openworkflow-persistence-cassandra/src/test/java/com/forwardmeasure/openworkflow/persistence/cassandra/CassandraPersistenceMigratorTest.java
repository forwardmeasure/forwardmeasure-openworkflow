/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.persistence.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

class CassandraPersistenceMigratorTest {
  @Test
  void installsAllTenantKeyedPersistenceKeyspacesRestartSafely() {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      try (CqlSession session =
          CqlSession.builder()
              .addContactPoint(
                  new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042)))
              .withLocalDatacenter(cassandra.getLocalDatacenter())
              .build()) {
        CassandraPersistenceMigrator.migrate(session);
        CassandraPersistenceMigrator.migrate(session);
        long count =
            session
                .execute(
                    "SELECT count(*) FROM system_schema.keyspaces WHERE keyspace_name IN"
                        + " ('openworkflow_journal','openworkflow_snapshot',"
                        + "'openworkflow_projection','openworkflow_definition')")
                .one()
                .getLong(0);
        assertEquals(4, count);
      }
    }
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
