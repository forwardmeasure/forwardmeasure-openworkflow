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

import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

class OpenWorkflowMigrationsMainTest {
  @Test
  void parsesDnsAndIpv6CassandraContactPoints() {
    assertEquals(
        InetSocketAddress.createUnresolved("cassandra.internal", 9042),
        unresolved(OpenWorkflowMigrationsMain.contactPoint("cassandra.internal:9042")));
    assertEquals(
        InetSocketAddress.createUnresolved("2001:db8::1", 9142),
        unresolved(OpenWorkflowMigrationsMain.contactPoint("[2001:db8::1]:9142")));
  }

  @Test
  void rejectsContactPointsWithoutAnExplicitPort() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OpenWorkflowMigrationsMain.contactPoint("cassandra.internal"));
  }

  private static InetSocketAddress unresolved(InetSocketAddress address) {
    return InetSocketAddress.createUnresolved(address.getHostString(), address.getPort());
  }
}
