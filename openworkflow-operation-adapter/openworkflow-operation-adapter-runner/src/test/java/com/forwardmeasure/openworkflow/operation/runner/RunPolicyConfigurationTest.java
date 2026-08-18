package com.forwardmeasure.openworkflow.operation.runner;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import org.junit.jupiter.api.Test;

final class RunPolicyConfigurationTest {
  @Test
  void everyRunnerCapabilityIsTenantQualifiedAndDenyByDefault() {
    var north = new TenantId("did:web:forwardmeasure.com:tenant:north");
    var south = new TenantId("did:web:forwardmeasure.com:tenant:south");
    var policy =
        RunPolicyConfiguration.policy(
            north.value() + "=/usr/bin/printf",
            north.value() + "=python:/usr/bin/python3",
            north.value() + "=registry.test/job@sha256:" + "a".repeat(64),
            north.value() + "=/safe/input>/work/input",
            north.value() + "=127.0.0.1:18080>8080");

    assertDoesNotThrow(() -> policy.authorizeCommand(north, "/usr/bin/printf"));
    assertEquals("/usr/bin/python3", policy.interpreter(north, "PYTHON"));
    assertDoesNotThrow(() -> policy.authorizeVolume(north, "/safe/input", "/work/input"));
    assertDoesNotThrow(() -> policy.authorizePort(north, "127.0.0.1:18080", "8080"));
    assertThrows(SecurityException.class, () -> policy.authorizeCommand(south, "/usr/bin/printf"));
    assertThrows(
        SecurityException.class,
        () ->
            RunExecutionPolicy.rejecting()
                .authorizeImage(north, "registry.test/job@sha256:" + "a".repeat(64)));
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
