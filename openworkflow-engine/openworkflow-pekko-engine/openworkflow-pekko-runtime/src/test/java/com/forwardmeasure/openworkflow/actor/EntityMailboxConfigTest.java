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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

class EntityMailboxConfigTest {
  @Test
  void boundsEveryDomainEntityCommandBacklog() {
    var mailbox = ConfigFactory.load().getConfig("openworkflow.entity-mailbox");
    assertEquals("org.apache.pekko.dispatch.BoundedMailbox", mailbox.getString("mailbox-type"));
    assertEquals(10_000, mailbox.getInt("mailbox-capacity"));
    assertEquals(0L, mailbox.getDuration("mailbox-push-timeout-time").toMillis());
  }
}
