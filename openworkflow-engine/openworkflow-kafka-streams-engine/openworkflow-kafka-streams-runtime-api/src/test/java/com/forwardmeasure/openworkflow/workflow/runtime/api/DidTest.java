package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class DidTest {

  @Test
  void acceptsTheW3cCoreSyntaxAndExposesMethodParts() {
    Did did =
        Did.parse("did:web:tenant.example.com:actors:" + "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");

    assertEquals("web", did.method());
    assertEquals(
        "tenant.example.com:actors:" + "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45",
        did.methodSpecificId());
    assertEquals("did:example:abc%20def", Did.parse("did:example:abc%20def").toString());
  }

  @Test
  void rejectsDidUrlsAndMalformedDidsWhereASubjectDidIsRequired() {
    assertThrows(
        IllegalArgumentException.class, () -> Did.parse("did:web:tenant.example.com/actors/1"));
    assertThrows(IllegalArgumentException.class, () -> Did.parse("did:Web:tenant.example.com"));
    assertThrows(IllegalArgumentException.class, () -> Did.parse("did:web:bad%2"));
    assertThrows(IllegalArgumentException.class, () -> Did.parse("https://tenant.example.com"));
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
