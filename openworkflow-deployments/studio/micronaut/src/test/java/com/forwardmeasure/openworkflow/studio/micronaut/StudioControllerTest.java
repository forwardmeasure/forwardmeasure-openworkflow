package com.forwardmeasure.openworkflow.studio.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest
class StudioControllerTest {
  @Inject
  @Client("/")
  HttpClient http;

  @Test
  void servesSharedWebappAndRuntimeConfig() {
    HttpResponse<String> page =
        http.toBlocking().exchange(HttpRequest.GET("/studio/index.html"), String.class);
    assertEquals(200, page.code());
    assertTrue(page.body().contains("OpenWorkflow Studio"));
    HttpResponse<String> config =
        http.toBlocking().exchange(HttpRequest.GET("/studio/config.js"), String.class);
    assertEquals(200, config.code());
    assertTrue(config.body().contains("apiBasePath"));
    assertEquals("no-store", config.header("Cache-Control"));
    assertEquals(200, http.toBlocking().exchange(HttpRequest.GET("/health/readiness")).code());
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
