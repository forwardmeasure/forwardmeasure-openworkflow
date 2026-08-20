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
package com.forwardmeasure.openworkflow.binding.quarkus;

import com.forwardmeasure.openworkflow.definition.management.jaxrs.CorrelationIdProvider;
import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.UUID;

/** Reads {@code X-Correlation-ID} from Quarkus RESTEasy Reactive's request-scoped headers. */
@RequestScoped
public class QuarkusCorrelationIdProvider implements CorrelationIdProvider {
  private final HttpHeaders headers;

  public QuarkusCorrelationIdProvider(HttpHeaders headers) {
    this.headers = headers;
  }

  @Override
  public String current() {
    String value = headers.getHeaderString("X-Correlation-ID");
    return value == null || value.isBlank() ? UUID.randomUUID().toString() : value;
  }
}
