package com.forwardmeasure.openworkflow.adapter.http;

import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import java.net.URI;

/**
 * Resolves a logical HTTP target and authorizes the resulting physical target immediately before
 * network I/O.
 *
 * <p>The default resolver is the identity function so embedded callers remain source compatible.
 * Production services may bind an immutable logical endpoint from a workflow contract to a
 * deployment-owned service address.
 */
@FunctionalInterface
public interface HttpEndpointPolicy {

  void authorize(OperationRequest request, String method, URI endpoint);

  default URI resolve(OperationRequest request, String method, URI logicalEndpoint) {
    return logicalEndpoint;
  }

  static HttpEndpointPolicy allowAll() {
    return (request, method, endpoint) -> {
      // Explicit embedded/test default. Production services inject policy.
    };
  }

  static HttpEndpointPolicy denyAll() {
    return (request, method, endpoint) -> {
      throw new SecurityException("No HTTP endpoint policy has been configured");
    };
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
