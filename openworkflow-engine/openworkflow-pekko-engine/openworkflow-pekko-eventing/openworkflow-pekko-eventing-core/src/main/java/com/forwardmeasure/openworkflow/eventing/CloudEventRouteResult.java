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
package com.forwardmeasure.openworkflow.eventing;

import java.util.List;

/** Aggregate persist-confirmed result for one tenant-wide broker delivery. */
public record CloudEventRouteResult(
    int discoveredTargets, int acceptedTargets, List<String> retryableCodes) {
  public CloudEventRouteResult {
    if (discoveredTargets < 0 || acceptedTargets < 0 || acceptedTargets > discoveredTargets) {
      throw new IllegalArgumentException("Invalid CloudEvent routing counts");
    }
    retryableCodes = retryableCodes == null ? List.of() : List.copyOf(retryableCodes);
  }

  public boolean accepted() {
    return retryableCodes.isEmpty() && acceptedTargets == discoveredTargets;
  }
}
