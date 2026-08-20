package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import java.util.Objects;

/** Canonical tenant-scoped identity of one immutable workflow version. */
public record WorkflowDefinitionKey(OksTenantId tenantId, WorkflowCoordinates coordinates)
    implements Comparable<WorkflowDefinitionKey> {

  public WorkflowDefinitionKey {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(coordinates, "coordinates");
  }

  public String canonical() {
    return component(tenantId.toString())
        + component(coordinates.namespace())
        + component(coordinates.name())
        + component(coordinates.version())
        + component(coordinates.dsl());
  }

  @Override
  public int compareTo(WorkflowDefinitionKey other) {
    return canonical().compareTo(other.canonical());
  }

  @Override
  public String toString() {
    return canonical();
  }

  private static String component(String value) {
    return value.length() + ":" + value;
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
