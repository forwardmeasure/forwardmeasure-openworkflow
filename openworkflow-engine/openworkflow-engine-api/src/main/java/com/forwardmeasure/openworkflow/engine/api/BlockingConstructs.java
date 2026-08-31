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
package com.forwardmeasure.openworkflow.engine.api;

import com.forwardmeasure.openworkflow.definition.CallPlan;
import java.util.EnumSet;
import java.util.Set;

/**
 * Which {@code call} kinds represent a genuine external wait rather than active computation - the
 * single source of truth both engines consult when deciding whether a pending interaction of a
 * given kind should report {@code WAITING} instead of {@code RUNNING}. Before this class existed,
 * `openworkflow-pekko-engine` and `openworkflow-kafka-streams-engine` each hardcoded their own copy
 * of this rule for {@code CORRELATED_WORKER} independently - see
 * forwardmeasure-openworkflow/docs/engine-construct-gap-audit.md gap #4.
 */
public final class BlockingConstructs {

  private static final Set<CallPlan.Kind> BLOCKING = EnumSet.of(CallPlan.Kind.CORRELATED_WORKER);

  private BlockingConstructs() {}

  public static boolean isBlocking(CallPlan.Kind kind) {
    return BLOCKING.contains(kind);
  }
}
