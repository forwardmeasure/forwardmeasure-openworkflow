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
package com.forwardmeasure.openworkflow.execution.query;

import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.List;
import java.util.Optional;

/** Canonical query store; callers can never select an engine-native persistence model. */
public interface ExecutionQueryRepository {
  Optional<ExecutionProjection> find(TenantId tenantId, ExecutionId executionId);

  ExecutionPage search(ExecutionSearch search);

  List<ExecutionHistoryEntry> history(
      TenantId tenantId, ExecutionId executionId, long afterSequence, int limit);
}
