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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/** Immutable provider registry used by the trusted execution-admission service. */
public final class ExecutionEngineProviders {
  private final Map<EngineId, ExecutionEngineProvider> providers;

  public ExecutionEngineProviders(Collection<? extends ExecutionEngineProvider> providers) {
    Objects.requireNonNull(providers, "providers");
    var indexed = new HashMap<EngineId, ExecutionEngineProvider>();
    for (ExecutionEngineProvider provider : providers) {
      Objects.requireNonNull(provider, "provider");
      if (indexed.putIfAbsent(provider.engineId(), provider) != null) {
        throw new IllegalArgumentException(
            "duplicate engine provider " + provider.engineId().value());
      }
    }
    if (indexed.isEmpty()) {
      throw new IllegalArgumentException("at least one engine provider is required");
    }
    this.providers = Map.copyOf(indexed);
  }

  public ExecutionEngineProvider require(EngineId engineId) {
    Objects.requireNonNull(engineId, "engineId");
    var provider = providers.get(engineId);
    if (provider == null) {
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE,
          "engine provider is not available: " + engineId.value());
    }
    return provider;
  }

  /** Selects only among currently ready providers; the result is persisted by admission. */
  public ExecutionEngineProvider select(
      TenantActorContext context, DefinitionRevision definition, ExecutionEngineSelector selector) {
    Objects.requireNonNull(selector, "selector");
    var ready = new TreeSet<EngineId>();
    providers.values().stream()
        .filter(provider -> provider.health().ready())
        .map(ExecutionEngineProvider::engineId)
        .forEach(ready::add);
    if (ready.isEmpty()) {
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE, "no execution engine is ready");
    }
    EngineId selected =
        Objects.requireNonNull(
            selector.select(new EngineSelectionRequest(context, definition, ready)),
            "selector result");
    if (!ready.contains(selected)) {
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE,
          "selection policy chose an unavailable engine: " + selected.value());
    }
    return providers.get(selected);
  }
}
