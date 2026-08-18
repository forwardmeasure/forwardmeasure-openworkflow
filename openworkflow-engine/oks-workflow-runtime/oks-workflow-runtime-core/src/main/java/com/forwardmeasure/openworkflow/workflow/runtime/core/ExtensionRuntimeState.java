package com.forwardmeasure.openworkflow.workflow.runtime.core;

import java.util.List;
import java.util.Objects;

/**
 * Conditions selected once when a reusable extension wrapper is entered.
 *
 * <p>A switch case or catch clause on the original target can select a flow directive dynamically.
 * It is retained here until the wrapper has completed its after middleware, then applied in the
 * target's original parent scope.
 */
public record ExtensionRuntimeState(List<Boolean> applies, String deferredDirective) {
  public ExtensionRuntimeState {
    applies = List.copyOf(Objects.requireNonNull(applies, "applies"));
    if (applies.isEmpty()) {
      throw new IllegalArgumentException("Extension runtime state requires at least one decision");
    }
    if (deferredDirective != null && deferredDirective.isBlank()) {
      throw new IllegalArgumentException("deferredDirective must be null or non-blank");
    }
  }

  public ExtensionRuntimeState(List<Boolean> applies) {
    this(applies, null);
  }

  public ExtensionRuntimeState defer(String directive) {
    Objects.requireNonNull(directive, "directive");
    return new ExtensionRuntimeState(applies, directive);
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
