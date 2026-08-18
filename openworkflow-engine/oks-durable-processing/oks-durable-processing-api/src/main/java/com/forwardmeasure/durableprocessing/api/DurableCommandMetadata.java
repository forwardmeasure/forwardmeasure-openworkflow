package com.forwardmeasure.durableprocessing.api;

import java.time.Instant;
import java.util.OptionalLong;

/** Extracts durability metadata without imposing a command representation on the caller. */
public interface DurableCommandMetadata<C> {
  String aggregateKey(C command);

  String commandId(C command);

  Instant requestedAt(C command);

  /** Stable semantic command type exposed by the durable outcome projection. */
  default String commandType(C command) {
    return command.getClass().getSimpleName();
  }

  OptionalLong expectedRevision(C command);

  /**
   * Internal revision-guarded continuations do not need durable command-ID receipts. Authenticated
   * external commands normally do.
   */
  boolean deduplicate(C command);
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
