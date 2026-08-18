package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

/** One adapter observation, represented as either task output or RFC 9457 error. */
public record HttpOperationResult(JsonNode output, JsonNode error) {
  public HttpOperationResult {
    if ((output == null) == (error == null)) {
      throw new IllegalArgumentException("Exactly one output or error is required");
    }
    output = output == null ? null : output.deepCopy();
    error = error == null ? null : error.deepCopy();
  }

  public static HttpOperationResult success(JsonNode output) {
    return new HttpOperationResult(Objects.requireNonNull(output), null);
  }

  public static HttpOperationResult failure(JsonNode error) {
    return new HttpOperationResult(null, Objects.requireNonNull(error));
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
