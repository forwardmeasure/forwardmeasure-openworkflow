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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs;

import java.util.Objects;

/**
 * One rejected CloudEvent-ingress HTTP request. {@link OksCloudEventIngressResource} only ever
 * throws this; {@code OksCloudEventIngressExceptionMapper} is the only place that turns it into an
 * HTTP {@code Response} - same "resource methods just throw" convention as {@code
 * EngineCommandExceptionMapper}/{@code ExecutionManagementExceptionMapper}.
 */
public final class OksCloudEventIngressException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Kind kind;

  public OksCloudEventIngressException(Kind kind, String message, Throwable cause) {
    super(Objects.requireNonNull(message, "message"), cause);
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  public OksCloudEventIngressException(Kind kind, String message) {
    this(kind, message, null);
  }

  public Kind kind() {
    return kind;
  }

  public enum Kind {
    /** The request body/headers do not decode to a valid CloudEvents envelope. */
    MALFORMED,
    /** The decoded CloudEvent's inline JSON exceeds the runtime's inline data limit. */
    TOO_LARGE
  }
}
