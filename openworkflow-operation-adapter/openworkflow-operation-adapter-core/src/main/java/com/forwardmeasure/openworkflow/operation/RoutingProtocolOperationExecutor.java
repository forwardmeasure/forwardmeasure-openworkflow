package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;

/** Explicit protocol registry; an unconfigured AsyncAPI/gRPC edge fails closed. */
public final class RoutingProtocolOperationExecutor implements ProtocolOperationExecutor {
  private final Map<DriverKey, ProtocolOperationExecutor> drivers;

  public RoutingProtocolOperationExecutor(Map<DriverKey, ProtocolOperationExecutor> drivers) {
    Objects.requireNonNull(drivers, "drivers");
    var copy = new LinkedHashMap<DriverKey, ProtocolOperationExecutor>();
    drivers.forEach(
        (key, driver) ->
            copy.put(
                Objects.requireNonNull(key, "driver key"),
                Objects.requireNonNull(driver, "driver")));
    this.drivers = Map.copyOf(copy);
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    ProtocolOperationExecutor driver =
        drivers.get(new DriverKey(operation.kind(), operation.protocol()));
    if (driver == null)
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException(
              "No protocol driver is configured for "
                  + operation.kind()
                  + "/"
                  + operation.protocol()));
    return driver.execute(executionId, operation, sink);
  }

  public record DriverKey(ProtocolOperationDescriptor.Kind kind, String protocol) {
    public DriverKey {
      Objects.requireNonNull(kind, "kind");
      protocol = Objects.requireNonNull(protocol, "protocol").toLowerCase(Locale.ROOT);
      if (protocol.isBlank()) throw new IllegalArgumentException("protocol must not be blank");
    }
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
