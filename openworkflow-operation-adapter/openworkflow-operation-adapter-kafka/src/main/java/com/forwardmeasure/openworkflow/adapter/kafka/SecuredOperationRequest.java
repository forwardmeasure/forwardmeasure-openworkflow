/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedSecret;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Authorized adapter request whose ephemeral credentials are destroyed on close. */
public final class SecuredOperationRequest implements AutoCloseable {
  private final OperationRequest request;
  private final AtomicBoolean closed = new AtomicBoolean();

  public SecuredOperationRequest(OperationRequest request) {
    this.request = Objects.requireNonNull(request, "request");
  }

  public OperationRequest request() {
    if (closed.get()) throw new IllegalStateException("Secured operation request is closed");
    return request;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    if (request.authentication() != null) request.authentication().close();
    request.secrets().values().forEach(ResolvedSecret::close);
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
