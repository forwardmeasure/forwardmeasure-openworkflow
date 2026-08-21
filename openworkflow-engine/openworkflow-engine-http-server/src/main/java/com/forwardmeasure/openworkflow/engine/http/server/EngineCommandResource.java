/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.engine.http.server;

import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Internal command endpoint hosted by one engine-flavour image. */
@Path("/internal/v1/engine/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class EngineCommandResource {
  private final ExecutionEngineProvider provider;

  public EngineCommandResource(ExecutionEngineProvider provider) {
    this.provider = Objects.requireNonNull(provider, "provider");
  }

  @POST
  @Path("commands")
  public CompletionStage<?> submit(ExecutionCommandEnvelope envelope) {
    return provider.submit(envelope);
  }

  @GET
  @Path("health")
  public Object health() {
    return provider.health();
  }
}
