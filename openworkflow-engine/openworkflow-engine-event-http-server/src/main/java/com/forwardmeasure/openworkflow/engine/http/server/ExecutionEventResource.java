/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.engine.http.server;

import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Internal ingress retained with execution management until it is deliberately split later. */
@Path("/internal/v1/execution-events/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class ExecutionEventResource {
  private final ExecutionEventSink sink;

  public ExecutionEventResource(ExecutionEventSink sink) {
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  @POST
  @Path("events")
  public CompletionStage<Void> project(
      ExecutionEvent event, @QueryParam("next") @DefaultValue("false") boolean next) {
    return next ? sink.projectNext(event) : sink.project(event);
  }
}
