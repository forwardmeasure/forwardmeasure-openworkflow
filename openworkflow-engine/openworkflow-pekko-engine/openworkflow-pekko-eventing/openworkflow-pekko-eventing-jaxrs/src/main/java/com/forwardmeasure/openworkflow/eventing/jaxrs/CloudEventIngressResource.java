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
package com.forwardmeasure.openworkflow.eventing.jaxrs;

import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.eventing.CloudEventHttpDecoder;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/** Tenant-qualified CloudEvents HTTP ingress for executions and event schedules. */
@Path("/v1/cloud-events")
@Consumes({
  "application/cloudevents+json",
  MediaType.APPLICATION_JSON,
  MediaType.APPLICATION_OCTET_STREAM
})
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public final class CloudEventIngressResource {
  private final CloudEventIngress ingress;
  private final AuthenticatedActorProvider actors;
  private final CloudEventHttpDecoder decoder;
  private final Clock clock;

  @Inject
  public CloudEventIngressResource(
      CloudEventIngress ingress, AuthenticatedActorProvider actors, CloudEventHttpDecoder decoder) {
    this(ingress, actors, decoder, Clock.systemUTC());
  }

  CloudEventIngressResource(
      CloudEventIngress ingress,
      AuthenticatedActorProvider actors,
      CloudEventHttpDecoder decoder,
      Clock clock) {
    this.ingress = Objects.requireNonNull(ingress, "ingress");
    this.actors = Objects.requireNonNull(actors, "actors");
    this.decoder = Objects.requireNonNull(decoder, "decoder");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @POST
  public CompletionStage<Response> route(
      @HeaderParam(HttpHeaders.CONTENT_TYPE) String contentType,
      @jakarta.ws.rs.core.Context jakarta.ws.rs.core.HttpHeaders headers,
      byte[] body) {
    return route(contentType, copy(headers), body);
  }

  public CompletionStage<Response> route(
      String contentType, Map<String, List<String>> headers, byte[] body) {
    var tenant = actors.authorize("event.route", "event-target", "*").tenantId();
    return ingress
        .route(tenant, decode(contentType, headers, body), clock.instant())
        .thenApply(
            result -> {
              var document =
                  new CloudEventRouteDocument(
                      result.accepted(), result.discoveredTargets(),
                      result.acceptedTargets(), result.retryableCodes());
              return (result.accepted()
                      ? Response.accepted(document)
                      : Response.status(Response.Status.CONFLICT).entity(document))
                  .build();
            });
  }

  @POST
  @Path("/executions/{executionId}")
  public CompletionStage<Response> execution(
      @PathParam("executionId") UUID executionId,
      @HeaderParam(HttpHeaders.CONTENT_TYPE) String contentType,
      @jakarta.ws.rs.core.Context jakarta.ws.rs.core.HttpHeaders headers,
      byte[] body) {
    return execution(executionId, contentType, copy(headers), body);
  }

  public CompletionStage<Response> execution(
      UUID executionId, String contentType, Map<String, List<String>> headers, byte[] body) {
    var tenant = actors.authorize("event.deliver", "execution", executionId.toString()).tenantId();
    return ingress
        .deliver(
            new ExecutionId(tenant, executionId),
            decode(contentType, headers, body),
            clock.instant())
        .thenApply(CloudEventIngressResource::response);
  }

  @POST
  @Path("/schedules/{namespace}/{name}/{version}")
  public CompletionStage<Response> schedule(
      @PathParam("namespace") String namespace,
      @PathParam("name") String name,
      @PathParam("version") String version,
      @QueryParam("dsl") @DefaultValue("1.0.3") String dsl,
      @HeaderParam(HttpHeaders.CONTENT_TYPE) String contentType,
      @jakarta.ws.rs.core.Context jakarta.ws.rs.core.HttpHeaders headers,
      byte[] body) {
    return schedule(namespace, name, version, dsl, contentType, copy(headers), body);
  }

  public CompletionStage<Response> schedule(
      String namespace,
      String name,
      String version,
      String dsl,
      String contentType,
      Map<String, List<String>> headers,
      byte[] body) {
    var resourceId = namespace + "/" + name + "/" + version + "/" + dsl;
    var scheduleId =
        new ScheduleId(
            actors.authorize("event.deliver", "schedule", resourceId).tenantId(),
            new WorkflowCoordinates(namespace, name, version, dsl));
    return ingress
        .deliver(scheduleId, decode(contentType, headers, body), clock.instant())
        .thenApply(CloudEventIngressResource::response);
  }

  private com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent decode(
      String contentType, Map<String, List<String>> headers, byte[] body) {
    try {
      return decoder.decode(contentType, headers, body);
    } catch (RuntimeException malformed) {
      throw new BadRequestException("Malformed CloudEvent", malformed);
    }
  }

  private static Map<String, List<String>> copy(jakarta.ws.rs.core.HttpHeaders headers) {
    return headers.getRequestHeaders().entrySet().stream()
        .collect(
            java.util.stream.Collectors.toMap(
                Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
  }

  private static Response response(WorkflowReply reply) {
    if (reply instanceof WorkflowReply.Accepted accepted) {
      return Response.accepted(
              new CloudEventDeliveryDocument(
                  true, accepted.revision(), accepted.status().name(), null, null))
          .build();
    }
    var rejected = (WorkflowReply.Rejected) reply;
    return Response.status(Response.Status.CONFLICT)
        .entity(
            new CloudEventDeliveryDocument(
                false,
                rejected.revision(),
                rejected.status().name(),
                rejected.code(),
                rejected.message()))
        .build();
  }

  private static Response response(ScheduleReply reply) {
    if (reply instanceof ScheduleReply.Accepted accepted) {
      return Response.accepted(
              new CloudEventDeliveryDocument(true, accepted.revision(), "ACTIVE", null, null))
          .build();
    }
    var rejected = (ScheduleReply.Rejected) reply;
    return Response.status(Response.Status.CONFLICT)
        .entity(
            new CloudEventDeliveryDocument(
                false, rejected.revision(), "INACTIVE", rejected.code(), rejected.message()))
        .build();
  }
}
