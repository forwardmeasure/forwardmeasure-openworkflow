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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.data.RuntimeDataLimitException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksInboundCloudEventGateway;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Production REST ingress for external CloudEvents: the only production code path that lets an
 * external system deliver a CloudEvent onto {@code inbound-events}, which {@code
 * OksInboundEventProcessor} (unchanged) journals and routes to matching {@code receive:}/{@code
 * listen:} subscriptions. Before this resource existed, that consumer side was only ever exercised
 * by test code using a raw {@code KafkaProducer} - confirmed by a repo-wide search for production
 * producers onto the topic; there were none.
 *
 * <p>Mirrors the Pekko engine's {@code CloudEventIngressResource} {@code @Path}/{@code @Consumes}
 * shape and its general (target-less) {@code route()} method exactly - same path, same accepted
 * content types. It does not mirror that resource's {@code /executions/{id}} and {@code
 * /schedules/.../} sub-paths: those exist only because the Pekko engine has a separate
 * per-execution/per-schedule direct-delivery command path. The Kafka Streams engine has no
 * equivalent - every accepted event here is routed purely by {@code receive:}/{@code listen:}
 * subscription matching in {@code OksInboundEventProcessor}, so the single general route endpoint
 * is the complete mapping.
 */
@Path("/v1/cloud-events")
@Consumes({
  "application/cloudevents+json",
  MediaType.APPLICATION_JSON,
  MediaType.APPLICATION_OCTET_STREAM
})
@Produces(MediaType.APPLICATION_JSON)
public final class OksCloudEventIngressResource {
  private final OksInboundCloudEventGateway events;
  private final OksCloudEventIngressAuthorizer authorizer;
  private final OksCloudEventHttpDecoder decoder;
  private final Clock clock;

  public OksCloudEventIngressResource(
      OksInboundCloudEventGateway events,
      ActiveOrganizationProvider organizations,
      AuthorizationService authorization,
      ObjectMapper mapper) {
    this(events, organizations, authorization, mapper, Clock.systemUTC());
  }

  OksCloudEventIngressResource(
      OksInboundCloudEventGateway events,
      ActiveOrganizationProvider organizations,
      AuthorizationService authorization,
      ObjectMapper mapper,
      Clock clock) {
    this.events = Objects.requireNonNull(events, "events");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.authorizer = new OksCloudEventIngressAuthorizer(organizations, authorization, clock);
    this.decoder = new OksCloudEventHttpDecoder(mapper);
  }

  @POST
  public CompletionStage<Response> ingest(
      @HeaderParam(HttpHeaders.CONTENT_TYPE) String contentType,
      @Context HttpHeaders headers,
      byte[] body) {
    return ingest(contentType, copy(headers), body);
  }

  /** Framework-agnostic entry point, exercised directly by unit and integration tests. */
  public CompletionStage<Response> ingest(
      String contentType, Map<String, List<String>> headers, byte[] body) {
    ActorContext acceptedBy = authorizer.authorizeEventRoute();
    JsonNode envelope = decode(contentType, headers, body);
    InboundCloudEvent inbound = accept(acceptedBy, envelope);
    return events
        .publish(inbound)
        .thenApply(
            ignored ->
                Response.accepted(
                        new CloudEventIngressAcceptedDocument(
                            inbound.tenantId().toString(),
                            envelope.path("id").asText(),
                            envelope.path("source").asText(),
                            inbound.eventKey(),
                            acceptedBy.correlationId().toString()))
                    .build());
  }

  private JsonNode decode(String contentType, Map<String, List<String>> headers, byte[] body) {
    try {
      return decoder.decode(contentType, headers, body);
    } catch (RuntimeException malformed) {
      throw new OksCloudEventIngressException(
          OksCloudEventIngressException.Kind.MALFORMED,
          "Malformed CloudEvent: " + malformed.getMessage(),
          malformed);
    }
  }

  private InboundCloudEvent accept(ActorContext acceptedBy, JsonNode envelope) {
    try {
      return new InboundCloudEvent(
          acceptedBy.tenantId(), DataReferences.inline(envelope), acceptedBy, Instant.now(clock));
    } catch (RuntimeDataLimitException tooLarge) {
      throw new OksCloudEventIngressException(
          OksCloudEventIngressException.Kind.TOO_LARGE, tooLarge.getMessage(), tooLarge);
    } catch (IllegalArgumentException invalid) {
      throw new OksCloudEventIngressException(
          OksCloudEventIngressException.Kind.MALFORMED, invalid.getMessage(), invalid);
    }
  }

  private static Map<String, List<String>> copy(HttpHeaders headers) {
    return headers.getRequestHeaders().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
  }
}
