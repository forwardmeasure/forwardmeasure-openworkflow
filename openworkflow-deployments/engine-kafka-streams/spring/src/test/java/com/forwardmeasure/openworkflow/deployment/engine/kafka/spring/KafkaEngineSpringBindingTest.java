/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsEngineRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksInboundCloudEventGateway;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.OksCloudEventIngressResource;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;

/**
 * Exercises the wiring performed by {@link KafkaEngineSpringBinding} without a live Kafka broker.
 * The deployment module has no testcontainers-kafka convention of its own (that lives one layer
 * down, in {@code openworkflow-kafka-streams-engine}, which already covers real broker behaviour);
 * a full {@code @SpringBootTest} boot here would try to construct a real KafkaStreamsEngineRuntime
 * bean, which contacts a broker even to admin-create topics, so these tests instead invoke the
 * {@code @Bean} methods directly as plain Java calls and use a Mockito mockConstruction to stand in
 * for the Kafka client layer.
 *
 * <p>The specific regression this guards against: earlier this effort the sibling Pekko binding
 * built a fully-formed, correctly configured runtime object but never actually called start() on
 * it, so the engine looked wired but silently never processed a single submitted execution. The
 * {@code kafkaRuntime_startsTopologyAndWiresProviderChain} test below asserts start() is reached
 * for this Kafka Streams binding specifically so that failure shape cannot reoccur here unnoticed.
 */
class KafkaEngineSpringBindingTest {
  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void executionEvents_missingUrlFailsFast() {
    var binding = new KafkaEngineSpringBinding();
    var mapper = new ObjectMapper();
    NullPointerException thrown =
        assertThrows(
            NullPointerException.class,
            () -> binding.executionEvents(mapper, null, Duration.ofSeconds(1)));
    assertEquals("endpoint", thrown.getMessage());
  }

  @Test
  void executionEvents_missingTimeoutFailsFast() {
    var binding = new KafkaEngineSpringBinding();
    var mapper = new ObjectMapper();
    assertThrows(
        NullPointerException.class,
        () -> binding.executionEvents(mapper, URI.create("http://localhost:1/"), null));
  }

  @Test
  void executionEvents_wiresConfiguredEndpointAndDeliversSerializedEvent() throws Exception {
    AtomicReference<CapturedRequest> received = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/ingress/",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          received.set(
              new CapturedRequest(
                  exchange.getRequestMethod(),
                  exchange.getRequestURI().toString(),
                  exchange.getRequestHeaders().getFirst("Content-Type"),
                  body));
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    server.start();

    var binding = new KafkaEngineSpringBinding();
    var mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/ingress/");
    ExecutionEventSink sink = binding.executionEvents(mapper, endpoint, Duration.ofSeconds(5));
    assertNotNull(sink);

    ExecutionEvent event =
        new ExecutionEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            new ExecutionId(new TenantId(UUID.randomUUID()), UUID.randomUUID()),
            EngineId.KAFKA_STREAMS,
            0L,
            ExecutionEvent.EventType.STARTED,
            ExecutionLifecycleState.RUNNING,
            Instant.now(),
            JsonNodeFactory.instance.objectNode());

    sink.project(event).toCompletableFuture().get(5, TimeUnit.SECONDS);

    CapturedRequest capture = received.get();
    assertNotNull(capture, "binding never delivered the event to the configured endpoint");
    assertEquals("POST", capture.method());
    assertEquals("/ingress/events?next=false", capture.uri());
    assertEquals("application/json", capture.contentType());
    JsonNode delivered = mapper.readTree(capture.body());
    assertEquals(event.eventId().toString(), delivered.get("eventId").asText());
    assertEquals(event.commandId().toString(), delivered.get("commandId").asText());
  }

  @Test
  void kafkaRuntime_missingBootstrapServersFailsFast(@TempDir Path tempDir) {
    var binding = new KafkaEngineSpringBinding();
    ExecutionEventSink events = event -> CompletableFuture.completedFuture(null);
    NullPointerException thrown =
        assertThrows(
            NullPointerException.class,
            () -> binding.kafkaRuntime(events, null, "app-id", "instance-1", "owf-", tempDir));
    assertEquals("bootstrapServers", thrown.getMessage());
  }

  @Test
  void kafkaRuntime_startsTopologyAndWiresProviderChain(@TempDir Path tempDir) {
    var binding = new KafkaEngineSpringBinding();
    ExecutionEventSink events = event -> CompletableFuture.completedFuture(null);
    ExecutionEngineProvider fakeProvider = mock(ExecutionEngineProvider.class);

    try (MockedConstruction<KafkaStreamsEngineRuntime> constructed =
        mockConstruction(
            KafkaStreamsEngineRuntime.class,
            (mock, context) -> {
              assertEquals(2, context.arguments().size());
              var configuration =
                  (KafkaStreamsEngineRuntime.Configuration) context.arguments().get(0);
              assertEquals("broker:9092", configuration.bootstrapServers());
              assertEquals("app-id", configuration.applicationId());
              assertEquals("instance-1", configuration.instanceId());
              assertEquals("owf-", configuration.topicPrefix());
              assertEquals(tempDir, configuration.stateDirectory());
              assertSame(events, context.arguments().get(1));
              when(mock.provider()).thenReturn(fakeProvider);
            })) {
      KafkaStreamsEngineRuntime runtime =
          binding.kafkaRuntime(events, "broker:9092", "app-id", "instance-1", "owf-", tempDir);

      assertEquals(1, constructed.constructed().size());
      assertSame(constructed.constructed().get(0), runtime);
      // The exact regression this guards against: a binding that builds a correct runtime but
      // never starts it, leaving the engine silently inert. See class-level Javadoc.
      verify(runtime, times(1)).start();

      ExecutionEngineProvider provider = binding.engine(runtime);
      assertSame(fakeProvider, provider);
    }
  }

  @Test
  void commands_delegatesSubmitAndHealthToProvider() {
    var binding = new KafkaEngineSpringBinding();
    ExecutionEngineProvider provider = mock(ExecutionEngineProvider.class);

    EngineCommandResource resource = binding.commands(provider);
    assertNotNull(resource);

    resource.submit(null);
    verify(provider, times(1)).submit(null);

    resource.health();
    verify(provider, times(1)).health();
  }

  @Test
  void cloudEvents_wiresRuntimeInboundGatewayIntoTheResource() {
    var binding = new KafkaEngineSpringBinding();
    KafkaStreamsEngineRuntime runtime = mock(KafkaStreamsEngineRuntime.class);
    OksInboundCloudEventGateway gateway = mock(OksInboundCloudEventGateway.class);
    when(runtime.inboundEvents()).thenReturn(gateway);
    ActiveOrganizationProvider organizations = mock(ActiveOrganizationProvider.class);
    AuthorizationService authorization = mock(AuthorizationService.class);

    OksCloudEventIngressResource resource =
        binding.cloudEvents(runtime, organizations, authorization, new ObjectMapper());
    assertNotNull(resource);
    // The exact regression this guards against: a resource bean that is built against the wrong
    // (or a fresh, unwired) gateway instead of the one runtime.inboundEvents() actually owns,
    // leaving posted CloudEvents published through a producer nothing else observes.
    verify(runtime, times(1)).inboundEvents();
  }

  @Test
  void engineResources_registersBothCommandsAndCloudEventsResourcesOnJerseyConfig() {
    var binding = new KafkaEngineSpringBinding();
    EngineCommandResource commands = new EngineCommandResource(mock(ExecutionEngineProvider.class));
    OksCloudEventIngressResource cloudEvents =
        new OksCloudEventIngressResource(
            mock(OksInboundCloudEventGateway.class),
            mock(ActiveOrganizationProvider.class),
            mock(AuthorizationService.class),
            new ObjectMapper());

    ResourceConfigCustomizer customizer = binding.engineResources(commands, cloudEvents);
    assertNotNull(customizer);

    ResourceConfig resourceConfig = new ResourceConfig();
    customizer.customize(resourceConfig);
    // The exact regression this guards against: a customizer bean that is built but never
    // actually registers a resource on the Jersey config, leaving that HTTP endpoint unreachable
    // even though every other bean in the chain looks correctly wired.
    assertTrue(resourceConfig.isRegistered(commands));
    assertTrue(resourceConfig.isRegistered(cloudEvents));
  }

  private record CapturedRequest(String method, String uri, String contentType, byte[] body) {}
}
