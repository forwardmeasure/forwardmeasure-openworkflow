/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.adapter.http.HttpCallAdapter;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
final class KafkaOperationAdapterDispatcherIntegrationTest {
  @Container
  static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  @Test
  void committedEffectInvokesHttpAndPublishesDurableObservation() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String effects = "test.openworkflow.adapter." + suffix + ".effects";
    String definitions = "test.openworkflow.adapter." + suffix + ".definitions";
    String commands = "test.openworkflow.adapter." + suffix + ".commands";
    createTopics(effects, definitions, commands, effects + ".dead-letter");

    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/extract",
        exchange -> {
          byte[] request = exchange.getRequestBody().readAllBytes();
          ObjectNode response = JsonNodeFactory.instance.objectNode();
          response.put("method", exchange.getRequestMethod());
          response.put("body", new String(request, StandardCharsets.UTF_8));
          byte[] bytes = AdapterJson.write(response);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();

    OksTenantId tenant = OksTenantId.parse("did:web:tenant.example.com");
    Instant admittedAt = Instant.parse("2026-07-29T12:00:00Z");
    ActorContext actor = actor(tenant, admittedAt);
    WorkflowDefinitionBundle bundle = definition(tenant, actor, admittedAt, server.getAddress());
    Properties kafka = baseProperties();
    ActorId runtimeActor = ActorId.parse("did:web:runtime.openworkflow.test:actors:adapter");
    try (KafkaOperationAdapterDispatcher dispatcher =
            new KafkaOperationAdapterDispatcher(
                kafka,
                effects,
                definitions,
                commands,
                "openworkflow-adapter-test-" + suffix,
                "instance-1",
                List.of(new HttpCallAdapter()),
                request ->
                    java.util.concurrent.CompletableFuture.completedFuture(
                        new SecuredOperationRequest(request)),
                runtimeActor,
                "openworkflow-operation-adapter");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties());
        KafkaConsumer<String, byte[]> consumer =
            new KafkaConsumer<>(consumerProperties("adapter-result-" + suffix))) {
      dispatcher.start();
      consumer.subscribe(List.of(commands));
      producer
          .send(
              new ProducerRecord<>(
                  definitions, bundle.reference().canonical(), AdapterJson.write(bundle)))
          .get();
      WorkflowEffect effect = dispatchEffect(server.getAddress(), bundle, actor, admittedAt);
      producer
          .send(new ProducerRecord<>(effects, effect.key().canonical(), AdapterJson.write(effect)))
          .get();

      ObserveOperationCommand command = awaitObservation(consumer, dispatcher);
      assertEquals(effect.key(), command.key());
      assertEquals("operation-1", command.operationId());
      assertEquals(OperationObservationStatus.SUCCEEDED, command.observation().status());
      assertNull(command.observation().error());
      assertEquals(
          "POST", command.observation().output().inlineValue().required("method").textValue());
      assertTrue(
          command
              .observation()
              .output()
              .inlineValue()
              .required("body")
              .textValue()
              .contains("\"evidenceId\":\"E-1\""));
      assertTrue(dispatcher.running(), () -> String.valueOf(dispatcher.failure()));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void missingSecurityResolverFailsClosed() {
    var failure = OperationSecurityResolver.rejecting().secure(null).toCompletableFuture();
    assertInstanceOf(SecurityException.class, failure.handle((value, error) -> error).join());
  }

  private WorkflowDefinitionBundle definition(
      OksTenantId tenant, ActorContext actor, Instant admittedAt, InetSocketAddress address) {
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: http-extraction
          version: '1.0.0'
        do:
          - extract:
              call: http
              with:
                method: POST
                endpoint: http://127.0.0.1:%d/extract
                output: content
                body:
                  evidenceId: '${ .evidenceId }'
        """
            .formatted(address.getPort());
    var plan =
        new OpenWorkflowCompiler().compile(source.getBytes(StandardCharsets.UTF_8), List.of());
    return new WorkflowDefinitionBundle(
        new WorkflowDefinitionKey(tenant, plan.coordinates()),
        source,
        plan,
        OpenWorkflowCompiler.COMPILER_SHA256,
        "admit-" + plan.coordinates().name(),
        actor,
        admittedAt);
  }

  private WorkflowEffect dispatchEffect(
      InetSocketAddress address,
      WorkflowDefinitionBundle bundle,
      ActorContext actor,
      Instant requestedAt) {
    ExecutionKey key = new ExecutionKey(actor.tenantId(), new WorkflowExecutionId("execution-1"));
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.put("method", "POST");
    arguments.put("endpoint", "http://127.0.0.1:" + address.getPort() + "/extract");
    arguments.put("output", "content");
    arguments.putObject("body").put("evidenceId", "E-1");
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", "operation-1");
    descriptor.put("operationKind", "call");
    descriptor.put("executionKey", key.canonical());
    descriptor.put("definitionReference", bundle.reference().canonical());
    descriptor.put("definitionSha256", bundle.plan().definitionSha256());
    descriptor.put("taskPath", "/do/0/extract");
    descriptor.put("callKind", "HTTP");
    descriptor.set("arguments", arguments);
    descriptor.set("taskInput", JsonNodeFactory.instance.objectNode().put("evidenceId", "E-1"));
    return new WorkflowEffect(
        "effect-1",
        key,
        WorkflowEffectType.DISPATCH_OPERATION,
        "/do/0/extract",
        DataReferences.inline(descriptor),
        actor,
        requestedAt);
  }

  private ActorContext actor(OksTenantId tenant, Instant authenticatedAt) {
    return new ActorContext(
        tenant,
        ActorId.parse("did:web:tenant.example.com:actors:user-1"),
        ActorType.HUMAN,
        "User One",
        "ssb-public",
        Set.of("evidence-control"),
        null,
        authenticatedAt);
  }

  private ObserveOperationCommand awaitObservation(
      KafkaConsumer<String, byte[]> consumer, KafkaOperationAdapterDispatcher dispatcher) {
    Instant deadline = Instant.now().plusSeconds(30);
    while (Instant.now().isBefore(deadline)) {
      if (dispatcher.failure() != null) {
        throw new AssertionError("Operation adapter dispatcher failed", dispatcher.failure());
      }
      for (var record : consumer.poll(Duration.ofMillis(250))) {
        ExecutionCommand command = AdapterJson.read(record.value(), ExecutionCommand.class);
        return assertInstanceOf(ObserveOperationCommand.class, command);
      }
    }
    throw new AssertionError("No operation observation arrived within 30 seconds");
  }

  private void createTopics(String... names) throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers()))) {
      admin
          .createTopics(
              java.util.Arrays.stream(names).map(name -> new NewTopic(name, 1, (short) 1)).toList())
          .all()
          .get();
    }
  }

  private Properties baseProperties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
    return properties;
  }

  private Properties producerProperties() {
    Properties properties = baseProperties();
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", ByteArraySerializer.class.getName());
    properties.put("acks", "all");
    return properties;
  }

  private Properties consumerProperties(String groupId) {
    Properties properties = baseProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return properties;
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
