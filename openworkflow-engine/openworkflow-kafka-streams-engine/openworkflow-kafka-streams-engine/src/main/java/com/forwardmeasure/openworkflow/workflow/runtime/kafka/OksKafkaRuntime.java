/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.engine.api.BlockingConstructs;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

/** Lifecycle for one OKS Kafka Streams member and its canonical history projection consumer. */
public final class OksKafkaRuntime implements AutoCloseable {
  private final KafkaStreams streams;
  private final KafkaConsumer<String, byte[]> history;
  private final ExecutionEventSink sink;
  private final OksTopics topics;
  private final AtomicBoolean running = new AtomicBoolean();
  private Thread projectionThread;

  public OksKafkaRuntime(
      String bootstrapServers,
      String applicationId,
      String instanceId,
      Path stateDirectory,
      OksTopics topics,
      com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId runtimeActor,
      ExecutionEventSink sink) {
    this.topics = java.util.Objects.requireNonNull(topics, "topics");
    this.sink = java.util.Objects.requireNonNull(sink, "sink");
    createTopics(bootstrapServers, topics);
    Properties streamProperties = new Properties();
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
    streamProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    streamProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
    streamProperties.put(StreamsConfig.CLIENT_ID_CONFIG, instanceId);
    this.streams =
        new KafkaStreams(new OksTopology(runtimeActor, instanceId).build(topics), streamProperties);

    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId + "-canonical-projection");
    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, instanceId + "-canonical-projection");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    this.history = new KafkaConsumer<>(consumerProperties);
  }

  public void start() {
    if (!running.compareAndSet(false, true)) return;
    streams.start();
    history.subscribe(List.of(topics.history()));
    projectionThread =
        Thread.ofPlatform().name("openworkflow-kafka-projection").start(this::project);
  }

  public boolean ready() {
    return streams.state().isRunningOrRebalancing();
  }

  private void project() {
    JsonSerde<ExecutionHistoryEvent> serde = new JsonSerde<>(ExecutionHistoryEvent.class);
    while (running.get()) {
      try {
        var records = history.poll(Duration.ofMillis(250));
        for (var record : records) {
          ExecutionHistoryEvent source =
              serde.deserializer().deserialize(record.topic(), record.value());
          ExecutionEvent event = canonical(source);
          if (event != null) sink.project(event).toCompletableFuture().join();
        }
        if (!records.isEmpty()) history.commitSync();
      } catch (org.apache.kafka.common.errors.WakeupException stopped) {
        if (running.get()) throw stopped;
      }
    }
  }

  private static ExecutionEvent canonical(ExecutionHistoryEvent source) {
    Mapping mapping = mapping(source.type());
    if (mapping == null || source.type() == ExecutionEventType.EXECUTION_STARTED) return null;
    UUID tenant =
        UUID.fromString(
            source.key().tenantId().value().methodSpecificId().substring("tenant:".length()));
    ExecutionId id =
        new ExecutionId(new TenantId(tenant), UUID.fromString(source.key().executionId().value()));
    var data = JsonNodeFactory.instance.objectNode();
    data.put("sourceEventType", source.type().name());
    if (source.taskPath() != null) data.put("taskPath", source.taskPath());
    if (source.taskName() != null) data.put("taskName", source.taskName());
    if (source.output() != null && source.output().storage() == DataReference.Storage.INLINE) {
      if (mapping.type() == ExecutionEvent.EventType.COMPLETED)
        return event(source, id, mapping, source.output().inlineValue());
      data.set("output", source.output().inlineValue());
    }
    if (source.failure() != null) data.put("failure", source.failure().toString());
    if (mapping.type() == ExecutionEvent.EventType.ERROR_RAISED
        || mapping.type() == ExecutionEvent.EventType.FAILED) {
      return event(source, id, mapping, canonicalError(source.failure(), source.taskPath(), data));
    }
    return event(source, id, mapping, data);
  }

  static com.fasterxml.jackson.databind.JsonNode canonicalError(
      com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure failure,
      String taskPath,
      com.fasterxml.jackson.databind.node.ObjectNode details) {
    var error = JsonNodeFactory.instance.objectNode();
    error.put("code", failure == null ? "OPENWORKFLOW_RUNTIME_ERROR" : failure.type());
    error.put(
        "message", failure == null ? "Workflow runtime reported an error" : failure.message());
    String resolvedTaskPath =
        taskPath != null ? taskPath : failure == null ? null : failure.definitionPath();
    if (resolvedTaskPath == null) error.putNull("taskPath");
    else error.put("taskPath", resolvedTaskPath);
    error.put("retryable", false);
    error.set("details", details.deepCopy());
    return error;
  }

  private static ExecutionEvent event(
      ExecutionHistoryEvent source,
      ExecutionId id,
      Mapping mapping,
      com.fasterxml.jackson.databind.JsonNode data) {
    UUID eventId =
        UUID.nameUUIDFromBytes(("kafka:" + source.eventId()).getBytes(StandardCharsets.UTF_8));
    return new ExecutionEvent(
        eventId,
        eventId,
        id,
        EngineId.KAFKA_STREAMS,
        source.sequence(),
        mapping.type(),
        mapping.state(),
        source.occurredAt(),
        data);
  }

  /**
   * The canonical {@link ExecutionEvent.EventType}/{@link ExecutionLifecycleState} this engine
   * reports for a given history event type. Public (not just package-visible) specifically so
   * cross-engine parity tests outside this module can drive it directly - see
   * openworkflow-engine-cross-engine-tests.
   */
  public static Mapping mapping(ExecutionEventType type) {
    return switch (type) {
      case EXECUTION_STARTED ->
          new Mapping(ExecutionEvent.EventType.STARTED, ExecutionLifecycleState.RUNNING);
      case TASK_STARTED ->
          new Mapping(ExecutionEvent.EventType.TASK_ENTERED, ExecutionLifecycleState.RUNNING);
      case TASK_COMPLETED, TASK_SKIPPED ->
          new Mapping(ExecutionEvent.EventType.TASK_COMPLETED, ExecutionLifecycleState.RUNNING);
      case TIMER_SCHEDULED ->
          new Mapping(ExecutionEvent.EventType.TIMER_SCHEDULED, ExecutionLifecycleState.WAITING);
      case TIMER_FIRED ->
          new Mapping(ExecutionEvent.EventType.TIMER_FIRED, ExecutionLifecycleState.RUNNING);
      case TIMER_CANCELLED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.RUNNING);
      case ERROR_RAISED ->
          new Mapping(ExecutionEvent.EventType.ERROR_RAISED, ExecutionLifecycleState.RUNNING);
      case ERROR_CAUGHT, RETRY_STARTED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.RUNNING);
      case RETRY_SCHEDULED ->
          new Mapping(ExecutionEvent.EventType.RETRY_SCHEDULED, ExecutionLifecycleState.WAITING);
      case RETRY_EXHAUSTED ->
          new Mapping(ExecutionEvent.EventType.ERROR_RAISED, ExecutionLifecycleState.RUNNING);
      case EVENT_EMITTED,
          EVENT_RECEIVED,
          ASYNC_API_MESSAGE_RECEIVED,
          OPERATION_PROGRESS,
          OPERATION_RESULT_BUFFERED,
          OPERATION_FAILURE_BUFFERED,
          HUMAN_TASK_OUTCOME_BUFFERED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.RUNNING);
      // A correlated-worker call is a real wait, not active computation - same semantic WAITING
      // already carries correctly for timer/retry waits (see TIMER_SCHEDULED/RETRY_SCHEDULED
      // above). Without this, an execution blocked on an external worker for hours reports
      // RUNNING the whole time, indistinguishable from genuine computation through the public
      // contract - see docs/engine-construct-gap-audit.md gap #4. Routed through the same
      // BlockingConstructs.isBlocking(CORRELATED_WORKER) check openworkflow-pekko-engine consults,
      // instead of a second independently-authored WAITING literal - see gap #4's Phase 4 note.
      // HUMAN_TASK_* deliberately left unchanged: that construct is still unreachable in
      // production (see gap #2), so its mapping is dead code either way.
      case CORRELATED_WORKER_PROGRESS ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, correlatedWorkerLifecycleState());
      case SUBSCRIPTION_CREATED,
          ASYNC_API_SUBSCRIPTION_CREATED,
          OPERATION_DISPATCHED,
          HUMAN_TASK_CREATED,
          FORK_BRANCH_STARTED,
          ITERATION_STARTED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_REQUESTED, ExecutionLifecycleState.RUNNING);
      case CORRELATED_WORKER_STARTED, CORRELATED_WORKER_COMMAND_PUBLISHED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_REQUESTED, correlatedWorkerLifecycleState());
      case SUBSCRIPTION_COMPLETED,
          SUBSCRIPTION_CANCELLED,
          ASYNC_API_SUBSCRIPTION_COMPLETED,
          ASYNC_API_SUBSCRIPTION_CANCELLED,
          CORRELATED_WORKER_COMPLETED,
          CORRELATED_WORKER_CANCELLATION_REQUESTED,
          CORRELATED_WORKER_CANCELLED,
          OPERATION_COMPLETED,
          OPERATION_CANCELLATION_REQUESTED,
          OPERATION_CANCELLED,
          HUMAN_TASK_APPROVED,
          HUMAN_TASK_RESOLVED,
          HUMAN_TASK_REJECTED,
          HUMAN_TASK_REWORK_REQUESTED,
          HUMAN_TASK_EXPIRED,
          HUMAN_TASK_CANCELLATION_REQUESTED,
          HUMAN_TASK_CANCELLED,
          FORK_BRANCH_COMPLETED,
          FORK_BRANCH_ABANDONED,
          ITERATION_COMPLETED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.RUNNING);
      case CORRELATED_WORKER_ACCEPTED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, correlatedWorkerLifecycleState());
      case ASYNC_API_MESSAGE_FILTERED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.WAITING);
      case CORRELATED_WORKER_FAILED, CORRELATED_WORKER_OUTCOME_UNKNOWN, OPERATION_OUTCOME_UNKNOWN ->
          new Mapping(ExecutionEvent.EventType.ERROR_RAISED, ExecutionLifecycleState.RUNNING);
      case EXECUTION_PAUSED ->
          new Mapping(ExecutionEvent.EventType.PAUSED, ExecutionLifecycleState.PAUSED);
      case EXECUTION_RESUMED ->
          new Mapping(ExecutionEvent.EventType.RESUMED, ExecutionLifecycleState.RUNNING);
      case EXECUTION_CANCEL_REQUESTED ->
          new Mapping(
              ExecutionEvent.EventType.CANCELLATION_REQUESTED, ExecutionLifecycleState.CANCELLING);
      case EXECUTION_CANCELLED ->
          new Mapping(ExecutionEvent.EventType.CANCELLED, ExecutionLifecycleState.CANCELLED);
      case EXECUTION_COMPLETED ->
          new Mapping(ExecutionEvent.EventType.COMPLETED, ExecutionLifecycleState.COMPLETED);
      case EXECUTION_FAILED ->
          new Mapping(ExecutionEvent.EventType.FAILED, ExecutionLifecycleState.FAILED);
      case EXECUTION_PURGE_REQUESTED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_REQUESTED, ExecutionLifecycleState.RUNNING);
      case EXECUTION_PURGE_FAILED ->
          new Mapping(ExecutionEvent.EventType.ERROR_RAISED, ExecutionLifecycleState.RUNNING);
      case EXECUTION_PURGED ->
          new Mapping(ExecutionEvent.EventType.EFFECT_COMPLETED, ExecutionLifecycleState.COMPLETED);
    };
  }

  private static ExecutionLifecycleState correlatedWorkerLifecycleState() {
    return BlockingConstructs.isBlocking(CallPlan.Kind.CORRELATED_WORKER)
        ? ExecutionLifecycleState.WAITING
        : ExecutionLifecycleState.RUNNING;
  }

  private static void createTopics(String bootstrapServers, OksTopics topics) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    try (Admin admin = Admin.create(properties)) {
      List<NewTopic> desired =
          List.of(
                  topics.definitionCommands(),
                  topics.definitionHistory(),
                  topics.definitionCatalogue(),
                  topics.definitions(),
                  topics.commands(),
                  topics.history(),
                  topics.effects(),
                  topics.operationCheckpoints(),
                  topics.subscriptionEffects(),
                  topics.timerEffects(),
                  topics.subworkflowEffects(),
                  topics.inboundEvents(),
                  topics.emittedEvents(),
                  topics.deadLetters())
              .stream()
              .map(name -> new NewTopic(name, 6, (short) 1))
              .toList();
      for (NewTopic topic : desired) {
        try {
          admin.createTopics(List.of(topic)).all().get();
        } catch (java.util.concurrent.ExecutionException failure) {
          if (!(failure.getCause()
              instanceof org.apache.kafka.common.errors.TopicExistsException)) {
            throw new IllegalStateException(failure);
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
      }
    }
  }

  @Override
  public void close() {
    running.set(false);
    history.wakeup();
    if (projectionThread != null) {
      try {
        projectionThread.join(5000);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    history.close();
    streams.close(Duration.ofSeconds(10));
  }

  public record Mapping(ExecutionEvent.EventType type, ExecutionLifecycleState state) {}
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
