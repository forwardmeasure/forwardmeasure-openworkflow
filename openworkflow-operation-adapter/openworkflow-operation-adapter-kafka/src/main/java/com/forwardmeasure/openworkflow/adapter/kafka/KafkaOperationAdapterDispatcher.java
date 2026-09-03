package com.forwardmeasure.openworkflow.adapter.kafka;

import com.forwardmeasure.openworkflow.adapter.api.OperationAdapter;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedSecret;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * At-least-once Kafka dispatcher for operation adapters.
 *
 * <p>The dispatcher consumes the committed workflow outbox outside Kafka Streams, performs protocol
 * I/O asynchronously, and publishes correlated durable commands. Effect offsets are committed only
 * after the terminal observation has been acknowledged by Kafka. A crash can therefore repeat a
 * remote call; the stable operation ID and deterministic observation command IDs are the required
 * idempotency boundary.
 */
public final class KafkaOperationAdapterDispatcher implements AutoCloseable {
  public static final String RUNTIME_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/runtime";
  private static final Duration DEFINITION_AVAILABILITY_TIMEOUT = Duration.ofSeconds(30);

  private final Properties baseProperties;
  private final String effectsTopic;
  private final String definitionsTopic;
  private final String commandsTopic;
  private final String checkpointsTopic;
  private final String deadLetterTopic;
  private final String effectsGroupId;
  private final String instanceId;
  private final List<OperationAdapter> adapters;
  private final OperationSecurityResolver security;
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private final int maxInFlightOperations;
  private final DefinitionResourceCache resources = new DefinitionResourceCache();
  private final BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
  private final CountDownLatch definitionsReady = new CountDownLatch(1);
  private final CountDownLatch checkpointsReady = new CountDownLatch(1);
  private final Map<String, AdapterCheckpoint> checkpoints =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final ExecutorService completionExecutor = Executors.newVirtualThreadPerTaskExecutor();

  private volatile KafkaConsumer<String, byte[]> definitionConsumer;
  private volatile KafkaConsumer<String, byte[]> checkpointConsumer;
  private volatile KafkaConsumer<String, byte[]> effectConsumer;
  private volatile KafkaProducer<String, byte[]> producer;
  private volatile Thread definitionThread;
  private volatile Thread checkpointThread;
  private volatile Thread effectThread;

  public KafkaOperationAdapterDispatcher(
      Properties kafkaProperties,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String effectsGroupId,
      String instanceId,
      List<OperationAdapter> adapters,
      ActorId runtimeActorId,
      String runtimeComponent) {
    this(
        kafkaProperties,
        effectsTopic,
        definitionsTopic,
        commandsTopic,
        effectsTopic + ".dead-letter",
        effectsGroupId,
        instanceId,
        adapters,
        OperationSecurityResolver.rejecting(),
        runtimeActorId,
        runtimeComponent);
  }

  public KafkaOperationAdapterDispatcher(
      Properties kafkaProperties,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String effectsGroupId,
      String instanceId,
      List<OperationAdapter> adapters,
      OperationSecurityResolver security,
      ActorId runtimeActorId,
      String runtimeComponent) {
    this(
        kafkaProperties,
        effectsTopic,
        definitionsTopic,
        commandsTopic,
        effectsTopic + ".dead-letter",
        effectsGroupId,
        instanceId,
        adapters,
        security,
        runtimeActorId,
        runtimeComponent);
  }

  public KafkaOperationAdapterDispatcher(
      Properties kafkaProperties,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String deadLetterTopic,
      String effectsGroupId,
      String instanceId,
      List<OperationAdapter> adapters,
      OperationSecurityResolver security,
      ActorId runtimeActorId,
      String runtimeComponent) {
    this(
        kafkaProperties,
        effectsTopic,
        definitionsTopic,
        commandsTopic,
        deadLetterTopic,
        effectsGroupId,
        instanceId,
        adapters,
        security,
        runtimeActorId,
        runtimeComponent,
        64);
  }

  public KafkaOperationAdapterDispatcher(
      Properties kafkaProperties,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String deadLetterTopic,
      String effectsGroupId,
      String instanceId,
      List<OperationAdapter> adapters,
      OperationSecurityResolver security,
      ActorId runtimeActorId,
      String runtimeComponent,
      int maxInFlightOperations) {
    this.baseProperties = new Properties();
    this.baseProperties.putAll(Objects.requireNonNull(kafkaProperties, "kafkaProperties"));
    this.effectsTopic = requireText(effectsTopic, "effectsTopic");
    this.definitionsTopic = requireText(definitionsTopic, "definitionsTopic");
    this.commandsTopic = requireText(commandsTopic, "commandsTopic");
    this.checkpointsTopic = checkpointTopic(effectsTopic);
    this.deadLetterTopic = requireText(deadLetterTopic, "deadLetterTopic");
    this.effectsGroupId = requireText(effectsGroupId, "effectsGroupId");
    this.instanceId = requireText(instanceId, "instanceId");
    this.adapters = List.copyOf(Objects.requireNonNull(adapters, "adapters"));
    if (this.adapters.isEmpty()) {
      throw new IllegalArgumentException("At least one operation adapter is required");
    }
    this.security = Objects.requireNonNull(security, "security");
    this.runtimeActorId = Objects.requireNonNull(runtimeActorId, "runtimeActorId");
    this.runtimeComponent = requireText(runtimeComponent, "runtimeComponent");
    if (maxInFlightOperations < 1) {
      throw new IllegalArgumentException("maxInFlightOperations must be at least one");
    }
    this.maxInFlightOperations = maxInFlightOperations;
  }

  public synchronized void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Operation adapter dispatcher is already running");
    }
    producer = new KafkaProducer<>(producerProperties());
    definitionThread =
        Thread.ofVirtual().name("oks-definition-cache-" + instanceId).start(this::definitionLoop);
    checkpointThread =
        Thread.ofVirtual()
            .name("oks-operation-checkpoints-" + instanceId)
            .start(this::checkpointLoop);
    effectThread =
        Thread.ofVirtual().name("oks-operation-effects-" + instanceId).start(this::effectLoop);
  }

  public boolean running() {
    return running.get() && failure.get() == null;
  }

  public Throwable failure() {
    return failure.get();
  }

  private void definitionLoop() {
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(definitionConsumerProperties())) {
      definitionConsumer = consumer;
      consumer.subscribe(List.of(definitionsTopic));
      while (running.get()) {
        consumer
            .poll(Duration.ofMillis(250))
            .forEach(
                record -> {
                  if (record.value() == null) {
                    resources.remove(record.key());
                  } else {
                    resources.put(AdapterJson.read(record.value(), WorkflowDefinitionBundle.class));
                  }
                });
        if (definitionsReady.getCount() > 0 && !consumer.assignment().isEmpty()) {
          Map<TopicPartition, Long> ends = consumer.endOffsets(consumer.assignment());
          boolean caughtUp = true;
          for (TopicPartition partition : consumer.assignment()) {
            if (consumer.position(partition) < ends.get(partition)) {
              caughtUp = false;
              break;
            }
          }
          if (caughtUp) {
            definitionsReady.countDown();
          }
        }
      }
    } catch (WakeupException expected) {
      if (running.get()) fail(expected);
    } catch (Throwable unexpected) {
      fail(unexpected);
    } finally {
      definitionConsumer = null;
    }
  }

  private void checkpointLoop() {
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(checkpointConsumerProperties())) {
      checkpointConsumer = consumer;
      consumer.subscribe(List.of(checkpointsTopic));
      while (running.get()) {
        consumer
            .poll(Duration.ofMillis(250))
            .forEach(
                record -> {
                  if (record.value() == null) {
                    checkpoints.remove(record.key());
                  } else {
                    AdapterCheckpoint checkpoint =
                        AdapterJson.read(record.value(), AdapterCheckpoint.class);
                    if (!record.key().equals(checkpoint.key())) {
                      throw new IllegalStateException(
                          "Operation checkpoint Kafka key " + "does not match its " + "payload");
                    }
                    checkpoints.put(record.key(), checkpoint);
                  }
                });
        if (checkpointsReady.getCount() > 0 && !consumer.assignment().isEmpty()) {
          Map<TopicPartition, Long> ends = consumer.endOffsets(consumer.assignment());
          boolean caughtUp = true;
          for (TopicPartition partition : consumer.assignment()) {
            if (consumer.position(partition) < ends.get(partition)) {
              caughtUp = false;
              break;
            }
          }
          if (caughtUp) checkpointsReady.countDown();
        }
      }
    } catch (WakeupException expected) {
      if (running.get()) fail(expected);
    } catch (Throwable unexpected) {
      fail(unexpected);
    } finally {
      checkpointConsumer = null;
    }
  }

  private void effectLoop() {
    Map<TopicPartition, NavigableMap<Long, Boolean>> pending = new HashMap<>();
    Map<String, CompletableFuture<ActiveOperation>> inFlight =
        new java.util.concurrent.ConcurrentHashMap<>();
    ArrayDeque<ConsumerRecord<String, byte[]>> buffered = new ArrayDeque<>();
    try {
      while (running.get()
          && (!definitionsReady.await(250, TimeUnit.MILLISECONDS)
              || !checkpointsReady.await(250, TimeUnit.MILLISECONDS))) {
        // Keep checking host lifecycle while the compacted projection
        // and operation checkpoints catch up from their earliest
        // available offsets.
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      fail(interrupted);
      return;
    }
    if (!running.get()) return;
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(effectConsumerProperties())) {
      effectConsumer = consumer;
      consumer.subscribe(List.of(effectsTopic));
      while (running.get()) {
        var records = consumer.poll(Duration.ofMillis(100));
        records.forEach(buffered::addLast);
        buffered.removeIf(
            record ->
                !consumer
                    .assignment()
                    .contains(new TopicPartition(record.topic(), record.partition())));
        while (!buffered.isEmpty() && inFlight.size() < maxInFlightOperations) {
          ConsumerRecord<String, byte[]> record = buffered.removeFirst();
          TopicPartition partition = new TopicPartition(record.topic(), record.partition());
          pending
              .computeIfAbsent(partition, ignored -> new TreeMap<>())
              .put(record.offset(), false);
          try {
            handle(record, partition, inFlight);
          } catch (RuntimeException poison) {
            publishDeadLetter(record, poison);
            completions.add(Completion.done(partition, record.offset(), null));
          }
        }
        drainCompletions(pending, inFlight);
        commitCompleted(consumer, pending);
        pending.keySet().retainAll(consumer.assignment());
        if (buffered.isEmpty() && inFlight.size() < maxInFlightOperations) {
          consumer.resume(consumer.assignment());
        } else {
          consumer.pause(consumer.assignment());
        }
      }
    } catch (WakeupException expected) {
      if (running.get()) fail(expected);
    } catch (Throwable unexpected) {
      fail(unexpected);
    } finally {
      effectConsumer = null;
    }
  }

  private void handle(
      ConsumerRecord<String, byte[]> record,
      TopicPartition partition,
      Map<String, CompletableFuture<ActiveOperation>> inFlight) {
    if (record.value() == null) {
      completions.add(Completion.done(partition, record.offset(), null));
      return;
    }
    WorkflowEffect effect = AdapterJson.read(record.value(), WorkflowEffect.class);
    if (effect.type() != WorkflowEffectType.DISPATCH_OPERATION
        && effect.type() != WorkflowEffectType.CANCEL_OPERATION) {
      completions.add(Completion.done(partition, record.offset(), null));
      return;
    }
    OperationRequest unresolved = OperationRequest.from(effect);
    final OperationAdapter adapter;
    try {
      adapter = select(unresolved);
    } catch (RuntimeException unsupported) {
      if (effect.type() == WorkflowEffectType.DISPATCH_OPERATION) {
        completions.add(
            Completion.terminal(
                partition, record.offset(), effect, failed(unresolved, unsupported)));
      } else {
        completions.add(Completion.done(partition, record.offset(), null));
      }
      return;
    }
    final OperationRequest request;
    try {
      if (!resources.awaitDefinition(
              unresolved.definitionReference(), DEFINITION_AVAILABILITY_TIMEOUT, running::get)
          && !running.get()) {
        return;
      }
      resources.validateDefinition(
          unresolved.definitionReference(),
          effect.key().tenantId(),
          unresolved.descriptor().required("definitionSha256").textValue());
      request = unresolved.resolveResource(resources);
    } catch (RuntimeException unavailable) {
      completions.add(
          Completion.terminal(partition, record.offset(), effect, failed(unresolved, unavailable)));
      return;
    }

    if (effect.type() == WorkflowEffectType.CANCEL_OPERATION) {
      cancel(request, adapter, partition, record.offset(), inFlight);
      return;
    }

    CompletableFuture<ActiveOperation> active = new CompletableFuture<>();
    CompletableFuture<ActiveOperation> existing =
        inFlight.putIfAbsent(request.operationId(), active);
    if (existing != null) {
      existing.whenCompleteAsync(
          (operation, startupFailure) -> {
            if (startupFailure != null) {
              completions.add(
                  Completion.terminal(
                      partition, record.offset(), effect, failed(request, startupFailure)));
              return;
            }
            operation
                .terminal()
                .whenComplete(
                    (observation, failure) ->
                        completions.add(
                            Completion.terminal(
                                partition,
                                record.offset(),
                                effect,
                                failure == null ? observation : failed(request, failure))));
          },
          completionExecutor);
      return;
    }

    security
        .secure(request)
        .whenCompleteAsync(
            (secured, securityFailure) -> {
              if (securityFailure != null) {
                active.completeExceptionally(securityFailure);
                inFlight.remove(request.operationId(), active);
                completions.add(
                    Completion.terminal(
                        partition, record.offset(), effect, failed(request, securityFailure)));
                return;
              }
              start(
                  effect, request, adapter, secured, active, partition, record.offset(), inFlight);
            },
            completionExecutor);
  }

  private void start(
      WorkflowEffect effect,
      OperationRequest unresolved,
      OperationAdapter adapter,
      SecuredOperationRequest secured,
      CompletableFuture<ActiveOperation> activeFuture,
      TopicPartition partition,
      long offset,
      Map<String, CompletableFuture<ActiveOperation>> inFlight) {
    OperationRequest request = secured.request();
    CompletableFuture<OperationObservation> terminal = new CompletableFuture<>();
    ActiveOperation active = new ActiveOperation(adapter, secured, terminal);
    activeFuture.complete(active);
    try {
      AdapterCheckpoint checkpoint = checkpoint(effect, request);
      CompletionStage<OperationObservation> execution =
          checkpoint == null
              ? adapter.execute(request, progress -> publishProgress(effect, request, progress))
              : adapter.recover(
                  request,
                  checkpoint.observation(),
                  progress -> publishProgress(effect, request, progress));
      execution.whenCompleteAsync(
          (observation, adapterFailure) -> {
            OperationObservation result =
                adapterFailure == null
                    ? terminalObservation(request, observation)
                    : failed(request, adapterFailure);
            result = redact(request, result);
            terminal.complete(result);
            secured.close();
            inFlight.remove(unresolved.operationId(), activeFuture);
            completions.add(Completion.terminal(partition, offset, effect, result));
          },
          completionExecutor);
    } catch (RuntimeException adapterFailure) {
      OperationObservation result = redact(request, failed(request, adapterFailure));
      terminal.complete(result);
      secured.close();
      inFlight.remove(unresolved.operationId(), activeFuture);
      completions.add(Completion.terminal(partition, offset, effect, result));
    }
  }

  private void cancel(
      OperationRequest request,
      OperationAdapter selected,
      TopicPartition partition,
      long offset,
      Map<String, CompletableFuture<ActiveOperation>> inFlight) {
    CompletableFuture<ActiveOperation> active = inFlight.get(request.operationId());
    if (active != null) {
      active
          .thenCompose(operation -> operation.adapter().cancel(operation.secured().request()))
          .whenComplete(
              (ignored, failure) -> completions.add(Completion.done(partition, offset, failure)));
      return;
    }
    AdapterCheckpoint checkpoint = checkpoint(request);
    security
        .secure(request)
        .whenCompleteAsync(
            (secured, securityFailure) -> {
              if (securityFailure != null) {
                completions.add(Completion.done(partition, offset, securityFailure));
                return;
              }
              try {
                (checkpoint == null
                        ? selected.cancel(secured.request())
                        : selected.cancel(secured.request(), checkpoint.observation()))
                    .whenComplete(
                        (ignored, failure) -> {
                          secured.close();
                          completions.add(Completion.done(partition, offset, failure));
                        });
              } catch (RuntimeException failure) {
                secured.close();
                completions.add(Completion.done(partition, offset, failure));
              }
            },
            completionExecutor);
  }

  private static OperationObservation terminalObservation(
      OperationRequest request, OperationObservation observation) {
    if (observation == null) {
      return failed(
          request, new IllegalStateException("Operation adapter returned no observation"));
    }
    if (observation.status() == OperationObservationStatus.PROGRESS) {
      return failed(
          request, new IllegalStateException("Operation adapter completed with a progress signal"));
    }
    return observation;
  }

  private static OperationObservation redact(
      OperationRequest request, OperationObservation observation) {
    if (observation == null) return null;
    OperationObservation redacted = observation;
    ResolvedAuthentication authentication = request.authentication();
    if (authentication != null) {
      redacted = redact(authentication, redacted);
    }
    for (ResolvedSecret secret : request.secrets().values()) {
      redacted = redact(secret, redacted);
    }
    return redacted;
  }

  private static OperationObservation redact(
      ResolvedAuthentication authentication, OperationObservation observation) {
    WorkflowError error = observation.error();
    WorkflowError redactedError =
        error == null
            ? null
            : new WorkflowError(
                error.type(),
                error.status(),
                authentication.redact(error.instance()),
                authentication.redact(error.title()),
                authentication.redact(error.detail()));
    return new OperationObservation(
        observation.status(),
        redact(authentication, observation.output()),
        redactedError,
        redact(authentication, observation.metadata()));
  }

  private static OperationObservation redact(
      ResolvedSecret secret, OperationObservation observation) {
    WorkflowError error = observation.error();
    WorkflowError redactedError =
        error == null
            ? null
            : new WorkflowError(
                error.type(),
                error.status(),
                secret.redact(error.instance()),
                secret.redact(error.title()),
                secret.redact(error.detail()));
    return new OperationObservation(
        observation.status(),
        redact(secret, observation.output()),
        redactedError,
        redact(secret, observation.metadata()));
  }

  private static DataReference redact(
      ResolvedAuthentication authentication, DataReference reference) {
    if (reference == null || reference.storage() != DataReference.Storage.INLINE) {
      return reference;
    }
    return DataReferences.inline(authentication.redact(reference.inlineValue()));
  }

  private static DataReference redact(ResolvedSecret secret, DataReference reference) {
    if (reference == null || reference.storage() != DataReference.Storage.INLINE) {
      return reference;
    }
    return DataReferences.inline(secret.redact(reference.inlineValue()));
  }

  private OperationAdapter select(OperationRequest request) {
    List<OperationAdapter> matches =
        adapters.stream().filter(adapter -> adapter.supports(request)).toList();
    if (matches.size() != 1) {
      throw new IllegalStateException(
          "Expected exactly one adapter for "
              + request.operationId()
              + " but found "
              + matches.size());
    }
    return matches.getFirst();
  }

  private void drainCompletions(
      Map<TopicPartition, NavigableMap<Long, Boolean>> pending,
      Map<String, CompletableFuture<ActiveOperation>> inFlight) {
    List<Completion> drained = new ArrayList<>();
    completions.drainTo(drained);
    for (Completion completion : drained) {
      if (completion.failure() != null) {
        publishDeadLetter(completion.partition(), completion.offset(), completion.failure());
      }
      if (completion.observation() != null) {
        publishObservation(completion.effect(), completion.observation());
        deleteCheckpoint(completion.effect());
      }
      NavigableMap<Long, Boolean> partition = pending.get(completion.partition());
      if (partition != null && partition.containsKey(completion.offset())) {
        partition.put(completion.offset(), true);
      }
    }
  }

  private void commitCompleted(
      KafkaConsumer<String, byte[]> consumer,
      Map<TopicPartition, NavigableMap<Long, Boolean>> pending) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Set<TopicPartition> assignment = consumer.assignment();
    pending.forEach(
        (partition, records) -> {
          if (!assignment.contains(partition)) {
            return;
          }
          long nextOffset = -1;
          while (!records.isEmpty() && Boolean.TRUE.equals(records.firstEntry().getValue())) {
            nextOffset = records.pollFirstEntry().getKey() + 1;
          }
          if (nextOffset >= 0) {
            offsets.put(partition, new OffsetAndMetadata(nextOffset));
          }
        });
    if (!offsets.isEmpty()) {
      consumer.commitSync(offsets);
    }
  }

  private void publishObservation(WorkflowEffect effect, OperationObservation observation) {
    Objects.requireNonNull(observation, "observation");
    ExecutionKey key = effect.key();
    ActorContext actor =
        new ActorContext(
            key.tenantId(),
            runtimeActorId,
            ActorType.SYSTEM,
            runtimeComponent,
            runtimeComponent,
            effect.actor().correlationId(),
            Set.of(),
            null,
            effect.requestedAt());
    String operationId = operationId(effect);
    String commandId = operationId + ":observation:" + sha256(AdapterJson.write(observation));
    ExecutionCommand command =
        new ObserveOperationCommand(
            commandId, key, operationId, observation, actor, effect.requestedAt());
    try {
      producer
          .send(new ProducerRecord<>(commandsTopic, key.canonical(), AdapterJson.write(command)))
          .get(30, TimeUnit.SECONDS);
    } catch (Exception failure) {
      throw new CompletionException("Unable to publish operation observation", failure);
    }
  }

  private void publishProgress(
      WorkflowEffect effect, OperationRequest request, OperationObservation progress) {
    OperationObservation redacted = redact(request, progress);
    if (redacted == null || redacted.status() != OperationObservationStatus.PROGRESS) {
      throw new IllegalArgumentException(
          "Operation progress sink accepts only progress " + "observations");
    }
    AdapterCheckpoint checkpoint =
        new AdapterCheckpoint(
            checkpointKey(effect),
            request.operationId(),
            request.definitionReference(),
            request.descriptor().required("taskPath").textValue(),
            redacted,
            Instant.now());
    try {
      producer
          .send(
              new ProducerRecord<>(
                  checkpointsTopic, checkpoint.key(), AdapterJson.write(checkpoint)))
          .get(30, TimeUnit.SECONDS);
      checkpoints.put(checkpoint.key(), checkpoint);
    } catch (Exception failure) {
      throw new CompletionException("Unable to publish operation checkpoint", failure);
    }
    publishObservation(effect, redacted);
  }

  private void deleteCheckpoint(WorkflowEffect effect) {
    String key = checkpointKey(effect);
    try {
      producer.send(new ProducerRecord<>(checkpointsTopic, key, null)).get(30, TimeUnit.SECONDS);
      checkpoints.remove(key);
    } catch (Exception failure) {
      throw new CompletionException("Unable to delete operation checkpoint", failure);
    }
  }

  private void publishDeadLetter(ConsumerRecord<String, byte[]> record, Throwable failure) {
    publishDeadLetter(
        new TopicPartition(record.topic(), record.partition()),
        record.offset(),
        record.key(),
        record.value(),
        failure);
  }

  private void publishDeadLetter(TopicPartition partition, long offset, Throwable failure) {
    publishDeadLetter(partition, offset, null, null, failure);
  }

  private void publishDeadLetter(
      TopicPartition partition, long offset, String key, byte[] value, Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null
        && (root instanceof CompletionException
            || root instanceof java.util.concurrent.ExecutionException)) {
      root = root.getCause();
    }
    var deadLetter = AdapterJson.MAPPER.createObjectNode();
    deadLetter.put("sourceTopic", partition.topic());
    deadLetter.put("sourcePartition", partition.partition());
    deadLetter.put("sourceOffset", offset);
    deadLetter.put("sourceKey", key);
    if (value != null) {
      deadLetter.put("sourceValueBase64", Base64.getEncoder().encodeToString(value));
    }
    deadLetter.put("failureType", root.getClass().getName());
    deadLetter.put("failureMessage", root.getMessage() == null ? "" : root.getMessage());
    deadLetter.put("failedAt", Instant.now().toString());
    try {
      producer
          .send(
              new ProducerRecord<>(
                  deadLetterTopic,
                  partition.topic() + ":" + partition.partition() + ":" + offset,
                  AdapterJson.write(deadLetter)))
          .get(30, TimeUnit.SECONDS);
    } catch (Exception publishFailure) {
      throw new CompletionException("Unable to publish operation dead letter", publishFailure);
    }
  }

  private static String operationId(WorkflowEffect effect) {
    return effect.payload().inlineValue().required("operationId").textValue();
  }

  private static OperationObservation failed(OperationRequest request, Throwable failure) {
    Throwable root = failure;
    while (root instanceof CompletionException && root.getCause() != null) {
      root = root.getCause();
    }
    String detail = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
    return new OperationObservation(
        OperationObservationStatus.FAILED,
        null,
        new WorkflowError(
            RUNTIME_ERROR, 500, request.operationId(), "Operation adapter failed", detail),
        null);
  }

  private Properties producerProperties() {
    Properties properties = copyBase();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "oks-operation-adapter-" + instanceId);
    return properties;
  }

  private Properties definitionConsumerProperties() {
    Properties properties = consumerProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, effectsGroupId + "-definitions-" + instanceId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  private Properties checkpointConsumerProperties() {
    Properties properties = consumerProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, effectsGroupId + "-checkpoints-" + instanceId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  private Properties effectConsumerProperties() {
    Properties properties = consumerProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, effectsGroupId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxInFlightOperations);
    return properties;
  }

  private Properties consumerProperties() {
    Properties properties = copyBase();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return properties;
  }

  private Properties copyBase() {
    Properties copy = new Properties();
    copy.putAll(baseProperties);
    return copy;
  }

  private void fail(Throwable unexpected) {
    failure.compareAndSet(null, unexpected);
    running.set(false);
    wakeup();
  }

  private void wakeup() {
    KafkaConsumer<String, byte[]> definitions = definitionConsumer;
    if (definitions != null) definitions.wakeup();
    KafkaConsumer<String, byte[]> checkpoint = checkpointConsumer;
    if (checkpoint != null) checkpoint.wakeup();
    KafkaConsumer<String, byte[]> effects = effectConsumer;
    if (effects != null) effects.wakeup();
  }

  @Override
  public synchronized void close() {
    if (!running.getAndSet(false)
        && definitionThread == null
        && checkpointThread == null
        && effectThread == null) {
      return;
    }
    wakeup();
    resources.signal();
    join(definitionThread);
    join(checkpointThread);
    join(effectThread);
    definitionThread = null;
    checkpointThread = null;
    effectThread = null;
    completionExecutor.shutdownNow();
    KafkaProducer<String, byte[]> currentProducer = producer;
    producer = null;
    if (currentProducer != null) {
      currentProducer.close(Duration.ofSeconds(10));
    }
  }

  private static void join(Thread thread) {
    if (thread == null) return;
    try {
      thread.join(Duration.ofSeconds(10));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String checkpointTopic(String effectsTopic) {
    String suffix = ".effects";
    return effectsTopic.endsWith(suffix)
        ? effectsTopic.substring(0, effectsTopic.length() - suffix.length())
            + ".operation-checkpoints"
        : effectsTopic + ".operation-checkpoints";
  }

  private static String checkpointKey(WorkflowEffect effect) {
    return effect.key().canonical() + "::" + operationId(effect);
  }

  private static String checkpointKey(OperationRequest request) {
    return request.descriptor().required("executionKey").textValue() + "::" + request.operationId();
  }

  private AdapterCheckpoint checkpoint(WorkflowEffect effect, OperationRequest request) {
    AdapterCheckpoint checkpoint = checkpoints.get(checkpointKey(effect));
    validateCheckpoint(request, checkpoint);
    return checkpoint;
  }

  private AdapterCheckpoint checkpoint(OperationRequest request) {
    AdapterCheckpoint checkpoint = checkpoints.get(checkpointKey(request));
    validateCheckpoint(request, checkpoint);
    return checkpoint;
  }

  private static void validateCheckpoint(OperationRequest request, AdapterCheckpoint checkpoint) {
    if (checkpoint == null) return;
    if (!request.operationId().equals(checkpoint.operationId())
        || !request.definitionReference().equals(checkpoint.definitionReference())
        || !request.descriptor().required("taskPath").textValue().equals(checkpoint.taskPath())) {
      throw new SecurityException(
          "Operation checkpoint does not match its admitted " + "operation identity");
    }
  }

  private record AdapterCheckpoint(
      String key,
      String operationId,
      String definitionReference,
      String taskPath,
      OperationObservation observation,
      Instant checkpointedAt) {
    private AdapterCheckpoint {
      requireText(key, "checkpoint key");
      requireText(operationId, "checkpoint operationId");
      requireText(definitionReference, "checkpoint definitionReference");
      requireText(taskPath, "checkpoint taskPath");
      Objects.requireNonNull(observation, "checkpoint observation");
      Objects.requireNonNull(checkpointedAt, "checkpoint checkpointedAt");
      if (observation.status() != OperationObservationStatus.PROGRESS) {
        throw new IllegalArgumentException("Only progress observations can be checkpointed");
      }
    }
  }

  private record Completion(
      TopicPartition partition,
      long offset,
      WorkflowEffect effect,
      OperationObservation observation,
      Throwable failure) {
    static Completion done(TopicPartition partition, long offset, Throwable failure) {
      return new Completion(partition, offset, null, null, failure);
    }

    static Completion terminal(
        TopicPartition partition,
        long offset,
        WorkflowEffect effect,
        OperationObservation observation) {
      return new Completion(
          partition,
          offset,
          Objects.requireNonNull(effect, "effect"),
          Objects.requireNonNull(observation, "observation"),
          null);
    }
  }

  private record ActiveOperation(
      OperationAdapter adapter,
      SecuredOperationRequest secured,
      CompletableFuture<OperationObservation> terminal) {
    private ActiveOperation {
      Objects.requireNonNull(adapter, "adapter");
      Objects.requireNonNull(secured, "secured");
      Objects.requireNonNull(terminal, "terminal");
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
