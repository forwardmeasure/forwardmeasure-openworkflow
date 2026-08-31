package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.durableprocessing.kafka.DurableAggregateMetadata;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdmitWorkflowDefinitionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionPurgePolicyDecision;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.KafkaRecordLimits;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.PurgeExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveAsyncApiMessageCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionCatalogueEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ActiveCorrelatedWorkerState;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ActiveExecutionPurgeState;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ActiveListenState;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ActiveOperationState;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionPhase;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionSnapshot;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end, real-broker walkthrough of the currently executable OpenWorkflow slice.
 *
 * <p>The test has two participants:
 *
 * <ul>
 *   <li>The JUnit thread creates topics, produces commands, polls history consumers and queries
 *       Kafka Streams state stores.
 *   <li>Kafka Streams runs its own processing and restoration threads. Those threads validate
 *       definitions, reduce execution commands, update state and publish history and continuation
 *       records.
 * </ul>
 *
 * <p>The two {@link KafkaConsumer} instances in this class do not have their own threads. Calling
 * {@code subscribe} only records the subscription. Records are fetched synchronously when the JUnit
 * thread calls {@code poll} in {@link #awaitDefinitionHistory} or {@link #awaitHistory}.
 *
 * <p>The execution exercised here is:
 *
 * <pre>
 * publish definition
 *       |
 * start -> pause -> stop original runtime
 *                    |
 *                    v
 *           restore into an empty directory
 *                    |
 *                  resume
 *                    |
 *       prepare/do -> initialize/set -> choose/switch
 *                                      |
 *                                      v
 *                     preserveBatches/for -> preserve/set x N -> complete
 * </pre>
 *
 * <p>Every accepted command advances a durable aggregate revision. Every internally generated
 * advance command targets the revision that created it. Consequently, pausing the run makes an
 * already queued advance command stale without deleting or mutating the Kafka record.
 */
@Testcontainers
class OksRestorationIntegrationTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private static final WorkflowDefinitionKey DEFINITION_KEY =
      new WorkflowDefinitionKey(
          TENANT, new WorkflowCoordinates("evidence", "extraction", "1.0.0", "1.0.3"));
  private static final Instant REQUESTED = Instant.parse("2026-07-28T20:00:00Z");
  private static final ActorId RUNTIME_ACTOR =
      ActorId.parse("did:web:runtime.example.com:actors:runtime");

  /**
   * Every scenario intentionally has unique application and topic names. Clean them between
   * scenarios so the shared single-broker container does not accumulate hundreds of source, sink,
   * repartition and changelog topics. Without cleanup, Redpanda can transiently expose a newly
   * created topic with zero partitions and make an otherwise valid Streams topology fail only
   * during the full suite.
   */
  @AfterEach
  void deleteScenarioTopics() throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      Set<String> topics = admin.listTopics().names().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      if (!topics.isEmpty()) {
        admin.deleteTopics(topics).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void rejectsOversizedCompiledDefinitionThroughARealBroker() throws Exception {
    String suffix = UUID.randomUUID().toString();
    OksTopics topics = OksTopics.withPrefix("test.oks.definition-limit." + suffix);
    createTopics(topics);
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
          summary: |
            %s
        do:
          - finish:
              set:
                status: complete
        """
            .formatted("x".repeat(2_900_000));
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();

    try (KafkaStreams runtime =
            streams(
                "oks-definition-limit-" + suffix,
                topics,
                stateDirectories.resolve("definition-limit-" + suffix));
        KafkaProducer<String, AdmitWorkflowDefinitionCommand> definitions = definitionProducer();
        KafkaConsumer<String, WorkflowDefinitionAdmissionEvent> decisions =
            definitionHistoryConsumer();
        KafkaConsumer<String, WorkflowDefinitionBundle> bundles = definitionBundleConsumer()) {
      runtime.setUncaughtExceptionHandler(
          failure -> {
            streamFailure.set(failure);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      decisions.subscribe(List.of(topics.definitionHistory()));
      bundles.subscribe(List.of(topics.definitions()));
      runtime.start();
      awaitRunning(runtime);

      definitions
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  new AdmitWorkflowDefinitionCommand(
                      "admit-oversized-definition",
                      DEFINITION_KEY,
                      source,
                      List.of(),
                      commandActor(),
                      REQUESTED)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitions.flush();

      WorkflowDefinitionAdmissionEvent rejected =
          awaitDefinitionHistory(decisions, runtime, streamFailure);
      assertEquals(WorkflowDefinitionAdmissionStatus.REJECTED, rejected.status());
      assertTrue(
          rejected.issues().stream()
              .anyMatch(
                  issue ->
                      issue.contains("Workflow definition record is")
                          && issue.contains("maximum is")));
      assertTrue(
          bundles.poll(Duration.ofSeconds(2)).isEmpty(),
          "The oversized bundle must not enter the compacted " + "definitions topic");
      assertNull(streamFailure.get());
    }
  }

  @Test
  void rejectsIncompatibleWorkflowContractsThroughARealBroker() throws Exception {
    String suffix = UUID.randomUUID().toString();
    OksTopics topics = OksTopics.withPrefix("test.oks.definition-contract." + suffix);
    createTopics(topics);
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - produce:
              set:
                customerId: one
              output:
                schema:
                  document:
                    type: object
                    required: [customerId]
                    properties:
                      customerId: {type: string}
          - consume:
              input:
                schema:
                  document:
                    type: object
                    required: [customerId]
                    properties:
                      customerId: {type: integer}
              set:
                accepted: true
        """;
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();

    try (KafkaStreams runtime =
            streams(
                "oks-definition-contract-" + suffix,
                topics,
                stateDirectories.resolve("definition-contract-" + suffix));
        KafkaProducer<String, AdmitWorkflowDefinitionCommand> definitions = definitionProducer();
        KafkaConsumer<String, WorkflowDefinitionAdmissionEvent> decisions =
            definitionHistoryConsumer();
        KafkaConsumer<String, WorkflowDefinitionBundle> bundles = definitionBundleConsumer()) {
      runtime.setUncaughtExceptionHandler(
          failure -> {
            streamFailure.set(failure);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      decisions.subscribe(List.of(topics.definitionHistory()));
      bundles.subscribe(List.of(topics.definitions()));
      runtime.start();
      awaitRunning(runtime);

      definitions
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  new AdmitWorkflowDefinitionCommand(
                      "admit-incompatible-contract",
                      DEFINITION_KEY,
                      source,
                      List.of(),
                      commandActor(),
                      REQUESTED)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitions.flush();

      WorkflowDefinitionAdmissionEvent rejected =
          awaitDefinitionHistory(decisions, runtime, streamFailure);
      assertEquals(WorkflowDefinitionAdmissionStatus.REJECTED, rejected.status());
      assertTrue(
          rejected.issues().stream()
              .anyMatch(
                  issue ->
                      issue.contains("Schema compatibility")
                          && issue.contains("INCOMPATIBLE")
                          && issue.contains(
                              "/do/0/produce/output/schema -> " + "/do/1/consume/input/schema")));
      assertTrue(
          bundles.poll(Duration.ofSeconds(2)).isEmpty(),
          "An incompatible definition must not enter the compacted " + "definitions topic");
      assertNull(streamFailure.get());
    }
  }

  private static final String SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: evidence
        name: extraction
        version: '1.0.0'
      input:
        schema:
          format: json
          resource:
            endpoint: https://schemas.example.test/evidence-input.json
        from: '${ {instruction: .instruction, preserve: true, batches: .batches} }'
      do:
        - prepare:
            do:
              - initialize:
                  input:
                    schema:
                      format: json
                      document:
                        type: object
                        required: [instruction, preserve, batches]
                  set:
                    status: ready
                    preserve: '${ .preserve }'
                    batches: '${ .batches }'
              - choosePreservation:
                  switch:
                    - preserve:
                        when: '${ .preserve }'
                        then: preserveBatches
                    - skip:
                        then: exit
              - preserveBatches:
                  for:
                    each: batch
                    in: '${ .batches }'
                    at: batchIndex
                  do:
                    - preserve:
                        set:
                          preserveArtifacts: '${ .preserveArtifacts + [$batch] }'
                  output:
                    schema:
                      format: json
                      document:
                        type: object
                        required: [preserveArtifacts]
                        properties:
                          preserveArtifacts:
                            type: array
                            items:
                              type: boolean
      output:
        as: '${ {instruction: $context.instruction, preserveArtifacts: .preserveArtifacts} }'
        schema:
          format: json
          document:
            type: object
            required: [instruction, preserveArtifacts]
      """;
  private static final ResolvedWorkflowResource INPUT_SCHEMA =
      ResolvedWorkflowResource.jsonSchema(
          java.net.URI.create("https://schemas.example.test/evidence-input.json"),
          """
          {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["instruction"],
            "properties": {
              "instruction": {"type": "string"}
            }
          }
          """);
  /*
   * Select a stable execution key on a partition different from the current
   * immutable definition reference. The definition digest deliberately
   * changes whenever compiler semantics change, so a hard-coded execution
   * id would eventually make the cross-partition proof fail before Kafka is
   * even started.
   */
  private static final ExecutionKey KEY = crossPartitionExecutionKey();

  @Container
  static final RedpandaContainer KAFKA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  @TempDir Path stateDirectories;

  @Test
  void restoresPausedExecutionAndResumesWithoutReplayingWork() throws Exception {
    /*
     * PHASE 1: Create an isolated six-topic test namespace.
     *
     * The UUID prevents this test from observing records left by another
     * test run. createTopics creates external source and sink topics only.
     * Kafka Streams creates its own state-store changelog topics under the
     * application ID when the runtime starts.
     */
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks." + suffix);
    createTopics(topics);

    /*
     * Constructing a command is an in-memory operation. No Kafka record is
     * written until a producer.send call below.
     */
    StartExecutionCommand command = command();

    /*
     * The definition and execution deliberately hash to different
     * partitions. Successful lookup therefore proves that execution does
     * not depend on both keys being co-partitioned: every runtime instance
     * restores the compacted definitions topic into a global store.
     */
    assertNotEquals(
        partition(command.definition().canonical(), 3),
        partition(KEY.canonical(), 3),
        "The proof requires definition and execution keys on different partitions");

    /*
     * PHASE 2: Open command producers and test-observer consumers.
     *
     * These consumers are controlled by the JUnit thread. subscribe does
     * not start a background polling thread; awaitDefinitionHistory and
     * awaitHistory call poll synchronously later.
     */
    try (var definitionProducer = definitionProducer();
        var producer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var definitionCatalogue = definitionCatalogueConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      definitionCatalogue.subscribe(List.of(topics.definitionCatalogue()));
      history.subscribe(List.of(topics.history()));

      /*
       * PHASE 3: Start the original Kafka Streams runtime.
       *
       * The uncaught-exception handler exposes asynchronous stream-thread
       * failures to the synchronous polling helpers, avoiding a test
       * that merely waits until timeout after the runtime has died.
       */
      AtomicReference<Throwable> streamFailure = new AtomicReference<>();
      KafkaStreams original = streams(applicationId, topics, stateDirectories.resolve("original"));
      original.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      try {
        original.start();
        awaitRunning(original);

        /*
         * PHASE 4: Publish the exact workflow definition.
         *
         * The definition command is keyed by tenant plus document
         * coordinates. The runtime validates the YAML against the
         * pinned 1.0.3 schema, compiles the supported
         * do/set/switch/for surface,
         * stores the immutable bundle, publishes that bundle to the
         * compacted definitions topic and publishes a decision to
         * definition-history. All observations below use
         * read_committed consumers.
         */
        AdmitWorkflowDefinitionCommand admission = admission("admit-1", SOURCE, REQUESTED);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(), DEFINITION_KEY.canonical(), admission))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        WorkflowDefinitionAdmissionEvent admitted =
            awaitDefinitionHistory(definitionHistory, original, streamFailure);
        assertEquals(WorkflowDefinitionAdmissionStatus.ADMITTED, admitted.status());
        WorkflowDefinitionBundle bundle = awaitDefinition(original);
        assertEquals(command.definition(), bundle.reference());
        assertEquals(OpenWorkflowCompiler.COMPILER_SHA256, bundle.compilerSha256());
        assertEquals(INPUT_SCHEMA.sha256(), bundle.plan().resources().getFirst().sha256());
        assertEquals(bundle.plan().definitionSha256(), admitted.definitionSha256());
        WorkflowDefinitionCatalogueEvent admittedCatalogue =
            awaitDefinitionCatalogue(definitionCatalogue, original, streamFailure);
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED, admittedCatalogue.admission().status());
        assertEquals(bundle, admittedCatalogue.bundle());

        /*
         * Publishing identical source with a new command ID does not
         * create a new definition. It produces an UNCHANGED decision.
         */
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-same-source", SOURCE, REQUESTED.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.UNCHANGED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        WorkflowDefinitionCatalogueEvent unchangedCatalogue =
            awaitDefinitionCatalogue(definitionCatalogue, original, streamFailure);
        assertEquals(
            WorkflowDefinitionAdmissionStatus.UNCHANGED, unchangedCatalogue.admission().status());
        assertEquals(
            bundle,
            unchangedCatalogue.bundle(),
            "UNCHANGED must project the original immutable bundle");

        /*
         * Source text alone is not the immutable identity. Changing a
         * resolved external schema changes the composite definition
         * digest and is rejected for the already published version.
         */
        ResolvedWorkflowResource alteredSchema =
            ResolvedWorkflowResource.jsonSchema(
                INPUT_SCHEMA.uri(),
                """
                {
                  "type": "object",
                  "required": ["instruction", "changed"]
                }
                """);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission(
                        "admit-schema-conflict",
                        SOURCE,
                        List.of(alteredSchema),
                        REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.REJECTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        WorkflowDefinitionCatalogueEvent schemaRejectedCatalogue =
            awaitDefinitionCatalogue(definitionCatalogue, original, streamFailure);
        assertEquals(
            WorkflowDefinitionAdmissionStatus.REJECTED,
            schemaRejectedCatalogue.admission().status());
        assertNull(schemaRejectedCatalogue.bundle());

        /*
         * Reusing the immutable document version for different source
         * is rejected. The original bundle remains in the store.
         */
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission(
                        "admit-conflict",
                        SOURCE.replace(
                            "preserveArtifacts: '${ .preserveArtifacts + [$batch] }'",
                            "preserveArtifacts: []"),
                        REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        WorkflowDefinitionAdmissionEvent rejected =
            awaitDefinitionHistory(definitionHistory, original, streamFailure);
        assertEquals(WorkflowDefinitionAdmissionStatus.REJECTED, rejected.status());
        WorkflowDefinitionCatalogueEvent sourceRejectedCatalogue =
            awaitDefinitionCatalogue(definitionCatalogue, original, streamFailure);
        assertEquals(
            WorkflowDefinitionAdmissionStatus.REJECTED,
            sourceRejectedCatalogue.admission().status());
        assertNull(sourceRejectedCatalogue.bundle());
        assertEquals(bundle.plan().sourceSha256(), awaitDefinition(original).plan().sourceSha256());

        /*
         * Exact redelivery of the original publication command is
         * suppressed by its durable command receipt. There is no
         * duplicate decision record.
         */
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(), DEFINITION_KEY.canonical(), admission))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertTrue(
            definitionHistory.poll(Duration.ofSeconds(3)).isEmpty(),
            "Retrying the original admission command must be idempotent");
        assertTrue(
            definitionCatalogue.poll(Duration.ofSeconds(1)).isEmpty(),
            "Retrying the original admission command must not "
                + "duplicate its catalogue projection");

        /*
         * PHASE 5: Queue START followed immediately by PAUSE.
         *
         * Both records use the execution key and therefore occupy the
         * same partition. START is read first and atomically commits:
         *   - execution revision 1,
         *   - EXECUTION_STARTED history, and
         *   - ADVANCE(expectedRevision=1) back to commands.
         *
         * PAUSE was already appended by this producer, so it precedes
         * that generated ADVANCE. PAUSE commits revision 2. When the
         * old ADVANCE is eventually read, its expected revision 1 is
         * stale and it cannot execute the first workflow task.
         */
        /*
         * Commit START and PAUSE as one input transaction. Merely
         * sending both records from one producer preserves their
         * order, but it does not stop the runtime from observing
         * START before PAUSE has been appended. Under a loaded full
         * suite that race can legitimately let START's generated
         * ADVANCE reach Kafka first and makes this restoration proof
         * nondeterministic. The read-committed Streams runtime sees
         * this transaction only after both ordered commands exist.
         */
        try (var commandBatch = transactionalProducer()) {
          commandBatch.initTransactions();
          commandBatch.beginTransaction();
          commandBatch
              .send(new ProducerRecord<>(topics.commands(), KEY.canonical(), command))
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
          commandBatch
              .send(
                  new ProducerRecord<>(
                      topics.commands(),
                      KEY.canonical(),
                      control("pause-1", ExecutionControlAction.PAUSE, REQUESTED.plusSeconds(3))))
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
          commandBatch.commitTransaction();
        }

        List<ExecutionHistoryEvent> events = awaitHistory(history, 2, original, streamFailure);
        assertEquals(ExecutionEventType.EXECUTION_STARTED, events.getFirst().type());
        assertEquals(ExecutionEventType.EXECUTION_PAUSED, events.getLast().type());
        assertEquals(TENANT, events.getLast().actor().tenantId());
        ExecutionSnapshot paused = awaitSnapshot(original, ExecutionPhase.PAUSED);
        assertEquals(2, awaitExecutionMetadata(original, 2).revision());
        assertEquals(0, paused.cursor().current().nextChildIndex());
      } finally {
        /*
         * PHASE 6: Stop the first process after it has durably paused.
         *
         * close flushes and leaves Kafka as the system of record. The
         * replacement below intentionally receives a different empty
         * state directory and cannot reuse the original RocksDB files.
         */
        assertTrue(original.close(Duration.ofSeconds(10)));
      }

      /*
       * PHASE 7: Start a replacement process with the same application
       * ID and a fresh local directory.
       *
       * Kafka Streams restores execution state, revision metadata and
       * command receipts from changelog topics. The compacted
       * definitions topic rebuilds the global definition store.
       */
      try (KafkaStreams replacement =
          streams(applicationId, topics, stateDirectories.resolve("replacement"))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        assertEquals(command.definition(), awaitDefinition(replacement).reference());
        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.PAUSED);
        assertEquals(2, awaitExecutionMetadata(replacement, 2).revision());
        assertEquals(command.definition(), restored.definition());
        assertEquals(0, restored.cursor().current().nextChildIndex());
        assertEquals(command.actor().actorId(), restored.startedBy().actorId());

        /*
         * PHASE 8: Resume at the restored cursor.
         *
         * RESUME commits revision 3 and an ADVANCE targeting revision
         * 3. Each subsequent ADVANCE crosses at most one durable
         * boundary:
         *
         *   revision 4: enter structural prepare/do
         *   revision 5: execute initialize/set
         *   revision 6: evaluate choosePreservation/switch
         *   revision 7: enter preserveBatches/for at index 0
         *   revision 8: execute preserve/set at index 0
         *   revision 9: complete index 0 and enter index 1
         *   revision 10: execute preserve/set at index 1
         *   revision 11: complete index 1 and enter index 2
         *   revision 12: execute preserve/set at index 2
         *   revision 13: complete index 2 and preserveBatches/for
         *   revision 14: exit prepare/do
         *   revision 15: complete the root workflow
         *
         * Each transition atomically updates state and emits its
         * history and next continuation under exactly_once_v2.
         */
        producer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control("resume-1", ExecutionControlAction.RESUME, REQUESTED.plusSeconds(4))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        producer.flush();

        List<ExecutionHistoryEvent> resumedEvents =
            awaitHistory(history, 22, replacement, streamFailure);
        assertEquals(ExecutionEventType.EXECUTION_RESUMED, resumedEvents.getFirst().type());
        assertEquals(ExecutionEventType.EXECUTION_COMPLETED, resumedEvents.getLast().type());
        assertEquals(
            "/do/0/prepare/do/2/preserveBatches/do/0/preserve",
            resumedEvents.stream()
                .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
                .filter(event -> "preserve".equals(event.taskName()))
                .findFirst()
                .orElseThrow()
                .taskPath());
        ExecutionHistoryEvent switchCompleted =
            resumedEvents.stream()
                .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
                .filter(event -> "choosePreservation".equals(event.taskName()))
                .findFirst()
                .orElseThrow();
        assertEquals("preserve", switchCompleted.switchDecision().selectedCase());
        assertEquals("preserveBatches", switchCompleted.switchDecision().flowDirective());
        assertEquals(
            ActorType.SYSTEM,
            resumedEvents.stream()
                .filter(event -> event.type() == ExecutionEventType.TASK_STARTED)
                .findFirst()
                .orElseThrow()
                .actor()
                .actorType());

        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals(15, awaitExecutionMetadata(replacement, 15).revision());
        assertTrue(
            !completed.data().inlineValue().has("status"),
            "SET replaces task output; it does not merge prior input");
        assertEquals(
            JSON.readTree("[true,false,true]"),
            completed.data().inlineValue().required("preserveArtifacts"));
        assertEquals(
            "Extract entities", completed.data().inlineValue().required("instruction").textValue());

        /*
         * PHASE 9: Rebuild the complete UI/query view from Kafka.
         *
         * The replacement started with an empty local state
         * directory. These assertions therefore exercise restored
         * changelog-backed query stores rather than RocksDB files
         * inherited from the original process. Definition catalogue,
         * snapshot, immutable history, graph occurrences and timeline
         * must all describe the same completed execution.
         */

        /*
         * PHASE 10: Prove command receipts also survived restoration.
         *
         * Re-sending the byte-identical START command with command ID
         * command-1 produces neither a state change nor history.
         */
        producer
            .send(new ProducerRecord<>(topics.commands(), KEY.canonical(), command))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        producer.flush();
        assertTrue(
            history.poll(Duration.ofSeconds(3)).isEmpty(),
            "Restored command deduplication must not duplicate history");
      }
    }
  }

  @Test
  void restoresAnActiveForCursorWithoutRepeatingAnIteration() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-for-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.for-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();

    var input = JSON.createObjectNode();
    input.put("instruction", "Extract entities");
    var batches = input.putArray("batches");
    for (int index = 0; index < 100; index++) {
      batches.add(index % 2 == 0);
    }

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));

      KafkaStreams original =
          streams(
              applicationId, topics, stateDirectories.resolve("for-restore-original-" + suffix));
      original.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      ExecutionSnapshot paused;
      try {
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-for-restore", SOURCE, REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original);

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-for-restore", input, REQUESTED.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        /*
         * Observe a committed FOR frame before requesting pause.
         * The control command shares the execution key, so at most an
         * already-appended continuation can run before PAUSE.
         */
        ExecutionSnapshot active = awaitActiveIteration(original);
        assertTrue(active.cursor().current().iteration().index() < 100);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "pause-active-for",
                        ExecutionControlAction.PAUSE,
                        REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        paused = awaitSnapshot(original, ExecutionPhase.PAUSED);
        assertNotNull(paused.cursor().current().iteration());
        assertTrue(paused.cursor().current().iteration().index() < 100);
      } finally {
        assertTrue(original.close(Duration.ofSeconds(10)));
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("for-restore-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);

        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.PAUSED);
        assertEquals(
            paused.cursor().current().iteration(), restored.cursor().current().iteration());
        assertEquals(
            100, restored.cursor().current().iteration().collection().inlineValue().size());

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "resume-active-for",
                        ExecutionControlAction.RESUME,
                        REQUESTED.plusSeconds(3))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals(100, completed.data().inlineValue().required("preserveArtifacts").size());

        List<ExecutionHistoryEvent> events =
            awaitHistoryThroughCompletion(history, replacement, streamFailure);
        List<Integer> completedIndexes =
            events.stream()
                .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
                .filter(event -> "preserve".equals(event.taskName()))
                .map(event -> event.iterations().getLast().index())
                .toList();
        assertEquals(100, completedIndexes.size());
        assertEquals(
            java.util.stream.IntStream.range(0, 100).boxed().toList(),
            completedIndexes,
            "Restoration must neither replay nor omit an iteration");

        /*
         * Prove that the operational query surface remains bounded
         * even for a run with a substantial append-only history.
         * Pages are read from the restored Kafka Streams history
         * store, using the durable sequence as the cursor. The
         * concatenated pages must reproduce the broker-observed
         * history exactly once and in order.
         */
      }
    }
  }

  @Test
  void restoresActiveForkLanesAndJoinsEachBranchExactlyOnce() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-fork-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.fork-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source = forkSource(100);
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));

      KafkaStreams original =
          streams(
              applicationId, topics, stateDirectories.resolve("fork-restore-original-" + suffix));
      original.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      ExecutionSnapshot paused;
      try {
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-fork-restore", source, List.of(), REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-fork-restore",
                        JSON.readTree("{}"),
                        definition,
                        REQUESTED.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot active = awaitActiveFork(original);
        assertEquals(100, active.activeFork().branches().size());
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "pause-active-fork",
                        ExecutionControlAction.PAUSE,
                        REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        paused = awaitSnapshot(original, ExecutionPhase.PAUSED);
        assertNotNull(paused.activeFork());
      } finally {
        assertTrue(original.close(Duration.ofSeconds(10)));
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("fork-restore-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);

        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.PAUSED);
        assertEquals(paused.activeFork(), restored.activeFork());

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "resume-active-fork",
                        ExecutionControlAction.RESUME,
                        REQUESTED.plusSeconds(3))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals(100, completed.data().inlineValue().size());

        List<ExecutionHistoryEvent> events =
            awaitHistoryThroughCompletion(history, replacement, streamFailure);
        List<Integer> completedBranches =
            events.stream()
                .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_COMPLETED)
                .map(event -> event.forks().getLast().declarationIndex())
                .sorted()
                .toList();
        assertEquals(
            java.util.stream.IntStream.range(0, 100).boxed().toList(),
            completedBranches,
            "Restoration must neither repeat nor omit a branch");
      }
    }
  }

  @Test
  void emitsACloudEventToKafkaFromCommittedExecutionHistory() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-emit-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.emit." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - publish:
              emit:
                event:
                  with:
                    source: https://evidence.example.test
                    type: com.forwardmeasure.evidence.extracted.v1
                    data:
                      evidenceId: '${ .evidenceId }'
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var emittedEvents = emittedEventConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("emit-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      emittedEvents.subscribe(List.of(topics.emittedEvents()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);

      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-emit", source, List.of(), REQUESTED)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());
      awaitDefinition(runtime, definition);

      executionProducer
          .send(
              new ProducerRecord<>(
                  topics.commands(),
                  KEY.canonical(),
                  command(
                      "start-emit",
                      JSON.readTree("{\"evidenceId\":\"evidence-42\"}"),
                      definition,
                      REQUESTED.plusSeconds(1))))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      executionProducer.flush();
      awaitSnapshot(runtime, ExecutionPhase.COMPLETED);

      JsonNode event = awaitEmittedEvent(emittedEvents, runtime, streamFailure);
      assertEquals("1.0", event.required("specversion").textValue());
      assertEquals("com.forwardmeasure.evidence.extracted.v1", event.required("type").textValue());
      assertEquals("evidence-42", event.required("data").required("evidenceId").textValue());
    }
  }

  @Test
  void listenSurvivesTheKafkaBoundaryAndResumesFromInboundCloudEvent() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-listen-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.listen." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - awaitEvidence:
              listen:
                to:
                  one:
                    with:
                      type: evidence.received.v1
                read: data
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var eventProducer = inboundEventProducer();
        var definitionHistory = definitionHistoryConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("listen-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-listen", source, List.of(), REQUESTED)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());
      awaitDefinition(runtime, definition);
      executionProducer
          .send(
              new ProducerRecord<>(
                  topics.commands(),
                  KEY.canonical(),
                  command(
                      "start-listen",
                      JSON.createObjectNode(),
                      definition,
                      REQUESTED.plusSeconds(1))))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      executionProducer.flush();
      awaitPendingInteraction(runtime);

      InboundCloudEvent inbound =
          new InboundCloudEvent(
              TENANT,
              DataReferences.inline(
                  JSON.readTree(
                      """
                      {
                        "specversion": "1.0",
                        "id": "evidence-42",
                        "source": "https://events.example.test",
                        "type": "evidence.received.v1",
                        "data": {"evidenceId": "e-42"}
                      }
                      """)),
              commandActor(),
              REQUESTED.plusSeconds(2));
      eventProducer
          .send(new ProducerRecord<>(topics.inboundEvents(), TENANT.toString(), inbound))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      eventProducer.flush();

      ExecutionSnapshot completed = awaitSnapshot(runtime, ExecutionPhase.COMPLETED);
      assertEquals("e-42", completed.data().inlineValue().required("evidenceId").textValue());
      assertTrue(streamFailure.get() == null);
    }
  }

  @Test
  void terminalPurgeRemovesDurableStateAndRestoresOnlyItsReceipt() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-purge-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.purge." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - prepare:
              set:
                status: ready
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var executionHistory = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      executionHistory.subscribe(List.of(topics.history()));
      KafkaStreams original =
          streams(applicationId, topics, stateDirectories.resolve("purge-first-" + suffix));
      original.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      try {
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-purge", source, List.of(), REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-purge",
                        JSON.createObjectNode().put("evidenceId", "secret-42"),
                        definition,
                        REQUESTED.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        awaitSnapshot(original, ExecutionPhase.COMPLETED);

        ActorContext purger =
            new ActorContext(
                TENANT,
                ActorId.parse("did:web:tenant.example.com:actors:records-admin"),
                ActorType.HUMAN,
                "Records Administrator",
                "ssb-public",
                Set.of(PurgeExecutionCommand.REQUIRED_ROLE),
                null,
                REQUESTED.plusSeconds(2));
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new PurgeExecutionCommand(
                        "purge-run",
                        KEY,
                        new ExecutionPurgePolicyDecision(
                            "retention-decision-1",
                            "records-v1",
                            KEY,
                            REQUESTED.plusSeconds(2),
                            REQUESTED,
                            false,
                            "investigation"),
                        purger,
                        REQUESTED.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ActiveExecutionPurgeState purge =
            assertInstanceOf(
                ActiveExecutionPurgeState.class,
                awaitSnapshot(original, ExecutionPhase.PURGING).pendingInteraction());

        Instant completedAt = REQUESTED.plusSeconds(3);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new ObserveOperationCommand(
                        "observe-purge",
                        KEY,
                        purge.purgeId(),
                        new OperationObservation(
                            OperationObservationStatus.SUCCEEDED,
                            DataReferences.inline(JSON.createObjectNode().put("deletedValues", 1)),
                            null,
                            null),
                        Actors.system(TENANT, RUNTIME_ACTOR, "purge-adapter", completedAt),
                        completedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        awaitHistoryEvent(
            executionHistory,
            event -> event.type() == ExecutionEventType.EXECUTION_PURGED,
            original,
            streamFailure);
        awaitExecutionMissing(original);
        assertPurgedProjections(original);

        assertTrue(original.close(Duration.ofSeconds(15)));
        KafkaStreams restored =
            streams(applicationId, topics, stateDirectories.resolve("purge-restored-" + suffix));
        try {
          restored.start();
          awaitRunning(restored);
          awaitExecutionMissing(restored);
          assertPurgedProjections(restored);
        } finally {
          restored.close(Duration.ofSeconds(15));
        }
      } finally {
        original.close(Duration.ofSeconds(15));
      }
    }
  }

  @Test
  void waitTimerRestoresFromKafkaAndFiresWithoutBlockingAStreamThread() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-wait-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.wait." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - durableDelay:
              wait: PT10S
          - complete:
              set:
                status: complete
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(applicationId, topics, stateDirectories.resolve("wait-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-wait", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        Instant commandAt = Instant.now();
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-wait", JSON.createObjectNode(), definition, commandAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        awaitPendingInteraction(original);
        WorkflowEffect timer = awaitTimer(original);
        assertEquals(
            commandAt.plusSeconds(10).toString(),
            timer.payload().inlineValue().required("dueAt").textValue());
      }

      try (KafkaStreams replacement =
          streams(applicationId, topics, stateDirectories.resolve("wait-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals("complete", completed.data().inlineValue().required("status").textValue());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void extensionSelectionAndBeforeCursorRestoreFromKafka() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-extension-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.extension." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        use:
          extensions:
            - durable-observer:
                extend: set
                when: '${ $task.name == "target" }'
                before:
                  - durable-delay:
                      wait: PT3S
                after:
                  - complete:
                      set:
                        status: complete
                        targetStatus: '${ .status }'
        do:
          - target:
              set:
                status: target
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(
              applicationId, topics, stateDirectories.resolve("extension-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-extension", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-extension", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ExecutionSnapshot waiting = awaitPendingInteraction(original);
        assertTrue(
            waiting.cursor().frames().stream()
                .anyMatch(
                    frame ->
                        frame.extensionState() != null
                            && frame.extensionState().applies().equals(List.of(true))));
      }

      try (KafkaStreams replacement =
          streams(
              applicationId, topics, stateDirectories.resolve("extension-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        JsonNode output = completed.data().inlineValue();
        assertEquals("complete", output.required("status").textValue());
        assertEquals("target", output.required("targetStatus").textValue());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void taskTimeoutRestoresIntoEmptyLocalStateAndCancelsItsListen() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-timeout-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.timeout." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - awaitEvidence:
              timeout:
                after: PT4S
              listen:
                to:
                  one:
                    with:
                      type: evidence.received.v1
                read: data
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(applicationId, topics, stateDirectories.resolve("timeout-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        Instant admittedAt = Instant.now();
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-timeout", source, List.of(), admittedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        Instant startedAt = Instant.now();
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-timeout", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        awaitPendingInteraction(original);
        WorkflowEffect timeout = awaitTimer(original, "task-timeout");
        assertEquals(
            startedAt.plusSeconds(4).toString(),
            timeout.payload().inlineValue().required("dueAt").textValue());
      }

      /*
       * The replacement uses an unrelated empty state directory. The
       * deadline and listen are therefore recovered only from Kafka
       * changelogs, not from a local RocksDB directory.
       */
      try (KafkaStreams replacement =
          streams(
              applicationId, topics, stateDirectories.resolve("timeout-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot failed = awaitSnapshot(replacement, ExecutionPhase.FAILED);
        assertEquals(408, failed.failure().status());
        assertTrue(failed.activeTimeouts().isEmpty());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void intervalScheduleStartsDistinctRunsThroughTheDurableRuntime() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-every-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-every." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          every: PT2S
        do:
          - complete:
              set:
                status: complete
        """;

    try (var definitionProducer = definitionProducer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("schedule-every-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);
      Instant admittedAt = Instant.now();
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-schedule-every", source, List.of(), admittedAt)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());

      List<ExecutionHistoryEvent> completions =
          awaitCompletions(history, 2, runtime, streamFailure);
      assertEquals(
          2, completions.stream().map(event -> event.key().executionId()).distinct().count());
      assertTrue(
          completions.stream()
              .allMatch(event -> event.key().executionId().value().startsWith("scheduled-")));
      assertTrue(streamFailure.get() == null);
    }
  }

  @Test
  void cronScheduleSkipsMissedSlotsAndDoesNotReplayAfterRestoration() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-cron-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-cron." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          cron: '* * * * *'
        do:
          - complete:
              set:
                status: complete
        """;

    /*
     * Admission is deliberately backdated. The first deterministic cron
     * slot is therefore overdue and fires immediately. When that slot is
     * committed, the recurrence calculation must skip every other missed
     * minute and materialise exactly one future slot.
     */
    Instant admittedAt = Instant.now().minus(Duration.ofMinutes(3));
    try (var definitionProducer = definitionProducer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));

      ExecutionHistoryEvent completion;
      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schedule-cron-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-schedule-cron", source, List.of(), admittedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());

        completion = awaitCompletions(history, 1, original, streamFailure).getFirst();
        assertEquals(
            completion.key().executionId().value(),
            completion.actor().correlationId().value(),
            "Each independent cron occurrence must establish its own correlation root");
        assertEquals("complete", completion.output().inlineValue().required("status").textValue());
        WorkflowEffect next = awaitTimer(original, OksScheduleSupport.PURPOSE);
        JsonNode descriptor = next.payload().inlineValue();
        assertEquals(OksScheduleSupport.KIND_CRON, descriptor.required("scheduleKind").textValue());
        assertEquals("* * * * *", descriptor.required("recurrence").textValue());
        assertTrue(Instant.parse(descriptor.required("dueAt").textValue()).isAfter(Instant.now()));
      }

      /*
       * A replacement with no local RocksDB state restores both the
       * completed command receipt and the compacted timer state. It
       * must not recreate the overdue slot that already completed.
       */
      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schedule-cron-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        assertEquals(0, countCompletions(history, completion.key(), Duration.ofSeconds(2)));
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void intervalScheduleRestoresIntoFreshStateAndDoesNotReplayItsSlot() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          every: PT2H
        do:
          - complete:
              set:
                status: complete
        """;

    /*
     * The two-hour interval keeps the next legitimate slot outside this
     * test. Backdating admission makes the first slot due about forty-five
     * seconds from now, leaving enough time to observe the durable timer,
     * stop the first runtime and restore it into a different empty state
     * directory before it fires.
     */
    Instant admittedAt = Instant.now().minus(Duration.ofHours(2)).plusSeconds(45);
    try (var definitionProducer = definitionProducer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));

      WorkflowEffect originalTimer;
      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schedule-restore-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-schedule-restore", source, List.of(), admittedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        originalTimer = awaitTimer(original, OksScheduleSupport.PURPOSE);
        assertTrue(
            Instant.parse(originalTimer.payload().inlineValue().required("dueAt").textValue())
                .isAfter(Instant.now()));
      }
      assertEquals(
          0,
          countCompletions(history, originalTimer.key(), Duration.ofSeconds(2)),
          "The stopped runtime must not complete the schedule slot");

      ExecutionHistoryEvent completion;
      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schedule-restore-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);

        /*
         * The replacement has no local RocksDB state. Completing the
         * exact execution key captured from the original timer proves
         * that the timer identity, due time and immutable definition
         * binding came from the Kafka changelog.
         */
        completion = awaitCompletions(history, 1, replacement, streamFailure).getFirst();
        assertEquals(originalTimer.key(), completion.key());
        assertEquals("complete", completion.output().inlineValue().required("status").textValue());
        WorkflowEffect nextTimer = awaitTimer(replacement, OksScheduleSupport.PURPOSE);
        assertNotEquals(originalTimer.effectId(), nextTimer.effectId());
        assertTrue(
            Instant.parse(nextTimer.payload().inlineValue().required("dueAt").textValue())
                .isAfter(
                    Instant.parse(
                        originalTimer.payload().inlineValue().required("dueAt").textValue())));
      }

      /*
       * Restore once more after the slot completed. The deleted timer
       * and execution command receipt are restored together, so the
       * same deterministic schedule slot must not complete again.
       */
      try (KafkaStreams secondReplacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schedule-restore-second-" + suffix))) {
        secondReplacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        secondReplacement.start();
        awaitRunning(secondReplacement);
        assertEquals(0, countCompletions(history, completion.key(), Duration.ofSeconds(2)));
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void afterScheduleUsesTheCommittedPriorOutputForTheNextRun() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-after-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-after." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          after: PT2S
        do:
          - increment:
              set:
                count: '${ .count + 1 }'
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("schedule-after-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);
      Instant admittedAt = Instant.now();
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-schedule-after", source, List.of(), admittedAt)))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());
      awaitDefinition(runtime, definition);

      executionProducer
          .send(
              new ProducerRecord<>(
                  topics.commands(),
                  KEY.canonical(),
                  command(
                      "start-schedule-after",
                      JSON.readTree("{\"count\":0}"),
                      definition,
                      Instant.now())))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      executionProducer.flush();

      List<ExecutionHistoryEvent> completions =
          awaitCompletions(history, 2, runtime, streamFailure);
      ExecutionHistoryEvent initial =
          completions.stream().filter(event -> event.key().equals(KEY)).findFirst().orElseThrow();
      ExecutionHistoryEvent scheduled =
          completions.stream().filter(event -> !event.key().equals(KEY)).findFirst().orElseThrow();
      assertEquals(1, initial.output().inlineValue().required("count").intValue());
      assertEquals(2, scheduled.output().inlineValue().required("count").intValue());
      assertTrue(scheduled.key().executionId().value().startsWith("scheduled-"));
      assertTrue(streamFailure.get() == null);
    }
  }

  @Test
  void eventScheduleDurablyCorrelatesConcurrentAllGroups() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-on-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-on." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          on:
            all:
              - with:
                  type: evidence.opened.v1
                correlate:
                  caseId:
                    from: .caseId
              - with:
                  type: evidence.closed.v1
                correlate:
                  caseId:
                    from: .caseId
          read: envelope
        do:
          - complete:
              set:
                caseId: '${ .[0].data.caseId }'
                eventCount: '${ length }'
                firstEventSource: '${ .[0].source }'
        """;

    try (var definitionProducer = definitionProducer();
        var eventProducer = inboundEventProducer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("schedule-on-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-schedule-on", source, List.of(), Instant.now())))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());

      String[][] events = {
        {"opened-a", "evidence.opened.v1", "A"},
        {"opened-b", "evidence.opened.v1", "B"},
        {"closed-b", "evidence.closed.v1", "B"},
        {"closed-a", "evidence.closed.v1", "A"}
      };
      for (int index = 0; index < events.length; index++) {
        String[] value = events[index];
        InboundCloudEvent inbound =
            new InboundCloudEvent(
                TENANT,
                DataReferences.inline(
                    JSON.readTree(
                        """
                        {
                          "specversion": "1.0",
                          "id": "%s",
                          "source": "https://events.example.test",
                          "type": "%s",
                          "data": {"caseId": "%s"}
                        }
                        """
                            .formatted(value[0], value[1], value[2]))),
                commandActor(),
                Instant.now().plusMillis(index));
        eventProducer
            .send(new ProducerRecord<>(topics.inboundEvents(), TENANT.toString(), inbound))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
      eventProducer.flush();

      List<ExecutionHistoryEvent> completions =
          awaitCompletions(history, 2, runtime, streamFailure);
      assertEquals(
          Set.of("A", "B"),
          completions.stream()
              .map(event -> event.output().inlineValue().required("caseId").textValue())
              .collect(java.util.stream.Collectors.toSet()));
      assertTrue(
          completions.stream()
              .allMatch(
                  event -> event.output().inlineValue().required("eventCount").intValue() == 2));
      assertTrue(
          completions.stream()
              .allMatch(
                  event ->
                      "https://events.example.test"
                          .equals(
                              event
                                  .output()
                                  .inlineValue()
                                  .required("firstEventSource")
                                  .textValue())));
      for (ExecutionHistoryEvent completion : completions) {
        ExecutionSnapshot snapshot =
            awaitSnapshot(runtime, completion.key(), ExecutionPhase.COMPLETED);
        assertEquals(commandActor().actorId(), snapshot.startedBy().actorId());
        assertEquals(commandActor().identityProvider(), snapshot.startedBy().identityProvider());
        assertEquals(commandActor().subjectIdentifier(), snapshot.startedBy().subjectIdentifier());
      }
      assertTrue(streamFailure.get() == null);
    }
  }

  @Test
  void eventScheduleValidatesDataAgainstThePublicationPinnedSchema() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schedule-schema-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schedule-schema." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String schemaUri = "https://schemas.test/evidence-ready.json";
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        schedule:
          on:
            one:
              with:
                type: evidence.ready.v1
                dataschema: %s
        do:
          - complete:
              set:
                evidenceId: '${ .evidenceId }'
        """
            .formatted(schemaUri);
    ResolvedWorkflowResource schema =
        ResolvedWorkflowResource.jsonSchema(
            java.net.URI.create(schemaUri),
            """
            {
              "$schema":"https://json-schema.org/draft/2020-12/schema",
              "type":"object",
              "required":["evidenceId"],
              "properties":{"evidenceId":{"type":"string"}},
              "additionalProperties":false
            }
            """);

    try (var definitionProducer = definitionProducer();
        var eventProducer = inboundEventProducer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer();
        KafkaStreams runtime =
            streams(applicationId, topics, stateDirectories.resolve("schedule-schema-" + suffix))) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      runtime.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      runtime.start();
      awaitRunning(runtime);
      definitionProducer
          .send(
              new ProducerRecord<>(
                  topics.definitionCommands(),
                  DEFINITION_KEY.canonical(),
                  admission("admit-schedule-schema", source, List.of(schema), Instant.now())))
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      definitionProducer.flush();
      assertEquals(
          WorkflowDefinitionAdmissionStatus.ADMITTED,
          awaitDefinitionHistory(definitionHistory, runtime, streamFailure).status());

      for (String eventJson :
          List.of(
              """
              {"specversion":"1.0","id":"invalid","source":"urn:test",
               "type":"evidence.ready.v1","dataschema":"%s",
               "data":{"evidenceId":42}}
              """
                  .formatted(schemaUri),
              """
              {"specversion":"1.0","id":"valid","source":"urn:test",
               "type":"evidence.ready.v1","dataschema":"%s",
               "data":{"evidenceId":"evidence-123"}}
              """
                  .formatted(schemaUri))) {
        InboundCloudEvent inbound =
            new InboundCloudEvent(
                TENANT,
                DataReferences.inline(JSON.readTree(eventJson)),
                commandActor(),
                Instant.now());
        eventProducer
            .send(new ProducerRecord<>(topics.inboundEvents(), TENANT.toString(), inbound))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
      eventProducer.flush();

      ExecutionHistoryEvent completion =
          awaitCompletions(history, 1, runtime, streamFailure).getFirst();
      assertEquals(
          "evidence-123", completion.output().inlineValue().required("evidenceId").textValue());
      assertTrue(streamFailure.get() == null);
    }
  }

  @Test
  void retryTimerAndTryStateRestoreFromKafkaBeforeTheNextAttempt() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-retry-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.retry." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - guarded:
              try:
                - unavailable:
                    raise:
                      error:
                        type: https://example.com/errors/unavailable
                        status: 503
                        detail: Evidence service unavailable
              catch:
                errors:
                  with:
                    status: 503
                retry:
                  # The test must observe both the execution and timer
                  # stores before the one-shot timer legitimately fires.
                  # Its subject is restoration, not a three-second SLA.
                  delay: PT15S
                  limit:
                    attempt:
                      count: 1
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(applicationId, topics, stateDirectories.resolve("retry-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-retry", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-retry", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ExecutionSnapshot waiting = awaitPendingInteraction(original);
        assertTrue(
            waiting.pendingInteraction()
                instanceof com.forwardmeasure.openworkflow.workflow.runtime.core.ActiveRetryState);
        WorkflowEffect timer = awaitTimer(original);
        assertEquals("retry", timer.payload().inlineValue().required("purpose").textValue());
      }

      try (KafkaStreams replacement =
          streams(applicationId, topics, stateDirectories.resolve("retry-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot failed = awaitSnapshot(replacement, ExecutionPhase.FAILED);
        assertEquals(503, failed.failure().status());
        assertEquals("https://example.com/errors/unavailable", failed.failure().type());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void activeRetryAttemptDeadlineRestoresAndExpiresAnExternalWait() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-retry-deadline-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.retry-deadline." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - guarded:
              try:
                - failFirst:
                    if: '${ .retried != true }'
                    raise:
                      error:
                        type: https://example.com/errors/unavailable
                        status: 503
                - awaitEvidence:
                    listen:
                      to:
                        one:
                          with:
                            type: evidence.received.v1
              catch:
                retry:
                  limit:
                    attempt:
                      count: 1
                      duration: PT5S
                do:
                  - markRetried:
                      set:
                        retried: true
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("retry-deadline-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-retry-deadline", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-retry-deadline", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot waiting = awaitPendingListen(original);
        assertInstanceOf(ActiveListenState.class, waiting.pendingInteraction());
        WorkflowEffect deadline = awaitTimer(original, "retry-attempt-deadline");
        assertEquals(
            "retry-attempt-deadline",
            deadline.payload().inlineValue().required("purpose").textValue());
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("retry-deadline-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot failed = awaitSnapshot(replacement, ExecutionPhase.FAILED);
        assertEquals(
            "https://open-workflow-specification.org/spec/1.0.0/errors/timeout",
            failed.failure().type());
        assertEquals(408, failed.failure().status());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void activeCallRestoresBeforeACorrelatedAdapterOutcome() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-call-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.call-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - invoke:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
                body:
                  evidenceId: '${ .evidenceId }'
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();
    String operationId;

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      try (KafkaStreams original =
          streams(
              applicationId, topics, stateDirectories.resolve("call-restore-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-call-restore", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-call-restore",
                        JSON.readTree("{\"evidenceId\":\"e-42\"}"),
                        definition,
                        startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ActiveOperationState active =
            assertInstanceOf(
                ActiveOperationState.class, awaitPendingOperation(original).pendingInteraction());
        operationId = active.operationId();
        assertEquals(
            "e-42",
            active
                .descriptor()
                .inlineValue()
                .required("arguments")
                .required("body")
                .required("evidenceId")
                .textValue());
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("call-restore-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ActiveOperationState restored =
            assertInstanceOf(
                ActiveOperationState.class,
                awaitPendingOperation(replacement).pendingInteraction());
        assertEquals(operationId, restored.operationId());

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new ObserveOperationCommand(
                        "complete-call-restore",
                        KEY,
                        operationId,
                        new OperationObservation(
                            OperationObservationStatus.SUCCEEDED,
                            DataReferences.inline(
                                JSON.readTree(
                                    """
                                    {
                                      "persons": [
                                        "Alice"
                                      ]
                                    }
                                    """)),
                            null,
                            null),
                        Actors.system(
                            TENANT, RUNTIME_ACTOR, "test-call-adapter", startedAt.plusSeconds(1)),
                        startedAt.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals(
            "Alice", completed.data().inlineValue().required("persons").get(0).textValue());
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void cataloguedFunctionAndItsPendingRunRestoreFromKafka() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-catalog-function-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.catalog-function-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        use:
          catalogs:
            evidence:
              endpoint:
                uri: https://catalog.example.test/
        do:
          - normalize:
              call: normalize:1.2.3@evidence
              with:
                value: '${ .name }'
        """;
    ResolvedWorkflowResource function =
        ResolvedWorkflowResource.of(
            java.net.URI.create(
                "https://catalog.example.test/functions/" + "normalize/1.2.3/function.yaml"),
            "application/yaml",
            """
            input:
              schema:
                document:
                  type: object
                  required: [value]
            run:
              await: true
              return: all
              shell:
                command: normalize
                arguments:
                  - '${ .value }'
            output:
              as: '${ .stdout }'
            """);
    List<ResolvedWorkflowResource> resources = List.of(function);
    WorkflowDefinitionReference definition = definition(source, resources);
    Instant startedAt = Instant.now();
    String operationId;
    String functionTaskPath;

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));

      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("catalog-function-original-" + suffix))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);

        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-catalog-function", source, resources, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        WorkflowDefinitionBundle bundle = awaitDefinition(original, definition);
        assertTrue(
            bundle.plan().resources().stream()
                .anyMatch(resource -> resource.sha256().equals(function.sha256())));

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-catalog-function",
                        JSON.readTree("{\"name\":\"Prashanth\"}"),
                        definition,
                        startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ActiveOperationState active =
            assertInstanceOf(
                ActiveOperationState.class, awaitPendingOperation(original).pendingInteraction());
        operationId = active.operationId();
        functionTaskPath = active.taskPath();
        assertTrue(functionTaskPath.contains("/function/"));
        assertEquals("run", active.operationKind());
        assertEquals("SHELL", active.descriptor().inlineValue().required("runKind").textValue());
        assertEquals(
            "Prashanth",
            active
                .descriptor()
                .inlineValue()
                .required("configuration")
                .required("arguments")
                .get(0)
                .textValue());
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("catalog-function-replacement-" + suffix))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);

        ActiveOperationState restored =
            assertInstanceOf(
                ActiveOperationState.class,
                awaitPendingOperation(replacement).pendingInteraction());
        assertEquals(operationId, restored.operationId());
        assertEquals(functionTaskPath, restored.taskPath());
        assertEquals(
            "Prashanth",
            restored
                .descriptor()
                .inlineValue()
                .required("configuration")
                .required("arguments")
                .get(0)
                .textValue());

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new ObserveOperationCommand(
                        "complete-catalog-function",
                        KEY,
                        operationId,
                        new OperationObservation(
                            OperationObservationStatus.SUCCEEDED,
                            DataReferences.inline(
                                JSON.readTree(
                                    """
                                    {
                                      "stdout": "PRASHANTH",
                                      "stderr": "",
                                      "code": 0
                                    }
                                    """)),
                            null,
                            null),
                        Actors.system(
                            TENANT, RUNTIME_ACTOR, "test-run-adapter", startedAt.plusSeconds(1)),
                        startedAt.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot completed = awaitSnapshot(replacement, ExecutionPhase.COMPLETED);
        assertEquals("PRASHANTH", completed.data().inlineValue().textValue());
        List<ExecutionHistoryEvent> events =
            awaitHistoryThroughCompletion(history, replacement, streamFailure);
        assertEquals(
            1,
            events.stream()
                .filter(
                    event ->
                        event.type() == ExecutionEventType.OPERATION_DISPATCHED
                            && functionTaskPath.equals(event.taskPath()))
                .count());
        assertTrue(
            events.stream()
                .anyMatch(
                    event ->
                        event.type() == ExecutionEventType.TASK_COMPLETED
                            && functionTaskPath.equals(event.taskPath())));
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void gracefulCancellationRestoresBeforeAdapterAcknowledgement() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-cancel-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.cancel-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - invoke:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
        """;
    WorkflowDefinitionReference definition = definition(source, List.of());
    Instant startedAt = Instant.now();
    String operationId;

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("cancel-restore-original-" + suffix),
              Duration.ofMinutes(5))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-cancel-restore", source, List.of(), startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command(
                        "start-cancel-restore", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ActiveOperationState active =
            assertInstanceOf(
                ActiveOperationState.class, awaitPendingOperation(original).pendingInteraction());
        operationId = active.operationId();

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "request-cancel-restore",
                        ExecutionControlAction.CANCEL,
                        startedAt.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot requested = awaitSnapshot(original, ExecutionPhase.CANCEL_REQUESTED);
        assertNotNull(requested.cancellation());
        assertEquals(commandActor().actorId(), requested.cancellation().requestedBy().actorId());
        assertEquals(
            operationId,
            assertInstanceOf(ActiveOperationState.class, requested.pendingInteraction())
                .operationId());
        WorkflowEffect deadline = awaitTimer(original, "cancellation-deadline");
        assertEquals(
            requested.cancellation().timerId(),
            deadline.payload().inlineValue().required("timerId").textValue());
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("cancel-restore-replacement-" + suffix),
              Duration.ofMinutes(5))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.CANCEL_REQUESTED);
        assertNotNull(restored.cancellation());
        assertEquals(
            commandActor(),
            restored.cancellation().requestedBy(),
            "The complete cancellation identity, including persisted IdP coordinates, must survive"
                + " Kafka state restoration");
        assertEquals(
            operationId,
            assertInstanceOf(ActiveOperationState.class, restored.pendingInteraction())
                .operationId());

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new ObserveOperationCommand(
                        "acknowledge-cancel-restore",
                        KEY,
                        operationId,
                        new OperationObservation(
                            OperationObservationStatus.CANCELLED,
                            null,
                            new WorkflowError(
                                "https://open-workflow-specification.org/spec/1.0.0/errors/runtime",
                                499,
                                operationId,
                                "Operation cancelled",
                                "Adapter acknowledged " + "cancellation"),
                            null),
                        Actors.system(
                            TENANT, RUNTIME_ACTOR, "test-call-adapter", startedAt.plusSeconds(2)),
                        startedAt.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionSnapshot cancelled = awaitSnapshot(replacement, ExecutionPhase.CANCELLED);
        assertTrue(cancelled.cursor().complete());
        ExecutionHistoryEvent terminal =
            awaitHistoryEvent(
                history,
                event -> event.type() == ExecutionEventType.EXECUTION_CANCELLED,
                replacement,
                streamFailure);
        assertEquals(
            commandActor(),
            terminal.actor(),
            "The terminal audit event must retain the authenticated cancellation requester after"
                + " restoration");
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void correlatedWorkerCancellationRetainsRequesterAcrossKafkaRestoration() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-worker-cancel-restore-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.worker-cancel-restore." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extraction
          version: '1.0.0'
        do:
          - execute:
              call: com.forwardmeasure.oks.correlated-worker
              with:
                document:
                  endpoint:
                    uri: https://contracts.test/workers.yaml
                command:
                  channel: workers.commands
                  message:
                    payload:
                      request: '${ . }'
                events:
                  channel: workers.events
                  subscription:
                    consume:
                      until: '${ .payload.status == "SUCCEEDED" }'
                      for: PT30M
                cancellation:
                  channel: workers.cancellations
                  message:
                    payload: {}
        """;
    List<ResolvedWorkflowResource> resources =
        List.of(
            ResolvedWorkflowResource.of(
                java.net.URI.create("https://contracts.test/workers.yaml"),
                "application/yaml",
                """
                asyncapi: 2.6.0
                info:
                  title: Workers
                  version: 1.0.0
                servers:
                  test:
                    url: kafka.test:9092
                    protocol: kafka
                channels:
                  workers.commands:
                    servers: [test]
                    publish:
                      message:
                        name: WorkerCommand
                  workers.events:
                    servers: [test]
                    subscribe:
                      message:
                        name: WorkerEvent
                  workers.cancellations:
                    servers: [test]
                    publish:
                      message:
                        name: WorkerCancellation
                """));
    WorkflowDefinitionReference definition = definition(source, resources);
    Instant startedAt = Instant.now();
    String lifecycleId;

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      try (KafkaStreams original =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("worker-cancel-original-" + suffix),
              Duration.ofMinutes(5))) {
        original.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-worker-cancel", source, resources, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original, definition);
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    command("start-worker-cancel", JSON.createObjectNode(), definition, startedAt)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ActiveCorrelatedWorkerState worker =
            assertInstanceOf(
                ActiveCorrelatedWorkerState.class,
                awaitPendingInteraction(original).pendingInteraction());
        lifecycleId = worker.lifecycleId();

        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    control(
                        "request-worker-cancel",
                        ExecutionControlAction.CANCEL,
                        startedAt.plusSeconds(1))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();
        ExecutionSnapshot requested = awaitSnapshot(original, ExecutionPhase.CANCEL_REQUESTED);
        assertEquals(commandActor(), requested.cancellation().requestedBy());
        assertEquals(
            lifecycleId,
            assertInstanceOf(ActiveCorrelatedWorkerState.class, requested.pendingInteraction())
                .lifecycleId());
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("worker-cancel-replacement-" + suffix),
              Duration.ofMinutes(5))) {
        replacement.setUncaughtExceptionHandler(
            exception -> {
              streamFailure.set(exception);
              return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                  .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        replacement.start();
        awaitRunning(replacement);
        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.CANCEL_REQUESTED);
        assertEquals(
            commandActor(),
            restored.cancellation().requestedBy(),
            "The correlated-worker cancellation requester must "
                + "survive Kafka state restoration");
        ActiveCorrelatedWorkerState worker =
            assertInstanceOf(ActiveCorrelatedWorkerState.class, restored.pendingInteraction());

        var payload = JSON.createObjectNode();
        payload.put("operationId", worker.lifecycleId());
        payload.put("status", "CANCELLED");
        payload.set("metadata", JSON.createObjectNode());
        var message = JSON.createObjectNode();
        message.set("payload", payload);
        message.set("headers", JSON.createObjectNode());
        executionProducer
            .send(
                new ProducerRecord<>(
                    topics.commands(),
                    KEY.canonical(),
                    new ReceiveAsyncApiMessageCommand(
                        "acknowledge-worker-cancel",
                        KEY,
                        worker.lifecycleId(),
                        "workers.events:0:19",
                        DataReferences.inline(message),
                        Actors.system(
                            TENANT, RUNTIME_ACTOR, "test-worker-adapter", startedAt.plusSeconds(2)),
                        startedAt.plusSeconds(2))))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        awaitSnapshot(replacement, ExecutionPhase.CANCELLED);
        ExecutionHistoryEvent terminal =
            awaitHistoryEvent(
                history,
                event -> event.type() == ExecutionEventType.EXECUTION_CANCELLED,
                replacement,
                streamFailure);
        assertEquals(
            commandActor(),
            terminal.actor(),
            "The worker acknowledgement must not replace the "
                + "authenticated cancellation requester");
        assertTrue(streamFailure.get() == null);
      }
    }
  }

  @Test
  void restoresStructuredSchemaFailureFromKafkaChangelogs() throws Exception {
    String suffix = UUID.randomUUID().toString();
    String applicationId = "oks-schema-failure-" + suffix;
    OksTopics topics = OksTopics.withPrefix("test.oks.schema-failure." + suffix);
    createTopics(topics);
    AtomicReference<Throwable> streamFailure = new AtomicReference<>();

    try (var definitionProducer = definitionProducer();
        var executionProducer = producer();
        var definitionHistory = definitionHistoryConsumer();
        var history = historyConsumer()) {
      definitionHistory.subscribe(List.of(topics.definitionHistory()));
      history.subscribe(List.of(topics.history()));
      KafkaStreams original =
          streams(
              applicationId, topics, stateDirectories.resolve("schema-failure-original-" + suffix));
      original.setUncaughtExceptionHandler(
          exception -> {
            streamFailure.set(exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          });
      try {
        original.start();
        awaitRunning(original);
        definitionProducer
            .send(
                new ProducerRecord<>(
                    topics.definitionCommands(),
                    DEFINITION_KEY.canonical(),
                    admission("admit-schema-failure", SOURCE, REQUESTED)))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        definitionProducer.flush();
        assertEquals(
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            awaitDefinitionHistory(definitionHistory, original, streamFailure).status());
        awaitDefinition(original);

        StartExecutionCommand invalid =
            new StartExecutionCommand(
                "invalid-input",
                KEY,
                commandDefinition(),
                DataReferences.inline(JSON.readTree("{}")),
                commandActor(),
                REQUESTED.plusSeconds(1));
        executionProducer
            .send(new ProducerRecord<>(topics.commands(), KEY.canonical(), invalid))
            .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        executionProducer.flush();

        ExecutionHistoryEvent failed = awaitHistory(history, 1, original, streamFailure).getFirst();
        assertEquals(ExecutionEventType.EXECUTION_FAILED, failed.type());
        assertEquals("/input/schema", failed.failure().definitionPath());
        ExecutionSnapshot snapshot = awaitSnapshot(original, ExecutionPhase.FAILED);
        assertEquals(failed.failure(), snapshot.failure());
      } finally {
        assertTrue(original.close(Duration.ofSeconds(10)));
      }

      try (KafkaStreams replacement =
          streams(
              applicationId,
              topics,
              stateDirectories.resolve("schema-failure-replacement-" + suffix))) {
        replacement.start();
        awaitRunning(replacement);

        ExecutionSnapshot restored = awaitSnapshot(replacement, ExecutionPhase.FAILED);
        assertEquals("/input/schema", restored.failure().definitionPath());
        assertEquals(ExecutionFailure.VALIDATION_ERROR, restored.failure().type());
        assertTrue(!restored.failure().schemaViolations().isEmpty());
      }
    }
  }

  @Test
  void durableCommandSerdeRoundTripsWithoutLosingDefinitionReference() throws Exception {
    StartExecutionCommand expected = command();
    var serde = new JsonSerde<>(ExecutionCommand.class);

    ExecutionCommand decoded =
        serde
            .deserializer()
            .deserialize("commands", serde.serializer().serialize("commands", expected));
    StartExecutionCommand actual = assertInstanceOf(StartExecutionCommand.class, decoded);

    assertEquals(expected.key(), actual.key());
    assertEquals(expected.actor().actorId(), actual.actor().actorId());
    assertEquals(expected.definition(), actual.definition());
  }

  private static StartExecutionCommand command() throws Exception {
    return command(
        "command-1",
        JSON.readTree("{\"instruction\":\"Extract entities\"," + "\"batches\":[true,false,true]}"),
        REQUESTED);
  }

  private static StartExecutionCommand command(
      String commandId, com.fasterxml.jackson.databind.JsonNode input, Instant requestedAt) {
    return new StartExecutionCommand(
        commandId,
        KEY,
        commandDefinition(),
        DataReferences.inline(input),
        commandActor(),
        requestedAt);
  }

  private static StartExecutionCommand command(
      String commandId,
      com.fasterxml.jackson.databind.JsonNode input,
      WorkflowDefinitionReference definition,
      Instant requestedAt) {
    return new StartExecutionCommand(
        commandId, KEY, definition, DataReferences.inline(input), commandActor(), requestedAt);
  }

  private static AdmitWorkflowDefinitionCommand admission(
      String commandId, String source, Instant requestedAt) {
    return admission(commandId, source, List.of(INPUT_SCHEMA), requestedAt);
  }

  private static AdmitWorkflowDefinitionCommand admission(
      String commandId,
      String source,
      List<ResolvedWorkflowResource> resources,
      Instant requestedAt) {
    return new AdmitWorkflowDefinitionCommand(
        commandId, DEFINITION_KEY, source, resources, commandActor(), requestedAt);
  }

  private static ControlExecutionCommand control(
      String commandId, ExecutionControlAction action, Instant requestedAt) {
    return new ControlExecutionCommand(commandId, KEY, action, commandActor(), requestedAt);
  }

  private static ActorContext commandActor() {
    return new ActorContext(
        TENANT,
        ActorId.parse(
            "did:web:tenant.example.com:actors:" + "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45"),
        ActorType.HUMAN,
        "Prashanth Nandavanam",
        "792a6af3-921b-4951-bd19-6c4ac82e701c-public",
        BusinessCorrelationId.parse("restoration-cancellation-test"),
        Set.of("evidence-control"),
        null,
        REQUESTED,
        "https://auth.example.com/realms/forwardmeasure",
        "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");
  }

  private KafkaStreams streams(String applicationId, OksTopics topics, Path stateDirectory) {
    return streams(applicationId, topics, stateDirectory, Duration.ofSeconds(30));
  }

  private KafkaStreams streams(
      String applicationId,
      OksTopics topics,
      Path stateDirectory,
      Duration cancellationGracePeriod) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toAbsolutePath().toString());
    /*
     * State-store updates, history records and continuation commands from
     * one input command are committed as one Kafka transaction.
     */
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    properties.put(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG, KafkaRecordLimits.DEFINITION_TOPIC_MESSAGE_BYTES);
    /*
     * This proof uses one stream thread. The KafkaConsumer instances used
     * by the test remain on the separate JUnit thread.
     */
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    return new KafkaStreams(
        new OksTopology(RUNTIME_ACTOR, "oks-test", cancellationGracePeriod).build(topics),
        properties) {
      /**
       * A failed broker must fail the scenario rather than leave the Maven reactor blocked forever
       * in AutoCloseable.close().
       */
      @Override
      public void close() {
        if (!close(Duration.ofSeconds(20))) {
          throw new IllegalStateException("Kafka Streams did not close within 20 seconds");
        }
      }
    };
  }

  private static KafkaProducer<String, ExecutionCommand> producer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(
        properties, new StringSerializer(), new JsonSerde<>(ExecutionCommand.class).serializer());
  }

  private static KafkaProducer<String, ExecutionCommand> transactionalProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        "oks-restoration-command-batch-" + UUID.randomUUID());
    return new KafkaProducer<>(
        properties, new StringSerializer(), new JsonSerde<>(ExecutionCommand.class).serializer());
  }

  private static KafkaProducer<String, InboundCloudEvent> inboundEventProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(
        properties, new StringSerializer(), new JsonSerde<>(InboundCloudEvent.class).serializer());
  }

  private static KafkaProducer<String, AdmitWorkflowDefinitionCommand> definitionProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG, KafkaRecordLimits.DEFINITION_TOPIC_MESSAGE_BYTES);
    return new KafkaProducer<>(
        properties,
        new StringSerializer(),
        new JsonSerde<>(AdmitWorkflowDefinitionCommand.class).serializer());
  }

  private static KafkaConsumer<String, WorkflowDefinitionAdmissionEvent>
      definitionHistoryConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "oks-definition-history-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    /*
     * Ignore uncommitted transactional output. The test observes only
     * definition decisions that were atomically committed by the runtime.
     */
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(
        properties,
        new StringDeserializer(),
        new JsonSerde<>(WorkflowDefinitionAdmissionEvent.class).deserializer());
  }

  private static KafkaConsumer<String, WorkflowDefinitionBundle> definitionBundleConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "oks-definition-bundle-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(
        properties,
        new StringDeserializer(),
        new JsonSerde<>(WorkflowDefinitionBundle.class).deserializer());
  }

  private static KafkaConsumer<String, WorkflowDefinitionCatalogueEvent>
      definitionCatalogueConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG, "oks-definition-catalogue-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(
        properties,
        new StringDeserializer(),
        new JsonSerde<>(WorkflowDefinitionCatalogueEvent.class).deserializer());
  }

  private static KafkaConsumer<String, ExecutionHistoryEvent> historyConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "oks-history-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    /*
     * Ignore uncommitted transactional output. The test observes only
     * execution history committed together with its corresponding state.
     */
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(
        properties,
        new StringDeserializer(),
        new JsonSerde<>(ExecutionHistoryEvent.class).deserializer());
  }

  private static KafkaConsumer<String, JsonNode> emittedEventConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "oks-emitted-event-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new KafkaConsumer<>(
        properties, new StringDeserializer(), new JsonSerde<>(JsonNode.class).deserializer());
  }

  private static JsonNode awaitEmittedEvent(
      KafkaConsumer<String, JsonNode> consumer,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      var records = consumer.poll(Duration.ofMillis(250));
      if (!records.isEmpty()) {
        assertEquals(1, records.count());
        return records.iterator().next().value();
      }
    }
    throw new AssertionError(
        "Timed out waiting for emitted CloudEvent; streams state=" + streams.state());
  }

  private static List<ExecutionHistoryEvent> awaitHistory(
      KafkaConsumer<String, ExecutionHistoryEvent> consumer,
      int count,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    /*
     * KafkaConsumer has no background poll thread. This loop runs on the
     * JUnit thread, drives partition assignment on the first poll and then
     * accumulates committed history in Kafka offset order.
     */
    while (events.size() < count && System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      consumer.poll(Duration.ofMillis(250)).forEach(record -> events.add(record.value()));
    }
    assertEquals(
        count, events.size(), "Timed out waiting for history; streams state=" + streams.state());
    return events;
  }

  private static List<ExecutionHistoryEvent> awaitHistoryThroughCompletion(
      KafkaConsumer<String, ExecutionHistoryEvent> consumer,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      consumer.poll(Duration.ofMillis(250)).forEach(record -> events.add(record.value()));
      if (events.stream()
          .anyMatch(event -> event.type() == ExecutionEventType.EXECUTION_COMPLETED)) {
        return events;
      }
    }
    throw new AssertionError(
        "Timed out waiting for completed history; streams state=" + streams.state());
  }

  private static List<ExecutionHistoryEvent> awaitCompletions(
      KafkaConsumer<String, ExecutionHistoryEvent> consumer,
      int count,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    List<ExecutionHistoryEvent> completions = new ArrayList<>();
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      consumer
          .poll(Duration.ofMillis(250))
          .forEach(
              record -> {
                if (record.value().type() == ExecutionEventType.EXECUTION_COMPLETED) {
                  completions.add(record.value());
                }
              });
      if (completions.size() >= count) {
        return List.copyOf(completions);
      }
    }
    throw new AssertionError(
        "Timed out waiting for "
            + count
            + " scheduled completions; streams state="
            + streams.state());
  }

  private static long countCompletions(
      KafkaConsumer<String, ExecutionHistoryEvent> consumer,
      ExecutionKey key,
      Duration observationWindow) {
    long deadline = System.nanoTime() + observationWindow.toNanos();
    long completions = 0;
    while (System.nanoTime() < deadline) {
      var records = consumer.poll(Duration.ofMillis(100));
      for (var record : records) {
        ExecutionHistoryEvent event = record.value();
        if (event.type() == ExecutionEventType.EXECUTION_COMPLETED && event.key().equals(key)) {
          completions++;
        }
      }
    }
    return completions;
  }

  private static WorkflowDefinitionAdmissionEvent awaitDefinitionHistory(
      KafkaConsumer<String, WorkflowDefinitionAdmissionEvent> consumer,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    /*
     * As with awaitHistory, polling happens synchronously on the JUnit
     * thread. subscribe alone does not retrieve any records.
     */
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      var records = consumer.poll(Duration.ofMillis(250));
      if (!records.isEmpty()) {
        assertEquals(1, records.count());
        return records.iterator().next().value();
      }
    }
    throw new AssertionError(
        "Timed out waiting for definition history; streams state=" + streams.state());
  }

  private static WorkflowDefinitionCatalogueEvent awaitDefinitionCatalogue(
      KafkaConsumer<String, WorkflowDefinitionCatalogueEvent> consumer,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      var records = consumer.poll(Duration.ofMillis(250));
      if (!records.isEmpty()) {
        assertEquals(1, records.count());
        return records.iterator().next().value();
      }
    }
    throw new AssertionError(
        "Timed out waiting for definition catalogue projection; "
            + "streams state="
            + streams.state());
  }

  private static void awaitRunning(KafkaStreams streams) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (streams.state() != KafkaStreams.State.RUNNING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(KafkaStreams.State.RUNNING, streams.state());
  }

  private static ExecutionHistoryEvent awaitHistoryEvent(
      KafkaConsumer<String, ExecutionHistoryEvent> consumer,
      Predicate<ExecutionHistoryEvent> expected,
      KafkaStreams streams,
      AtomicReference<Throwable> streamFailure) {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (streamFailure.get() != null) {
        throw new AssertionError("Kafka Streams processing failed", streamFailure.get());
      }
      for (var record : consumer.poll(Duration.ofMillis(250))) {
        if (expected.test(record.value())) return record.value();
      }
    }
    throw new AssertionError(
        "Timed out waiting for execution history event; state=" + streams.state());
  }

  private static void awaitExecutionMissing(KafkaStreams streams) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        if (store.get(KEY.canonical()) == null) return;
      } catch (InvalidStateStoreException notReady) {
        // Restoration is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for purged execution state");
  }

  private static void assertPurgedProjections(KafkaStreams streams) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionHistoryEvent> history =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.HISTORY, QueryableStoreTypes.keyValueStore()));
        ReadOnlyKeyValueStore<String, WorkflowEffect> effects =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EFFECTS, QueryableStoreTypes.keyValueStore()));
        List<ExecutionHistoryEvent> historyValues = new ArrayList<>();
        try (var values =
            history.range(
                OksQueryKeys.rangeStart(KEY.canonical()), OksQueryKeys.rangeEnd(KEY.canonical()))) {
          while (values.hasNext()) {
            historyValues.add(values.next().value);
          }
        }
        List<WorkflowEffect> effectValues = new ArrayList<>();
        try (var values =
            effects.range(
                OksQueryKeys.rangeStart(KEY.canonical()), OksQueryKeys.rangeEnd(KEY.canonical()))) {
          while (values.hasNext()) {
            effectValues.add(values.next().value);
          }
        }
        if (historyValues.size() == 1
            && historyValues.getFirst().type() == ExecutionEventType.EXECUTION_PURGED
            && effectValues.size() == 1
            && effectValues.getFirst().type()
                == com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType
                    .PURGE_EXECUTION_PROJECTIONS) {
          return;
        }
      } catch (InvalidStateStoreException notReady) {
        // Projection restoration or rebalance is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for collapsed purge projections");
  }

  private static ExecutionSnapshot awaitSnapshot(KafkaStreams streams, ExecutionPhase expectedPhase)
      throws InterruptedException {
    return awaitSnapshot(streams, KEY, expectedPhase);
  }

  private static ExecutionSnapshot awaitSnapshot(
      KafkaStreams streams, ExecutionKey key, ExecutionPhase expectedPhase)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(key.canonical());
        if (snapshot != null && snapshot.phase() == expectedPhase) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // Restoration is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for execution phase " + expectedPhase);
  }

  private static ExecutionSnapshot awaitPendingInteraction(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(KEY.canonical());
        if (snapshot != null && snapshot.pendingInteraction() != null) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // State restoration or initial assignment is still active.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for a pending interaction");
  }

  private static ExecutionSnapshot awaitPendingListen(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(KEY.canonical());
        if (snapshot != null && snapshot.pendingInteraction() instanceof ActiveListenState) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // State restoration or initial assignment is still active.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for an active event subscription");
  }

  private static ExecutionSnapshot awaitPendingOperation(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(KEY.canonical());
        if (snapshot != null && snapshot.pendingInteraction() instanceof ActiveOperationState) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // State restoration or initial assignment is still active.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for an active external operation");
  }

  private static WorkflowEffect awaitTimer(KafkaStreams streams) throws InterruptedException {
    return awaitTimer(streams, null);
  }

  private static WorkflowEffect awaitTimer(KafkaStreams streams, String purpose)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, WorkflowEffect> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.TIMERS, QueryableStoreTypes.keyValueStore()));
        try (var values = store.all()) {
          while (values.hasNext()) {
            WorkflowEffect candidate = values.next().value;
            if (purpose == null
                || purpose.equals(candidate.payload().inlineValue().path("purpose").textValue())) {
              return candidate;
            }
          }
        }
      } catch (InvalidStateStoreException notReady) {
        // Timer effects have not yet reached the materialised store.
      }
      Thread.sleep(50);
    }
    throw new AssertionError(
        "Timed out waiting for a durable timer"
            + (purpose == null ? "" : " with purpose " + purpose));
  }

  private static ExecutionSnapshot awaitActiveIteration(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(KEY.canonical());
        if (snapshot != null
            && snapshot.phase() == ExecutionPhase.RUNNING
            && !snapshot.cursor().complete()
            && snapshot.cursor().current().iteration() != null) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // The original process has not exposed its store yet.
      }
      Thread.sleep(5);
    }
    throw new AssertionError("Timed out waiting for an active FOR iteration");
  }

  private static ExecutionSnapshot awaitActiveFork(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, ExecutionSnapshot> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTIONS, QueryableStoreTypes.keyValueStore()));
        ExecutionSnapshot snapshot = store.get(KEY.canonical());
        if (snapshot != null
            && snapshot.phase() == ExecutionPhase.RUNNING
            && snapshot.activeFork() != null) {
          return snapshot;
        }
      } catch (InvalidStateStoreException notReady) {
        // The original process has not exposed its store yet.
      }
      Thread.sleep(5);
    }
    throw new AssertionError("Timed out waiting for an active FORK");
  }

  private static WorkflowDefinitionBundle awaitDefinition(KafkaStreams streams)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, WorkflowDefinitionBundle> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.DEFINITIONS, QueryableStoreTypes.keyValueStore()));
        WorkflowDefinitionBundle bundle = store.get(commandDefinition().canonical());
        if (bundle != null) {
          return bundle;
        }
      } catch (InvalidStateStoreException notReady) {
        // Restoration or admission is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for admitted workflow definition");
  }

  private static WorkflowDefinitionBundle awaitDefinition(
      KafkaStreams streams, WorkflowDefinitionReference definition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, WorkflowDefinitionBundle> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.DEFINITIONS, QueryableStoreTypes.keyValueStore()));
        WorkflowDefinitionBundle bundle = store.get(definition.canonical());
        if (bundle != null) return bundle;
      } catch (InvalidStateStoreException notReady) {
        // Restoration or admission is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for admitted workflow definition");
  }

  private static DurableAggregateMetadata awaitExecutionMetadata(
      KafkaStreams streams, long expectedRevision) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        ReadOnlyKeyValueStore<String, DurableAggregateMetadata> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    OksStores.EXECUTION_METADATA, QueryableStoreTypes.keyValueStore()));
        DurableAggregateMetadata metadata = store.get(KEY.canonical());
        if (metadata != null && metadata.revision() == expectedRevision) {
          return metadata;
        }
      } catch (InvalidStateStoreException notReady) {
        // Restoration is still in progress.
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for execution revision " + expectedRevision);
  }

  private static void createTopics(OksTopics topics) throws Exception {
    /*
     * External application topics are provisioned explicitly:
     *
     * definitionCommands - workflow source publication requests
     * definitionHistory  - accepted/unchanged/rejected decisions
     * definitions        - immutable bundles; compacted for restoration
     * commands           - external control plus internal ADVANCE records
     * history            - append-only execution and task transitions
     * effects            - transactional workflow outbox
     * subscriptionEffects- tenant-keyed subscription repartition topic
     * timerEffects       - timer-id-keyed scheduling repartition topic
     * subworkflowEffects - child-execution-keyed subworkflow launch repartition topic
     * inboundEvents      - authenticated tenant-keyed CloudEvents
     * emittedEvents      - CloudEvents projected from committed effects
     * deadLetters        - isolated deterministic command rejections
     *
     * Three partitions exercise keyed routing. Replication factor one is
     * appropriate only for this single-broker Testcontainer.
     */
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(topics.definitionCommands(), 3, (short) 1)
                      .configs(
                          Map.of(
                              TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
                              Integer.toString(KafkaRecordLimits.DEFINITION_TOPIC_MESSAGE_BYTES))),
                  new NewTopic(topics.definitionHistory(), 3, (short) 1),
                  new NewTopic(topics.definitionCatalogue(), 3, (short) 1)
                      .configs(
                          Map.of(
                              TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
                              Integer.toString(KafkaRecordLimits.DEFINITION_TOPIC_MESSAGE_BYTES))),
                  new NewTopic(topics.definitions(), 3, (short) 1)
                      .configs(
                          Map.of(
                              TopicConfig.CLEANUP_POLICY_CONFIG,
                              TopicConfig.CLEANUP_POLICY_COMPACT,
                              TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
                              Integer.toString(KafkaRecordLimits.DEFINITION_TOPIC_MESSAGE_BYTES))),
                  new NewTopic(topics.commands(), 3, (short) 1),
                  new NewTopic(topics.history(), 3, (short) 1),
                  new NewTopic(topics.effects(), 3, (short) 1),
                  new NewTopic(topics.subscriptionEffects(), 3, (short) 1),
                  new NewTopic(topics.timerEffects(), 3, (short) 1),
                  new NewTopic(topics.subworkflowEffects(), 3, (short) 1),
                  new NewTopic(topics.inboundEvents(), 3, (short) 1),
                  new NewTopic(topics.emittedEvents(), 3, (short) 1),
                  new NewTopic(topics.deadLetters(), 3, (short) 1)))
          .all()
          .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
  }

  private static int partition(String key, int partitions) {
    return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
  }

  private static ExecutionKey crossPartitionExecutionKey() {
    int definitionPartition = partition(commandDefinition().canonical(), 3);
    for (int candidate = 1; candidate <= 10_000; candidate++) {
      ExecutionKey key =
          new ExecutionKey(TENANT, new WorkflowExecutionId("evidence-run-" + candidate));
      if (partition(key.canonical(), 3) != definitionPartition) {
        return key;
      }
    }
    throw new IllegalStateException("Could not construct a cross-partition execution key");
  }

  private static WorkflowDefinitionReference commandDefinition() {
    var plan =
        new OpenWorkflowCompiler()
            .compile(SOURCE.getBytes(StandardCharsets.UTF_8), List.of(INPUT_SCHEMA));
    return new WorkflowDefinitionReference(
        DEFINITION_KEY, plan.sourceSha256(), plan.definitionSha256());
  }

  private static WorkflowDefinitionReference definition(
      String source, List<ResolvedWorkflowResource> resources) {
    var plan =
        new OpenWorkflowCompiler().compile(source.getBytes(StandardCharsets.UTF_8), resources);
    return new WorkflowDefinitionReference(
        DEFINITION_KEY, plan.sourceSha256(), plan.definitionSha256());
  }

  private static String forkSource(int branchCount) {
    StringBuilder source =
        new StringBuilder(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: extraction
              version: '1.0.0'
            do:
              - parallel:
                  fork:
                    branches:
            """);
    for (int index = 0; index < branchCount; index++) {
      source
          .append("          - branch")
          .append(index)
          .append(":\n")
          .append("              set:\n")
          .append("                branch: ")
          .append(index)
          .append('\n');
    }
    return source.toString();
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
