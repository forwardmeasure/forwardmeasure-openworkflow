/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.k3s;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEngineProvider;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Deploys the already-built Quarkus Pekko engine image into a real, disposable k3s cluster (via
 * plain {@code kubectl}, matching the manifest {@code helmfile template} itself renders for the
 * pekko-postgresql-quarkus release, minus only the registry pull secret and digest pin) and drives
 * it over its real {@code EngineCommandResource} HTTP API - proves the Phase 12 outbox/subworkflow
 * wiring actually dispatches a subworkflow launch and propagates control to it in a real Kubernetes
 * deployment, not just in unit/integration tests.
 *
 * <p>Deliberately does not test publish:emit: CloudEvent delivery here - that needs an external
 * sink reachable from inside a k3s pod, a genuinely separate networking question from what this
 * test already answers (can the real image boot, join a one-node Pekko cluster, and correctly run a
 * workflow against a real in-cluster Postgres). {@code EngineCommandResource} has no query endpoint
 * (state queries go through execution-management, not deployed here), so subworkflow launch is
 * proven the same way: pausing the independently-computed child execution id and observing a real
 * {@code PAUSED} acknowledgement only exists if the child actually launched.
 *
 * <p>Opt-in only (see this module's pom - excluded from the default build). Needs Docker, the
 * target image already built (`mvn -Pcontainer-image package` on the sibling quarkus module), and
 * `kubectl` on PATH. Run explicitly: {@code mvn test -pl
 * openworkflow-deployments/engine-pekko/k3s-verification -DexcludedGroups= -Dtest=
 * PekkoEngineQuarkusK3sVerificationTest}.
 */
@Tag("k3s-verification")
class PekkoEngineQuarkusK3sVerificationTest {
  private static final Duration TIMEOUT = Duration.ofMinutes(3);
  private static final String ENGINE_IMAGE =
      "forwardmeasure/openworkflow-engine-pekko-quarkus:1.0.0-SNAPSHOT";
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
  private static final String RUNTIME_ROLE = "openworkflow";
  private static final String RUNTIME_PASSWORD = "k3s-verification-only";
  private static final String CHILD_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: k3s-subworkflow-child
        version: '1.0.0'
      do:
        - delay:
            wait: PT30S
        - finish:
            set:
              child: completed
      """;
  private static final String PARENT_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: k3s-subworkflow-parent
        version: '1.0.0'
      do:
        - child:
            run:
              await: true
              workflow:
                namespace: forwardmeasure
                name: k3s-subworkflow-child
                version: '1.0.0'
                input:
                  seed: '${ .seed }'
      """;

  private static K3sContainer k3s;
  private static Path kubeconfig;

  @BeforeAll
  static void startCluster() throws Exception {
    k3s =
        new K3sContainer(DockerImageName.parse("rancher/k3s:v1.31.2-k3s1"))
            .withLogConsumer(
                new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger(K3sContainer.class)));
    k3s.start();
    kubeconfig = Files.createTempFile("k3s-verification-kubeconfig", ".yaml");
    Files.writeString(kubeconfig, k3s.getKubeConfigYaml());
  }

  @AfterAll
  static void stopCluster() throws Exception {
    if (kubeconfig != null) {
      Files.deleteIfExists(kubeconfig);
    }
    if (k3s != null) {
      k3s.stop();
    }
  }

  @Test
  void subworkflowLaunchesAndAcceptsControlOnARealDeployment() throws Exception {
    kubectl("apply", "-f", writeManifest("postgres.yaml", postgresManifest()));
    kubectl(
        "wait",
        "--for=condition=Available",
        "deployment/postgresql",
        "--timeout=" + TIMEOUT.toSeconds() + "s");

    var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
    String schema = com.forwardmeasure.jpa.tenancy.TenantSchema.forTenant(jpaTenant).value();

    Process postgresForward = portForward("svc/postgresql", 15432, 5432);
    try {
      DataSource adminDataSource = dataSource(15432, "postgres", "postgres");
      var migrator = new OpenWorkflowTenantMigrator(adminDataSource, RUNTIME_ROLE);
      migrator.ensureRuntimeRole(RUNTIME_PASSWORD);
      migrator.provisionAndMigrate(jpaTenant);

      var childPlan =
          new OpenWorkflowCompiler().compile(CHILD_SOURCE.getBytes(StandardCharsets.UTF_8));
      seedPublishedChild(adminDataSource, schema, childPlan);
    } finally {
      postgresForward.destroy();
    }

    kubectl(
        "create",
        "secret",
        "generic",
        "openworkflow-foundation",
        "--from-literal=OPENWORKFLOW_DATABASE_PASSWORD=" + RUNTIME_PASSWORD);

    loadImageIntoK3s();

    kubectl("apply", "-f", writeManifest("engine.yaml", engineManifest()));
    try {
      kubectl(
          "wait",
          "--for=condition=Available",
          "deployment/openworkflow-engine-pekko-postgresql-quarkus",
          "--timeout=" + TIMEOUT.toSeconds() + "s");
    } catch (AssertionError failure) {
      throw new AssertionError(failure.getMessage() + "\n\n" + diagnostics(), failure);
    }

    Process engineForward =
        portForward("svc/openworkflow-engine-pekko-postgresql-quarkus", 18080, 8080);
    try {
      awaitHealthy("http://127.0.0.1:18080/internal/v1/engine/health");

      var childPlan =
          new OpenWorkflowCompiler().compile(CHILD_SOURCE.getBytes(StandardCharsets.UTF_8));
      var subflow =
          new ResolvedSubflow(
              childPlan.coordinates(), childPlan.sourceSha256(), childPlan.definitionSha256());
      WorkflowPlan parentPlan =
          new OpenWorkflowCompiler()
              .compile(
                  PARENT_SOURCE.getBytes(StandardCharsets.UTF_8),
                  List.of(),
                  (namespace, name, version) ->
                      namespace.equals(childPlan.coordinates().namespace())
                              && name.equals(childPlan.coordinates().name())
                              && version.equals(childPlan.coordinates().version())
                          ? Optional.of(subflow)
                          : Optional.empty());

      var provider =
          new HttpExecutionEngineProvider(
              EngineId.PEKKO,
              URI.create("http://127.0.0.1:18080/internal/v1/engine/"),
              HttpClient.newHttpClient(),
              objectMapper(),
              TIMEOUT);
      var context =
          new TenantActorContext(TENANT, "organization-1", new ActorId("k3s-verification-actor"));
      ExecutionId parentExecution = new ExecutionId(TENANT, UUID.randomUUID());

      ExecutionLifecycleState startState;
      try {
        var startAck =
            provider
                .submit(
                    new ExecutionCommandEnvelope(
                        UUID.randomUUID(),
                        "k3s-verification-start",
                        context,
                        EngineId.PEKKO,
                        0,
                        Instant.now(),
                        new ExecutionCommand.Start(
                            parentExecution,
                            DefinitionRevision.from(UUID.randomUUID(), parentPlan),
                            parentPlan,
                            JsonNodeFactory.instance.objectNode().put("seed", "k3s-1"))))
                .toCompletableFuture()
                .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        startState = startAck.state();
      } catch (Exception failure) {
        throw new AssertionError("Start command failed\n\n" + diagnostics(), failure);
      }
      assertEquals(ExecutionLifecycleState.RUNNING, startState);

      ExecutionId childExecution = childExecutionId(parentExecution, subflow);
      ExecutionLifecycleState childState =
          awaitChildLaunched(provider, context, childExecution, parentExecution);
      assertTrue(
          childState == ExecutionLifecycleState.PAUSED,
          "expected the launched child to accept Pause, got " + childState);
    } finally {
      engineForward.destroy();
    }
  }

  /**
   * The child only exists once the subworkflow outbox has actually picked up the durable
   * SubworkflowRequested event and launched it (an async, at-least-once projection, not synchronous
   * with the parent's own Start acknowledgement) - retries Pause against the independently-computed
   * child id until it's accepted or the timeout elapses.
   */
  private static ExecutionLifecycleState awaitChildLaunched(
      HttpExecutionEngineProvider provider,
      TenantActorContext context,
      ExecutionId childExecution,
      ExecutionId parentExecution)
      throws Exception {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    Exception lastFailure = null;
    while (System.nanoTime() < deadline) {
      try {
        var ack =
            provider
                .submit(
                    new ExecutionCommandEnvelope(
                        UUID.randomUUID(),
                        "k3s-verification-pause-child",
                        context,
                        EngineId.PEKKO,
                        0,
                        Instant.now(),
                        new ExecutionCommand.Pause(childExecution)))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        return ack.state();
      } catch (Exception failure) {
        lastFailure = failure;
        Thread.sleep(500);
      }
    }
    throw new AssertionError(
        "Child execution " + childExecution.value() + " never accepted Pause", lastFailure);
  }

  /** Mirrors WorkflowEntity's own derivation exactly - see RealSubworkflowRecoveryTest today. */
  private static ExecutionId childExecutionId(
      ExecutionId parentExecution, ResolvedSubflow subflow) {
    UUID childUuid =
        UUID.nameUUIDFromBytes(
            (parentExecution.entityId() + "|subworkflow|/do/0/child|1|" + subflow.canonical())
                .getBytes(StandardCharsets.UTF_8));
    return new ExecutionId(parentExecution.tenantId(), childUuid);
  }

  private static void seedPublishedChild(DataSource dataSource, String schema, WorkflowPlan plan)
      throws Exception {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "insert into "
              + schema
              + ".actor (id,version,uuid,subject_identifier,identity_type) values"
              + " (1,0,'10000000-0000-0000-0000-000000000001','actor','HUMAN')");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow (id,version,uuid,name,title,owner_id) values"
              + " (1,0,'20000000-0000-0000-0000-000000000001','k3s-subworkflow-child','K3s"
              + " Subworkflow Child',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_definition"
              + " (id,uuid,version,workflow_id,revision_number,lifecycle_state,source_document,"
              + "resolved_document,resolved_resources,namespace,document_version,"
              + "specification_version,compiler_profile,source_digest,resolved_digest,"
              + "author_actor_id) values (1,'30000000-0000-0000-0000-000000000001',0,1,1,"
              + "'PUBLISHED',$workflow$"
              + CHILD_SOURCE
              + "$workflow$,$workflow$"
              + plan.definition()
              + "$workflow$,$resources$"
              + WorkflowResourceBundleCodec.encode(plan.resources())
              + "$resources$,'"
              + plan.coordinates().namespace()
              + "','"
              + plan.coordinates().version()
              + "','"
              + plan.coordinates().dsl()
              + "','default','"
              + plan.sourceSha256()
              + "','"
              + plan.definitionSha256()
              + "',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_publication (id,version,definition_id,actor_id,definition_digest)"
              + " values (1,0,1,1,'"
              + plan.definitionSha256()
              + "')");
    }
  }

  private static void loadImageIntoK3s() throws Exception {
    Path tar = Files.createTempFile("k3s-verification-image", ".tar");
    try {
      run(new ProcessBuilder("docker", "save", "-o", tar.toString(), ENGINE_IMAGE));
      k3s.copyFileToContainer(
          org.testcontainers.utility.MountableFile.forHostPath(tar), "/tmp/engine-image.tar");
      k3s.execInContainer("ctr", "images", "import", "/tmp/engine-image.tar");
    } finally {
      Files.deleteIfExists(tar);
    }
  }

  private static void awaitHealthy(String url) throws Exception {
    var client = HttpClient.newHttpClient();
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    Exception lastFailure = null;
    while (System.nanoTime() < deadline) {
      try {
        var response =
            client.send(
                java.net.http.HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build(),
                java.net.http.HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() / 100 == 2) {
          return;
        }
      } catch (Exception failure) {
        lastFailure = failure;
      }
      Thread.sleep(1000);
    }
    throw new AssertionError("Engine health endpoint never became reachable: " + url, lastFailure);
  }

  private static Process portForward(String target, int localPort, int remotePort)
      throws Exception {
    Process process =
        new ProcessBuilder(
                "kubectl",
                "--kubeconfig",
                kubeconfig.toString(),
                "port-forward",
                target,
                localPort + ":" + remotePort)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    Thread.sleep(2000);
    if (!process.isAlive()) {
      throw new AssertionError("port-forward to " + target + " exited immediately");
    }
    return process;
  }

  private static DataSource dataSource(int port, String username, String password) {
    var dataSource = new PGSimpleDataSource();
    dataSource.setServerNames(new String[] {"127.0.0.1"});
    dataSource.setPortNumbers(new int[] {port});
    dataSource.setDatabaseName("openworkflow");
    dataSource.setUser(username);
    dataSource.setPassword(password);
    return dataSource;
  }

  private static ObjectMapper objectMapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  /**
   * Best-effort pod diagnostics for a failed wait - never throws, so it never masks the real
   * failure.
   */
  private static String diagnostics() {
    var builder = new StringBuilder();
    for (String[] command :
        List.of(
            new String[] {
              "describe",
              "pod",
              "-l",
              "app.kubernetes.io/name=openworkflow-engine-pekko-postgresql-quarkus"
            },
            new String[] {
              "logs",
              "-l",
              "app.kubernetes.io/name=openworkflow-engine-pekko-postgresql-quarkus",
              "--all-containers",
              "--tail=200"
            },
            new String[] {
              "logs",
              "-l",
              "app.kubernetes.io/name=openworkflow-engine-pekko-postgresql-quarkus",
              "--all-containers",
              "--previous",
              "--tail=200"
            })) {
      builder.append("=== kubectl ").append(String.join(" ", command)).append(" ===\n");
      try {
        var full = new java.util.ArrayList<String>();
        full.add("kubectl");
        full.add("--kubeconfig");
        full.add(kubeconfig.toString());
        full.addAll(List.of(command));
        var process = new ProcessBuilder(full).redirectErrorStream(true).start();
        builder.append(new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        process.waitFor(30, TimeUnit.SECONDS);
      } catch (Exception failure) {
        builder.append("(diagnostic command itself failed: ").append(failure).append(")\n");
      }
      builder.append('\n');
    }
    return builder.toString();
  }

  private static void kubectl(String... args) throws Exception {
    var command = new java.util.ArrayList<String>();
    command.add("kubectl");
    command.add("--kubeconfig");
    command.add(kubeconfig.toString());
    command.addAll(List.of(args));
    run(new ProcessBuilder(command));
  }

  private static void run(ProcessBuilder builder) throws Exception {
    builder.redirectErrorStream(true);
    Process process = builder.start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (!process.waitFor(TIMEOUT.toSeconds(), TimeUnit.SECONDS) || process.exitValue() != 0) {
      throw new AssertionError(
          "Command failed: " + String.join(" ", builder.command()) + "\n" + output);
    }
  }

  private static String writeManifest(String fileName, String content) throws IOException {
    Path path = Files.createTempFile(fileName, ".yaml");
    Files.writeString(path, content);
    return path.toString();
  }

  private static String postgresManifest() {
    return """
    apiVersion: apps/v1
    kind: Deployment
    metadata: {name: postgresql}
    spec:
      replicas: 1
      selector: {matchLabels: {app: postgresql}}
      template:
        metadata: {labels: {app: postgresql}}
        spec:
          containers:
            - name: postgresql
              image: postgres:18-alpine
              args: ["-c", "fsync=off"]
              env:
                - {name: POSTGRES_USER, value: postgres}
                - {name: POSTGRES_PASSWORD, value: postgres}
                - {name: POSTGRES_DB, value: openworkflow}
              ports: [{containerPort: 5432}]
              readinessProbe:
                exec: {command: ["pg_isready", "-U", "postgres"]}
                periodSeconds: 2
                failureThreshold: 60
    ---
    apiVersion: v1
    kind: Service
    metadata: {name: postgresql}
    spec:
      selector: {app: postgresql}
      ports: [{port: 5432, targetPort: 5432}]
    """;
  }

  private static String engineManifest() {
    return """
    apiVersion: v1
    kind: Service
    metadata: {name: openworkflow-engine-pekko-postgresql-quarkus}
    spec:
      clusterIP: None
      selector: {app.kubernetes.io/name: openworkflow-engine-pekko-postgresql-quarkus}
      ports:
        - {name: http, port: 8080, targetPort: http}
        - {name: artery, port: 25520, targetPort: artery}
        - {name: management, port: 8558, targetPort: management}
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: openworkflow-engine-pekko-postgresql-quarkus
      labels: {app.kubernetes.io/name: openworkflow-engine-pekko-postgresql-quarkus}
    spec:
      replicas: 1
      selector:
        matchLabels: {app.kubernetes.io/name: openworkflow-engine-pekko-postgresql-quarkus}
      template:
        metadata:
          labels: {app.kubernetes.io/name: openworkflow-engine-pekko-postgresql-quarkus}
        spec:
          terminationGracePeriodSeconds: 10
          containers:
            - name: workflow-engine
              image: forwardmeasure/openworkflow-engine-pekko-quarkus:1.0.0-SNAPSHOT
              imagePullPolicy: IfNotPresent
              env:
                - {name: OPENWORKFLOW_EXECUTION_EVENTS_URL, value: "http://127.0.0.1:1/internal/v1/execution-events/"}
                - {name: OPENWORKFLOW_PERSISTENCE_PROFILE, value: postgresql}
                - {name: OPENWORKFLOW_PERSISTENCE_ENDPOINT, value: "jdbc:postgresql://postgresql:5432/openworkflow"}
                - {name: OPENWORKFLOW_PERSISTENCE_USERNAME, value: openworkflow}
                - {name: OPENWORKFLOW_PERSISTENCE_LOCAL_DATACENTER, value: datacenter1}
                - {name: OPENWORKFLOW_CLUSTER_DISCOVERY_SERVICE, value: openworkflow-engine-pekko-postgresql-quarkus}
                - name: OPENWORKFLOW_CLUSTER_POD_IP
                  valueFrom: {fieldRef: {fieldPath: status.podIP}}
                - {name: OPENWORKFLOW_CLUSTER_REQUIRED_CONTACT_POINTS, value: "1"}
                - {name: OPENWORKFLOW_DATABASE_URL, value: "jdbc:postgresql://postgresql:5432/openworkflow"}
                - {name: OPENWORKFLOW_DATABASE_USERNAME, value: openworkflow}
                - {name: OPENWORKFLOW_EVENTING_ASK_TIMEOUT, value: 10s}
                - {name: OPENWORKFLOW_CLOUD_EVENTS_PUBLISH_URL, value: "http://127.0.0.1:1/"}
                - {name: OPENWORKFLOW_CLOUD_EVENTS_TIMEOUT, value: 10s}
                - {name: OPENWORKFLOW_AUTHORIZATION_ISSUER, value: "http://127.0.0.1:1/realms/openworkflow"}
                - {name: OPENWORKFLOW_AUTHORIZATION_CLIENT_ID, value: openworkflow-engine}
                - {name: OPENWORKFLOW_AUTHORIZATION_CLIENT_SECRET, value: k3s-verification-placeholder}
                - {name: OPENWORKFLOW_AUTHORIZATION_REQUEST_TIMEOUT, value: 5s}
                - {name: OPENWORKFLOW_AUTHORIZATION_DECISION_TTL, value: 30s}
                - {name: OPENWORKFLOW_AUTHORIZATION_MAXIMUM_CACHE_ENTRIES, value: "1000"}
                - {name: OPENWORKFLOW_AUTHORIZATION_POLICY_VERSION, value: "1"}
                - {name: OPENWORKFLOW_AUTHORIZATION_ORGANIZATION_CLIENT_ID, value: openworkflow-engine}
                - name: OPENWORKFLOW_PERSISTENCE_PASSWORD
                  valueFrom: {secretKeyRef: {name: openworkflow-foundation, key: OPENWORKFLOW_DATABASE_PASSWORD}}
                - name: OPENWORKFLOW_DATABASE_PASSWORD
                  valueFrom: {secretKeyRef: {name: openworkflow-foundation, key: OPENWORKFLOW_DATABASE_PASSWORD}}
              ports:
                - {name: http, containerPort: 8080}
                - {name: artery, containerPort: 25520}
                - {name: management, containerPort: 8558}
              startupProbe: {tcpSocket: {port: http}, periodSeconds: 2, failureThreshold: 60}
              readinessProbe: {tcpSocket: {port: http}, periodSeconds: 3, failureThreshold: 3}
    """;
  }
}
