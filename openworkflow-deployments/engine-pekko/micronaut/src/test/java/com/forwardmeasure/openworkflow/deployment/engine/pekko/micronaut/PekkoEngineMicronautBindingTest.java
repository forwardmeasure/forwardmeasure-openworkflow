/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.actor.PekkoEngineRuntime;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngressGateway;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link PekkoEngineMicronautBinding} is the class that wires the persistence profile (Postgres vs
 * Cassandra) into a live {@link PekkoEngineRuntime} and then starts the CloudEvent/subworkflow
 * outbox and subscription projections against that runtime's actor system - see that class's own
 * javadoc for the production incident this guards against: the projections were built correctly but
 * nothing ever started them, so every pending publish/subworkflow/listen silently never resolved
 * (no crash, no error). Its methods are plain {@code @Factory}/{@code @Singleton} bean methods -
 * calling them directly, bypassing the Micronaut container entirely, is a legitimate,
 * container-free way to exercise their actual decision logic and side effects, since
 * {@code @Singleton}/{@code @Value} are metadata the container reads, not something the method body
 * itself depends on.
 *
 * <p>{@code runtime(...)} genuinely starts a Pekko actor system (cluster provider, single-node
 * self-join - {@code discoveryService} left blank here, exactly like this binding already supports
 * for non-Kubernetes use). No real Postgres/Cassandra/Keycloak is available in this test
 * environment, so these tests never let that actor system's shard entities actually receive a
 * message, or a projection actually reach a live database - they instead prove the composed call
 * chain this binding is responsible for is genuinely reached with the right arguments, using
 * hand-rolled {@link Proxy}-based JDBC test doubles for {@link DataSource} (this module has no
 * Mockito or embedded-database test dependency, matching {@code TenantProjectionSupervisorTest} in
 * openworkflow-pekko-runtime, which takes the same approach for the same reason).
 */
class PekkoEngineMicronautBindingTest {
  private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration RESCAN_INTERVAL = Duration.ofMillis(50);
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(20);

  private static final String POSTGRES_ENDPOINT =
      "jdbc:postgresql://127.0.0.1:5432/openworkflow_binding_test";
  private static final String POSTGRES_USERNAME = "openworkflow_test_user";
  private static final String POSTGRES_PASSWORD = "openworkflow_test_password";
  private static final String CASSANDRA_RUNTIME_ENDPOINT = "127.0.0.1:9042";
  private static final String DATACENTER = "datacenter1";

  private static PekkoEngineMicronautBinding binding;
  private static PekkoEngineRuntime postgresRuntime;
  private static PekkoEngineRuntime cassandraRuntime;

  @BeforeAll
  @Timeout(30)
  static void startRuntimes() {
    binding = new PekkoEngineMicronautBinding();
    postgresRuntime =
        binding.runtime(
            event -> CompletableFuture.completedFuture(null),
            "pekko-micronaut-binding-test-postgres",
            ASK_TIMEOUT,
            "postgresql",
            POSTGRES_ENDPOINT,
            POSTGRES_USERNAME,
            POSTGRES_PASSWORD,
            DATACENTER,
            "",
            "",
            0,
            8558,
            1,
            "");
    cassandraRuntime =
        binding.runtime(
            event -> CompletableFuture.completedFuture(null),
            "pekko-micronaut-binding-test-cassandra",
            ASK_TIMEOUT,
            "cassandra",
            CASSANDRA_RUNTIME_ENDPOINT,
            "",
            "",
            DATACENTER,
            "",
            "",
            0,
            8559,
            1,
            "");
  }

  @AfterAll
  static void stopRuntimes() {
    if (postgresRuntime != null) postgresRuntime.close();
    if (cassandraRuntime != null) cassandraRuntime.close();
  }

  // ---- contactPoint(String) - private static helper; same shape as the Cassandra endpoint the
  // outbox wiring below depends on, so a bad value here is exactly the kind of thing that used to
  // fail silently instead of loudly. ----

  @Test
  void parsesAWellFormedHostPortCassandraEndpoint() throws ReflectiveOperationException {
    InetSocketAddress address = contactPoint("127.0.0.1:9042");
    assertEquals(9042, address.getPort());
    assertEquals("127.0.0.1", address.getHostString());
  }

  @Test
  void usesTheLastColonSoAnIpv6ContactPointStillResolvesCorrectly()
      throws ReflectiveOperationException {
    // "0:0:0:0:0:0:0:1" is ::1 in expanded form - a realistic IPv6 Cassandra contact point, and
    // exactly the shape lastIndexOf(':') must handle correctly since the host itself contains
    // colons.
    InetSocketAddress address = contactPoint("0:0:0:0:0:0:0:1:9042");
    assertEquals(9042, address.getPort());
    assertFalse(address.isUnresolved());
    assertTrue(address.getAddress().isLoopbackAddress());
  }

  @Test
  void rejectsAnEndpointMissingAPortSeparatorInsteadOfSilentlyMisconfiguringTheContactPoint() {
    IllegalArgumentException failure =
        assertThrows(IllegalArgumentException.class, () -> contactPoint("cassandra-broker"));
    assertEquals("Cassandra endpoint must be host:port: cassandra-broker", failure.getMessage());
  }

  @Test
  void rejectsANonNumericPortInsteadOfSilentlyMisconfiguringTheContactPoint() {
    assertThrows(NumberFormatException.class, () -> contactPoint("127.0.0.1:notaport"));
  }

  // ---- runtime(...) - which PostgresConnectionSettings gets built for which profile ----

  @Test
  void buildsPostgresConnectionSettingsFromConfigOnlyForThePostgresqlProfile() {
    assertEquals(
        Optional.of(
            new PostgresConnectionSettings(
                POSTGRES_ENDPOINT, POSTGRES_USERNAME, POSTGRES_PASSWORD)),
        postgresRuntime.postgresConnection());
  }

  @Test
  void leavesPostgresConnectionSettingsEmptyForTheCassandraProfile() {
    assertEquals(Optional.empty(), cassandraRuntime.postgresConnection());
  }

  // ---- startEventing(...) - the wiring that was previously never started at all ----

  @Test
  @Timeout(30)
  void startingWithPostgresqlProfileConfigActuallyStartsTheTenantProjectionSupervisor()
      throws InterruptedException {
    AtomicInteger connectionAttempts = new AtomicInteger();
    DataSource dataSource = countingDataSource(connectionAttempts);

    CloudEventIngress ingress =
        binding.startEventing(
            postgresRuntime,
            // CloudEventPublisher/ExecutionQueryRepository/HibernateSessionSubworkflowPlanResolver
            // are only captured by the per-tenant TenantProjections lambdas startEventing() builds
            // for the Postgres branch, and those never fire here - this fake DataSource always
            // reports zero provisioned tenant schemas, so nothing ever dereferences these. Passing
            // null keeps the test from needing to fake three more collaborator types it never
            // actually exercises, and if a future change makes startEventing() dereference one of
            // these eagerly, this test starts failing loudly instead of silently no longer proving
            // what its name says it proves.
            null,
            null,
            null,
            dataSource,
            "postgresql",
            "",
            DATACENTER,
            ASK_TIMEOUT,
            RESCAN_INTERVAL);

    assertTrue(ingress instanceof CloudEventIngressGateway);

    long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
    while (connectionAttempts.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(
        connectionAttempts.get() > 0,
        "TenantProjectionSupervisor's periodic tenant-schema rescan never queried the DataSource"
            + " this binding gave it - the outbox/subscription projection wiring this class exists"
            + " to perform was not actually started");
  }

  @Test
  void aCassandraProfileEndpointMissingAPortSeparatorFailsFastInsteadOfNeverStartingTheOutbox() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                binding.startEventing(
                    cassandraRuntime,
                    null,
                    null,
                    null,
                    null, // DataSource - the Cassandra branch never touches it.
                    "cassandra",
                    "cassandra-broker-without-a-port",
                    DATACENTER,
                    ASK_TIMEOUT,
                    RESCAN_INTERVAL));
    assertEquals(
        "Cassandra endpoint must be host:port: cassandra-broker-without-a-port",
        failure.getMessage());
  }

  @Test
  @Timeout(30)
  void aPostgresqlIngressCallFailsFastWhenItsRuntimeWasBuiltWithoutPostgresConnectionSettings() {
    // Defends the exact mismatch this class's own orElseThrow guards against: a runtime built for
    // one profile fed to startEventing() configured for the other. Without that guard this would
    // be a confusing NullPointerException deep inside the outbox wiring instead of a clear,
    // actionable failure at the boundary - exactly the "silent" failure shape this whole class
    // exists to avoid.
    //
    // Deliberately builds its own runtime rather than reusing the shared cassandraRuntime field:
    // startEventing() unconditionally calls ScheduledExecutionDispatcher.spawn(system, workflows)
    // before it ever reaches the postgres/cassandra branch this test exercises, and that spawns a
    // fixed-named systemActorOf singleton - safe once per runtime (matching real production usage,
    // where startEventing() is only ever invoked once per app lifecycle), but a second call
    // against the SAME shared runtime (as aCassandraProfileEndpointMissingA... above also does,
    // against the same cassandraRuntime) throws InvalidActorNameException before ever reaching the
    // orElseThrow this test means to prove - a test-isolation bug, not a production one.
    PekkoEngineRuntime mismatchedRuntime =
        binding.runtime(
            event -> CompletableFuture.completedFuture(null),
            "pekko-micronaut-binding-test-cassandra-mismatch",
            ASK_TIMEOUT,
            "cassandra",
            CASSANDRA_RUNTIME_ENDPOINT,
            "",
            "",
            DATACENTER,
            "",
            "",
            0,
            8560,
            1,
            "");
    try {
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  binding.startEventing(
                      mismatchedRuntime,
                      null,
                      null,
                      null,
                      countingDataSource(new AtomicInteger()),
                      "postgresql",
                      "",
                      DATACENTER,
                      ASK_TIMEOUT,
                      RESCAN_INTERVAL));
      assertEquals(
          "Postgres persistence profile requires connection settings", failure.getMessage());
    } finally {
      mismatchedRuntime.close();
    }
  }

  // ---- test doubles ----

  private static InetSocketAddress contactPoint(String endpoint)
      throws ReflectiveOperationException {
    Method method =
        PekkoEngineMicronautBinding.class.getDeclaredMethod("contactPoint", String.class);
    method.setAccessible(true);
    try {
      return (InetSocketAddress) method.invoke(null, endpoint);
    } catch (InvocationTargetException wrapped) {
      if (wrapped.getCause() instanceof RuntimeException runtimeFailure) {
        throw runtimeFailure;
      }
      throw wrapped;
    }
  }

  private static DataSource countingDataSource(AtomicInteger connectionAttempts) {
    return newProxy(
        DataSource.class,
        (proxyObj, method, args) -> {
          if ("getConnection".equals(method.getName()) && method.getParameterCount() == 0) {
            connectionAttempts.incrementAndGet();
            return emptyConnection();
          }
          return handleObjectMethod(proxyObj, method, args, "FakeDataSource");
        });
  }

  private static Connection emptyConnection() {
    return newProxy(
        Connection.class,
        (proxyObj, method, args) -> {
          switch (method.getName()) {
            case "prepareStatement":
              return emptyPreparedStatement();
            case "close":
              return null;
            default:
              return handleObjectMethod(proxyObj, method, args, "FakeConnection");
          }
        });
  }

  private static PreparedStatement emptyPreparedStatement() {
    return newProxy(
        PreparedStatement.class,
        (proxyObj, method, args) -> {
          switch (method.getName()) {
            case "setString":
              return null;
            case "executeQuery":
              return emptyResultSet();
            case "close":
              return null;
            default:
              return handleObjectMethod(proxyObj, method, args, "FakePreparedStatement");
          }
        });
  }

  private static ResultSet emptyResultSet() {
    return newProxy(
        ResultSet.class,
        (proxyObj, method, args) -> {
          switch (method.getName()) {
            case "next":
              return false;
            case "close":
              return null;
            default:
              return handleObjectMethod(proxyObj, method, args, "FakeResultSet");
          }
        });
  }

  /** Handles the Object methods every JDK dynamic proxy routes through its handler regardless. */
  private static Object handleObjectMethod(
      Object proxyObj, Method method, Object[] args, String label) {
    return switch (method.getName()) {
      case "toString" -> label;
      case "hashCode" -> System.identityHashCode(proxyObj);
      case "equals" -> proxyObj == args[0];
      default ->
          throw new UnsupportedOperationException(
              label
                  + " does not stub "
                  + method.getName()
                  + " - the real code under test should not need it");
    };
  }

  @SuppressWarnings("unchecked")
  private static <T> T newProxy(Class<T> type, InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
  }
}
