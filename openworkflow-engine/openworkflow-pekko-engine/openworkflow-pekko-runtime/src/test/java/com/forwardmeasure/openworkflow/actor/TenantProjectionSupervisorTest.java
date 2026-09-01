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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link TenantProjectionSupervisor} periodically re-scans for newly-provisioned tenant schemas
 * (via {@code ProvisionedTenantSchemas.scan}, a plain JDBC query) and starts every registered
 * {@link TenantProjectionSupervisor.TenantProjections} exactly once per newly-discovered schema.
 * That scan is not an injected collaborator - it is a direct static JDBC call inside {@code
 * TenantProjectionSupervisor.start} - so these tests exercise it through a hand-rolled {@link
 * DataSource}/{@link java.sql.Connection}/{@link java.sql.PreparedStatement}/{@link
 * java.sql.ResultSet} test double (this module has neither Mockito nor an embedded database as a
 * test dependency), built with {@link Proxy} so only the handful of JDBC methods the real scan
 * query actually calls need behavior.
 */
class TenantProjectionSupervisorTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final PostgresConnectionSettings CONNECTION =
      new PostgresConnectionSettings("jdbc:postgresql://tenant-host:5432/openworkflow", "u", "p");

  private static ActorTestKit actors;

  @BeforeAll
  static void start() {
    actors = ActorTestKit.create();
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  @Test
  void startsProjectionsOnceForEachNewlyDiscoveredSchemaAndSkipsThemOnLaterRescans()
      throws InterruptedException {
    TenantSchema schemaA = tenantSchema("a");
    TenantSchema schemaB = tenantSchema("b");
    // Every tick re-discovers the exact same two already-known schemas - a real re-scan interval
    // firing many times over, exactly like production.
    DataSource dataSource = dataSourceAlwaysReturning(schemaA, schemaB);

    var firstProjectionCalls = new CopyOnWriteArrayList<StartCall>();
    var secondProjectionCalls = new CopyOnWriteArrayList<StartCall>();
    TenantProjectionSupervisor.TenantProjections firstProjection =
        (system, ds, schema, connection) ->
            firstProjectionCalls.add(new StartCall(system, ds, schema, connection));
    TenantProjectionSupervisor.TenantProjections secondProjection =
        (system, ds, schema, connection) ->
            secondProjectionCalls.add(new StartCall(system, ds, schema, connection));

    TenantProjectionSupervisor.start(
        actors.system(),
        dataSource,
        CONNECTION,
        Duration.ofMillis(30),
        List.of(firstProjection, secondProjection));

    // Give the fixed-delay scheduler several ticks' worth of time to prove dedup holds across
    // repeated rescans, not just within the first one.
    Thread.sleep(400);

    assertEquals(
        Set.of(schemaA, schemaB),
        firstProjectionCalls.stream()
            .map(StartCall::schema)
            .collect(java.util.stream.Collectors.toSet()));
    assertEquals(
        2,
        firstProjectionCalls.size(),
        "each discovered schema starts this projection exactly once");
    assertEquals(
        2,
        secondProjectionCalls.size(),
        "every registered projection is started, not just the first");

    StartCall call = firstProjectionCalls.get(0);
    assertSame(actors.system(), call.system());
    assertSame(dataSource, call.dataSource());
    assertSame(CONNECTION, call.connection());
  }

  @Test
  void aFailedScanIsLoggedAndSkippedButLaterScansStillStartNewlyDiscoveredSchemas()
      throws InterruptedException {
    TenantSchema schema = tenantSchema("c");
    // The first several rescans blow up (e.g. a transient DB outage); only once scans start
    // succeeding does the schema get discovered.
    DataSource dataSource = dataSourceFailingThenReturning(3, schema);

    var startedSchemas = new CopyOnWriteArrayList<TenantSchema>();
    TenantProjectionSupervisor.TenantProjections projection =
        (system, ds, discoveredSchema, connection) -> startedSchemas.add(discoveredSchema);

    TenantProjectionSupervisor.start(
        actors.system(), dataSource, CONNECTION, Duration.ofMillis(30), List.of(projection));

    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (startedSchemas.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    assertEquals(
        List.of(schema),
        startedSchemas,
        "the supervisor must keep ticking after a failed scan and start the schema once a later"
            + " scan actually succeeds");

    // Give it more time to re-confirm the failure-then-success sequence didn't also leave it
    // double-starting the same schema on subsequent successful rescans.
    Thread.sleep(200);
    assertEquals(1, startedSchemas.size());
  }

  private static TenantSchema tenantSchema(String marker) {
    return new TenantSchema("t_" + marker.repeat(32));
  }

  private record StartCall(
      org.apache.pekko.actor.typed.ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection) {}

  /** One JDBC round-trip's outcome: either a list of discovered schema names, or a failure. */
  @FunctionalInterface
  private interface ScanAttempt {
    List<String> schemaNames() throws SQLException;
  }

  private static DataSource dataSourceAlwaysReturning(TenantSchema... schemas) {
    List<String> names = java.util.Arrays.stream(schemas).map(TenantSchema::value).toList();
    ScanAttempt attempt = () -> names;
    return sequencedDataSource(List.of(attempt));
  }

  private static DataSource dataSourceFailingThenReturning(int failureCount, TenantSchema schema) {
    List<ScanAttempt> attempts = new java.util.ArrayList<>();
    for (int i = 0; i < failureCount; i++) {
      attempts.add(
          () -> {
            throw new SQLException("simulated transient scan failure");
          });
    }
    attempts.add(() -> List.of(schema.value()));
    return sequencedDataSource(attempts);
  }

  /**
   * A fake {@link DataSource} that replays one {@link ScanAttempt} per call, clamping past the end.
   */
  private static DataSource sequencedDataSource(List<ScanAttempt> attempts) {
    AtomicInteger callCount = new AtomicInteger();
    return newProxy(
        DataSource.class,
        (proxyObj, method, args) -> {
          if ("getConnection".equals(method.getName()) && method.getParameterCount() == 0) {
            int index = Math.min(callCount.getAndIncrement(), attempts.size() - 1);
            return fakeConnection(attempts.get(index));
          }
          return handleObjectMethod(proxyObj, method, args, "FakeDataSource");
        });
  }

  private static java.sql.Connection fakeConnection(ScanAttempt attempt) {
    return newProxy(
        java.sql.Connection.class,
        (proxyObj, method, args) -> {
          if ("prepareStatement".equals(method.getName())) {
            return fakePreparedStatement(attempt);
          }
          if ("close".equals(method.getName())) {
            return null;
          }
          return handleObjectMethod(proxyObj, method, args, "FakeConnection");
        });
  }

  private static java.sql.PreparedStatement fakePreparedStatement(ScanAttempt attempt) {
    return newProxy(
        java.sql.PreparedStatement.class,
        (proxyObj, method, args) -> {
          switch (method.getName()) {
            case "setString":
              return null;
            case "executeQuery":
              return fakeResultSet(attempt.schemaNames());
            case "close":
              return null;
            default:
              return handleObjectMethod(proxyObj, method, args, "FakePreparedStatement");
          }
        });
  }

  private static java.sql.ResultSet fakeResultSet(List<String> schemaNames) {
    Iterator<String> iterator = schemaNames.iterator();
    var current = new java.util.concurrent.atomic.AtomicReference<String>();
    return newProxy(
        java.sql.ResultSet.class,
        (proxyObj, method, args) -> {
          switch (method.getName()) {
            case "next":
              if (iterator.hasNext()) {
                current.set(iterator.next());
                return true;
              }
              return false;
            case "getString":
              return current.get();
            case "close":
              return null;
            default:
              return handleObjectMethod(proxyObj, method, args, "FakeResultSet");
          }
        });
  }

  /** Handles the Object methods every JDK dynamic proxy routes through its handler regardless. */
  private static Object handleObjectMethod(
      Object proxyObj, java.lang.reflect.Method method, Object[] args, String label) {
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
