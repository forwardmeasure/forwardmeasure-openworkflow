package com.forwardmeasure.openworkflow.actor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.actor.BootstrapSetup;
import org.apache.pekko.actor.setup.ActorSystemSetup;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap;
import org.apache.pekko.management.javadsl.LivenessCheckSetup;
import org.apache.pekko.management.javadsl.PekkoManagement;
import org.apache.pekko.management.javadsl.ReadinessCheckSetup;

/** Configures local self-join or DNS-based Kubernetes Cluster Bootstrap. */
public final class PekkoClusterRuntime {
  private static final String WIRE_SERIALIZER = "openworkflow-wire";
  private static final List<String> WIRE_PROTOCOL_TYPES =
      List.of(
          "com.forwardmeasure.openworkflow.actor.WorkflowCommand",
          "com.forwardmeasure.openworkflow.engine.api.EngineEvent",
          "com.forwardmeasure.openworkflow.actor.WorkflowReply",
          "com.forwardmeasure.openworkflow.actor.WorkflowState",
          "com.forwardmeasure.openworkflow.actor.ScheduleCommand",
          "com.forwardmeasure.openworkflow.actor.ScheduleEvent",
          "com.forwardmeasure.openworkflow.actor.ScheduleReply",
          "com.forwardmeasure.openworkflow.actor.ScheduleState",
          "com.forwardmeasure.openworkflow.actor.ScheduledExecutionRequest",
          "com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorCommand",
          "com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorEvent",
          "com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorReply",
          "com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorState",
          "com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorCommand",
          "com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorReply");

  private PekkoClusterRuntime() {}

  public static Config configure(Config base, Settings settings) {
    Objects.requireNonNull(base, "base");
    Objects.requireNonNull(settings, "settings");
    Config configured =
        withWireProtocol(base)
            .withValue("pekko.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
            .withValue(
                "pekko.remote.artery.canonical.port",
                ConfigValueFactory.fromAnyRef(settings.arteryPort()))
            .withValue(
                "pekko.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef("0.0.0.0"))
            .withValue(
                "pekko.remote.artery.bind.port",
                ConfigValueFactory.fromAnyRef(settings.arteryPort()))
            .withValue(
                "pekko.coordinated-shutdown.run-by-jvm-shutdown-hook",
                ConfigValueFactory.fromAnyRef(true))
            .withValue(
                "pekko.coordinated-shutdown.run-by-actor-system-terminate",
                ConfigValueFactory.fromAnyRef(true))
            // Cluster Bootstrap discovers members; it does not decide which
            // side may continue after a partition. Keep the majority side
            // and shut down the minority before sharded persistent actors
            // can be re-created there.
            .withValue(
                "pekko.cluster.downing-provider-class",
                ConfigValueFactory.fromAnyRef(
                    "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"))
            .withValue(
                "pekko.cluster.split-brain-resolver.active-strategy",
                ConfigValueFactory.fromAnyRef("keep-majority"))
            .withValue(
                "pekko.cluster.split-brain-resolver.stable-after",
                ConfigValueFactory.fromAnyRef(Duration.ofSeconds(20)));
    if (!settings.role().isEmpty()) {
      configured =
          configured.withValue(
              "pekko.cluster.roles", ConfigValueFactory.fromIterable(List.of(settings.role())));
    }
    if (!settings.clusterBootstrap()) return configured.resolve();
    Config bootstrap =
        configured
            .withValue(
                "pekko.remote.artery.canonical.hostname",
                ConfigValueFactory.fromAnyRef(settings.podIp()))
            .withValue(
                "pekko.management.http.hostname", ConfigValueFactory.fromAnyRef(settings.podIp()))
            .withValue(
                "pekko.management.http.bind-hostname", ConfigValueFactory.fromAnyRef("0.0.0.0"))
            .withValue(
                "pekko.management.http.port",
                ConfigValueFactory.fromAnyRef(settings.managementPort()))
            .withValue("pekko.discovery.method", ConfigValueFactory.fromAnyRef("pekko-dns"))
            .withValue(
                "pekko.management.cluster.bootstrap.contact-point-discovery.service-name",
                ConfigValueFactory.fromAnyRef(settings.discoveryService()))
            .withValue(
                "pekko.management.cluster.bootstrap.contact-point-discovery.port-name",
                ConfigValueFactory.fromAnyRef("management"))
            .withValue(
                "pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr",
                ConfigValueFactory.fromAnyRef(settings.requiredContactPoints()));
    List<String> nameservers = nameservers(Path.of("/etc/resolv.conf"));
    if (!nameservers.isEmpty()) {
      bootstrap =
          bootstrap.withValue(
              "pekko.io.dns.async-dns.nameservers", ConfigValueFactory.fromIterable(nameservers));
    }
    return bootstrap.resolve();
  }

  /*
   * Apply wire bindings at runtime as well as in reference.conf. Executable
   * jars merge dependency reference files in framework-specific orders; a
   * later Pekko default must never silently replace the workflow protocol
   * bindings and make remotely routed control commands time out.
   */
  static Config withWireProtocol(Config base) {
    Config configured =
        base.withValue(
                "pekko.actor.serializers." + WIRE_SERIALIZER,
                ConfigValueFactory.fromAnyRef(
                    "com.forwardmeasure.openworkflow.actor.serialization.OpenWorkflowWireSerializer"))
            .withValue(
                "pekko.serialization.jackson.allowed-class-prefix",
                ConfigValueFactory.fromIterable(List.of("com.forwardmeasure.openworkflow.")));
    for (String protocolType : WIRE_PROTOCOL_TYPES) {
      configured =
          configured.withValue(
              "pekko.actor.serialization-bindings.\"" + protocolType + "\"",
              ConfigValueFactory.fromAnyRef(WIRE_SERIALIZER));
    }
    return configured;
  }

  /** Creates an actor system with framework-neutral cluster health checks. */
  public static <T> ActorSystem<T> create(Behavior<T> guardian, String name, Config config) {
    var readiness =
        ReadinessCheckSetup.create(
            classic ->
                List.of(
                    () -> CompletableFuture.completedFuture(isReady(ActorSystem.wrap(classic)))));
    var liveness =
        LivenessCheckSetup.create(
            classic ->
                List.of(
                    () -> CompletableFuture.completedFuture(isAlive(ActorSystem.wrap(classic)))));
    var setup = ActorSystemSetup.create(BootstrapSetup.create(config), readiness, liveness);
    return ActorSystem.create(guardian, name, setup);
  }

  static boolean isReady(ActorSystem<?> system) {
    MemberStatus status = Cluster.get(system).selfMember().status();
    return status.equals(MemberStatus.up()) || status.equals(MemberStatus.weaklyUp());
  }

  static boolean isAlive(ActorSystem<?> system) {
    return !Cluster.get(system).isTerminated();
  }

  public static void start(ActorSystem<?> system, Settings settings) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(settings, "settings");
    if (!settings.clusterBootstrap()) {
      Cluster cluster = Cluster.get(system);
      cluster.manager().tell(Join.create(cluster.selfMember().address()));
      return;
    }
    try {
      // A runtime is not started until its management endpoint is bound. This keeps framework
      // startup and Kubernetes health deterministic and surfaces binding/configuration failures.
      PekkoManagement.get(system).start().toCompletableFuture().join();
    } catch (RuntimeException startupFailure) {
      system.terminate();
      throw startupFailure;
    }
    ClusterBootstrap.get(system).start();
  }

  public record Settings(
      String discoveryService,
      String podIp,
      int arteryPort,
      int managementPort,
      int requiredContactPoints,
      String role) {
    public Settings(
        String discoveryService,
        String podIp,
        int arteryPort,
        int managementPort,
        int requiredContactPoints) {
      this(discoveryService, podIp, arteryPort, managementPort, requiredContactPoints, "");
    }

    public Settings {
      discoveryService = Objects.requireNonNullElse(discoveryService, "").trim();
      podIp = Objects.requireNonNullElse(podIp, "").trim();
      role = Objects.requireNonNullElse(role, "").trim();
      if (arteryPort < 0 || arteryPort > 65_535) {
        throw new IllegalArgumentException("arteryPort must be between 0 and 65535");
      }
      if (managementPort < 1 || managementPort > 65_535) {
        throw new IllegalArgumentException("managementPort must be between 1 and 65535");
      }
      if (requiredContactPoints < 1) {
        throw new IllegalArgumentException("requiredContactPoints must be positive");
      }
      if (!discoveryService.isEmpty() && podIp.isEmpty()) {
        throw new IllegalArgumentException("podIp is required when discovery is enabled");
      }
      if (role.contains(",") || role.chars().anyMatch(Character::isWhitespace)) {
        throw new IllegalArgumentException("role must be one Pekko role name");
      }
    }

    public boolean clusterBootstrap() {
      return !discoveryService.isEmpty();
    }
  }

  static List<String> nameservers(Path resolvConf) {
    try (var lines = Files.lines(resolvConf)) {
      return lines
          .map(String::trim)
          .filter(line -> line.startsWith("nameserver "))
          .map(line -> line.substring("nameserver ".length()).trim())
          .filter(value -> !value.isBlank())
          .distinct()
          .toList();
    } catch (IOException unavailable) {
      return List.of();
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
