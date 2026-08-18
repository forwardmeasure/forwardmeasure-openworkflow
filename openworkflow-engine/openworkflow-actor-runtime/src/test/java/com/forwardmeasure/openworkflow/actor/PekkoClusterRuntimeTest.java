package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PekkoClusterRuntimeTest {
  @Test
  void configureAlwaysInstallsTheWorkflowWireProtocol() {
    var config =
        PekkoClusterRuntime.configure(
            ConfigFactory.load(), new PekkoClusterRuntime.Settings("", "", 0, 8558, 1));

    assertEquals(
        "com.forwardmeasure.openworkflow.actor.serialization.OpenWorkflowWireSerializer",
        config.getString("pekko.actor.serializers.openworkflow-wire"));
    assertEquals(
        "openworkflow-wire",
        config.getString(
            "pekko.actor.serialization-bindings.\"com.forwardmeasure.openworkflow.actor.WorkflowCommand\""));
    assertTrue(
        config
            .getStringList("pekko.serialization.jackson.allowed-class-prefix")
            .contains("com.forwardmeasure.openworkflow."));
  }

  @TempDir Path temporaryDirectory;

  @Test
  void configuresLocalDynamicPortWithoutBootstrap() {
    var settings = new PekkoClusterRuntime.Settings("", "", 0, 8558, 1);
    var config = PekkoClusterRuntime.configure(ConfigFactory.load(), settings);

    assertFalse(settings.clusterBootstrap());
    assertEquals("cluster", config.getString("pekko.actor.provider"));
    assertEquals(0, config.getInt("pekko.remote.artery.canonical.port"));
    assertEquals("0.0.0.0", config.getString("pekko.remote.artery.bind.hostname"));
  }

  @Test
  void configuresDnsBootstrapWithPodCanonicalAddress() {
    var settings =
        new PekkoClusterRuntime.Settings(
            "release-openworkflow-actor-engine-discovery", "10.0.0.17", 25520, 8558, 2);
    var config = PekkoClusterRuntime.configure(ConfigFactory.load(), settings);

    assertTrue(settings.clusterBootstrap());
    assertEquals("10.0.0.17", config.getString("pekko.remote.artery.canonical.hostname"));
    assertEquals("10.0.0.17", config.getString("pekko.management.http.hostname"));
    assertEquals("0.0.0.0", config.getString("pekko.management.http.bind-hostname"));
    assertEquals(25520, config.getInt("pekko.remote.artery.canonical.port"));
    assertEquals("pekko-dns", config.getString("pekko.discovery.method"));
    assertEquals(
        "release-openworkflow-actor-engine-discovery",
        config.getString(
            "pekko.management.cluster.bootstrap.contact-point-discovery.service-name"));
    assertEquals(
        "management",
        config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.port-name"));
    assertEquals(
        2,
        config.getInt(
            "pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr"));
  }

  @Test
  void rejectsDiscoveryWithoutCanonicalPodIp() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PekkoClusterRuntime.Settings("openworkflow-discovery", "", 25520, 8558, 2));
  }

  @Test
  void enablesCoordinatedShutdownForLocalAndClusterProcesses() {
    var settings = new PekkoClusterRuntime.Settings("", "", 0, 8558, 1);
    var config = PekkoClusterRuntime.configure(ConfigFactory.load(), settings);

    assertTrue(config.getBoolean("pekko.coordinated-shutdown.run-by-jvm-shutdown-hook"));
    assertTrue(config.getBoolean("pekko.coordinated-shutdown.run-by-actor-system-terminate"));
  }

  @Test
  void configuresAnExplicitKeepMajoritySplitBrainPolicy() {
    var config =
        PekkoClusterRuntime.configure(
            ConfigFactory.load(),
            new PekkoClusterRuntime.Settings(
                "openworkflow-discovery", "10.0.0.17", 25520, 8558, 3));

    assertEquals(
        "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider",
        config.getString("pekko.cluster.downing-provider-class"));
    assertEquals(
        "keep-majority", config.getString("pekko.cluster.split-brain-resolver.active-strategy"));
    assertEquals(
        java.time.Duration.ofSeconds(20),
        config.getDuration("pekko.cluster.split-brain-resolver.stable-after"));
  }

  @Test
  void assignsTheDedicatedOperationAdapterRoleOnlyWhenRequested() {
    var adapter =
        PekkoClusterRuntime.configure(
            ConfigFactory.load(),
            new PekkoClusterRuntime.Settings(
                "discovery", "10.0.0.18", 25520, 8558, 3, "operation-adapter"));
    var runtime =
        PekkoClusterRuntime.configure(
            ConfigFactory.load(),
            new PekkoClusterRuntime.Settings("discovery", "10.0.0.19", 25520, 8558, 3));

    assertEquals(List.of("operation-adapter"), adapter.getStringList("pekko.cluster.roles"));
    assertFalse(runtime.getStringList("pekko.cluster.roles").contains("operation-adapter"));
  }

  @Test
  void obtainsContainerNameserversWithoutJdkInternalReflection() throws Exception {
    Path resolv = temporaryDirectory.resolve("resolv.conf");
    Files.writeString(
        resolv,
        """
        search openworkflow.svc.cluster.local svc.cluster.local
        nameserver 10.96.0.10
        nameserver 10.96.0.10
        nameserver 2001:db8::53
        options ndots:5
        """);

    assertEquals(List.of("10.96.0.10", "2001:db8::53"), PekkoClusterRuntime.nameservers(resolv));
    assertTrue(PekkoClusterRuntime.nameservers(temporaryDirectory.resolve("missing")).isEmpty());
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
