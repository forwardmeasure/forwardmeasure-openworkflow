/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.quarkus;

import static org.junit.jupiter.api.Assertions.assertFalse;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves that {@link OperationAdapterQuarkusBinding}'s {@code runtime} (Kafka) producer actually
 * gets constructed and started as part of normal Quarkus application boot - not merely that it
 * compiles.
 *
 * <p>This is the operation-adapter analogue of the confirmed-and-fixed {@code
 * PekkoEngineQuarkusBinding} bug: CDI producer methods for normal-scoped ({@code
 * ApplicationScoped}) beans are lazily constructed - the container never runs the producer body
 * unless some other bean injects the produced type, or a startup observer forces it. Nothing in
 * this deployment module injects {@link
 * com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime} anywhere, so if this
 * binding lacked an eager trigger, {@code runtime.start()} - and therefore the whole tenant-facing
 * HTTP/protocol operation outbox - would never run on a real deployment, exactly like the pre-fix
 * {@code CloudEventIngress} gap.
 *
 * <p>Rather than injecting the bean from the test (which would force its creation for reasons that
 * have nothing to do with production wiring), this test observes the effect from entirely outside
 * the JVM: {@link
 * com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterDispatcher#start()} spawns
 * consumer threads that join a real Kafka consumer group as soon as they run. A raw {@code Admin}
 * client against the real Redpanda broker used for this test - independent of Quarkus's own
 * dependency injection - can see whether that group ever shows up.
 */
@QuarkusTest
@QuarkusTestResource(OperationAdapterQuarkusBindingTest.KafkaAdapterResource.class)
class OperationAdapterQuarkusBindingTest {

  @Test
  void kafkaOperationAdapterRuntimeStartsAutomaticallyOnBoot() throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaAdapterResource.bootstrapServers))) {
      Set<String> matching = pollForConsumerGroup(admin, KafkaAdapterResource.consumerGroup);
      assertFalse(
          matching.isEmpty(),
          "Expected a Kafka consumer group whose id starts with '"
              + KafkaAdapterResource.consumerGroup
              + "' to appear on the broker within the deadline. None did, which means"
              + " OperationAdapterQuarkusBinding.runtime()'s KafkaOperationAdapterRuntime producer"
              + " was never actually invoked by CDI during application boot (a lazily-constructed,"
              + " never-injected @ApplicationScoped producer bean) - the exact same silently-inert"
              + " shape as the pre-fix PekkoEngineQuarkusBinding CloudEventIngress bug. Tenant"
              + " HTTP/protocol operation outboxes for this deployment would never start in"
              + " production.");
    }
  }

  private static Set<String> pollForConsumerGroup(Admin admin, String consumerGroupPrefix)
      throws Exception {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(45));
    Set<String> matching = Set.of();
    while (Instant.now().isBefore(deadline)) {
      List<GroupListing> groups = List.copyOf(admin.listGroups().all().get());
      matching =
          groups.stream()
              .map(GroupListing::groupId)
              .filter(id -> id.startsWith(consumerGroupPrefix))
              .collect(Collectors.toUnmodifiableSet());
      if (!matching.isEmpty()) {
        return matching;
      }
      Thread.sleep(500);
    }
    return matching;
  }

  /**
   * Points the app at a real, per-test Redpanda broker instead of the default localhost:9092, and
   * disables the Pekko adapter runtime - its outbox wiring needs a live Postgres/Cassandra cluster
   * this test doesn't have, and {@link
   * com.forwardmeasure.openworkflow.adapter.pekko.PekkoOperationAdapterRuntime} no-ops cleanly when
   * disabled, so this test's scope stays limited to the Kafka producer's eager-start wiring.
   */
  public static final class KafkaAdapterResource implements QuarkusTestResourceLifecycleManager {
    static volatile String bootstrapServers;
    static volatile String consumerGroup;

    private RedpandaContainer redpanda;

    @Override
    public Map<String, String> start() {
      redpanda =
          new RedpandaContainer(
              DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));
      redpanda.start();
      bootstrapServers = redpanda.getBootstrapServers();
      String suffix = UUID.randomUUID().toString();
      String topicPrefix = "adapter-quarkus-test-" + suffix;
      consumerGroup = "adapter-quarkus-test-group-" + suffix;
      createTopics(topicPrefix);

      Map<String, String> overrides = new HashMap<>();
      overrides.put("openworkflow.kafka.bootstrap-servers", bootstrapServers);
      overrides.put("openworkflow.kafka.topic-prefix", topicPrefix);
      overrides.put("openworkflow.adapters.consumer-group", consumerGroup);
      overrides.put("openworkflow.adapters.instance-id", "quarkus-test");
      overrides.put("openworkflow.adapters.pekko-enabled", "false");
      return overrides;
    }

    private void createTopics(String topicPrefix) {
      try (Admin admin =
          Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
        List<NewTopic> topics =
            Stream.of(
                    topicPrefix + ".effects",
                    topicPrefix + ".definitions",
                    topicPrefix + ".commands",
                    topicPrefix + ".dead-letters",
                    topicPrefix + ".operation-checkpoints")
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();
        admin.createTopics(topics).all().get();
      } catch (Exception failure) {
        throw new IllegalStateException("Unable to create test Kafka topics", failure);
      }
    }

    @Override
    public void stop() {
      if (redpanda != null) {
        redpanda.stop();
      }
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
