/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.micronaut;

import static org.junit.jupiter.api.Assertions.assertFalse;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import java.time.Duration;
import java.time.Instant;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves that {@link OperationAdapterMicronautBinding}'s {@code runtime} (Kafka) {@code @Singleton}
 * factory method actually gets constructed and started as part of normal Micronaut application boot
 * - not merely that it compiles.
 *
 * <p>This is the operation-adapter analogue of the confirmed-and-fixed Pekko-engine binding bug (a
 * framework binding that built correct production code but never actually started it): Micronaut,
 * like CDI, does not eagerly initialize plain {@code @Singleton} beans by default - only beans
 * annotated {@code @Context}, beans implementing {@code ApplicationEventListener}, or beans some
 * other eagerly-initialized bean actually depends on. Nothing in this deployment module injects
 * {@link com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime} anywhere, so
 * if this binding lacks an eager trigger, {@code runtime.start()} - and therefore the tenant-facing
 * HTTP/protocol operation outbox - would never run on a real deployment.
 *
 * <p>Rather than injecting the bean from the test (which would force its creation for reasons that
 * have nothing to do with production wiring), this test observes the effect from entirely outside
 * the JVM: {@link
 * com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterDispatcher#start()} spawns
 * consumer threads that join a real Kafka consumer group as soon as they run. A raw {@code Admin}
 * client against the real Redpanda broker used for this test - independent of Micronaut's own
 * dependency injection - can see whether that group ever shows up.
 */
@MicronautTest
class OperationAdapterMicronautBindingTest implements TestPropertyProvider {
  private static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));
  private static final String SUFFIX = UUID.randomUUID().toString();
  private static final String TOPIC_PREFIX = "adapter-micronaut-test-" + SUFFIX;
  private static final String CONSUMER_GROUP = "adapter-micronaut-test-group-" + SUFFIX;

  /**
   * Called by the Micronaut JUnit5 extension before the application context is built, so the
   * container must be started (and the topics it needs created) here rather than in a JUnit
   * lifecycle callback.
   */
  @Override
  public Map<String, String> getProperties() {
    REDPANDA.start();
    createTopics();
    return Map.of(
        "openworkflow.kafka.bootstrap-servers",
        REDPANDA.getBootstrapServers(),
        "openworkflow.kafka.topic-prefix",
        TOPIC_PREFIX,
        "openworkflow.adapters.consumer-group",
        CONSUMER_GROUP,
        "openworkflow.adapters.instance-id",
        "micronaut-test",
        // Pekko's outbox wiring needs a live Postgres/Cassandra cluster this test doesn't have;
        // PekkoOperationAdapterRuntime no-ops cleanly when disabled (see its `enabled()` check),
        // so this test's scope stays limited to the Kafka producer's start-up wiring.
        "openworkflow.adapters.pekko-enabled",
        "false");
  }

  private static void createTopics() {
    try (Admin admin =
        Admin.create(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers()))) {
      List<NewTopic> topics =
          Stream.of(
                  TOPIC_PREFIX + ".effects",
                  TOPIC_PREFIX + ".definitions",
                  TOPIC_PREFIX + ".commands",
                  TOPIC_PREFIX + ".dead-letters",
                  TOPIC_PREFIX + ".operation-checkpoints")
              .map(name -> new NewTopic(name, 1, (short) 1))
              .toList();
      admin.createTopics(topics).all().get();
    } catch (Exception failure) {
      throw new IllegalStateException("Unable to create test Kafka topics", failure);
    }
  }

  @AfterAll
  static void stopBroker() {
    REDPANDA.stop();
  }

  @Test
  void kafkaOperationAdapterRuntimeStartsAutomaticallyOnBoot() throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers()))) {
      Set<String> matching = pollForConsumerGroup(admin, CONSUMER_GROUP);
      assertFalse(
          matching.isEmpty(),
          "Expected a Kafka consumer group whose id starts with '"
              + CONSUMER_GROUP
              + "' to appear on the broker within the deadline. None did, which means"
              + " OperationAdapterMicronautBinding.runtime()'s KafkaOperationAdapterRuntime bean"
              + " was never actually constructed and started during application boot (a plain"
              + " @Singleton factory method that nothing else in this deployment injects, so"
              + " Micronaut never has a reason to eagerly build it) - the same silently-inert shape"
              + " as the confirmed, since-fixed Pekko-engine binding bug. Tenant HTTP/protocol"
              + " operation outboxes for this deployment would never start in production.");
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
