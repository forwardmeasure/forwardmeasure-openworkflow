/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.spring;

import static org.junit.jupiter.api.Assertions.assertFalse;

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
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves that {@link OperationAdapterSpringBinding}'s {@code runtime} (Kafka) {@code @Bean}
 * actually gets constructed and started as part of normal Spring Boot application boot.
 *
 * <p>Unlike CDI producer methods (see the Quarkus sibling of this test), Spring {@code @Bean}
 * methods on a non-lazy {@code @Configuration} class are eagerly instantiated during context
 * refresh by default, and {@code runtime} is declared with {@code @Bean(initMethod = "start", ...)}
 * so nothing extra should be required here - but that is exactly the kind of assumption the sibling
 * Pekko-engine bug (a framework binding that built correct production code but never actually
 * started it) showed cannot be taken on faith. This test verifies the real, observable effect from
 * entirely outside the JVM and outside Spring's own container: once {@link
 * com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterDispatcher#start()} runs, it
 * spawns consumer threads that join a real Kafka consumer group. A raw {@code Admin} client against
 * the real Redpanda broker used for this test - independent of Spring dependency injection - can
 * see whether that group ever shows up.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class OperationAdapterSpringBindingTest {
  @Container
  static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2"));

  private static final String SUFFIX = UUID.randomUUID().toString();
  private static final String TOPIC_PREFIX = "adapter-spring-test-" + SUFFIX;
  private static final String CONSUMER_GROUP = "adapter-spring-test-group-" + SUFFIX;

  @DynamicPropertySource
  static void register(DynamicPropertyRegistry registry) {
    createTopics();
    registry.add("openworkflow.kafka.bootstrap-servers", REDPANDA::getBootstrapServers);
    registry.add("openworkflow.kafka.topic-prefix", () -> TOPIC_PREFIX);
    registry.add("openworkflow.adapters.consumer-group", () -> CONSUMER_GROUP);
    registry.add("openworkflow.adapters.instance-id", () -> "spring-test");
    // Pekko's outbox wiring needs a live Postgres/Cassandra cluster this test doesn't have;
    // PekkoOperationAdapterRuntime no-ops cleanly when disabled (see its `enabled()` check), so
    // this test's scope stays limited to the Kafka producer's start-up wiring.
    registry.add("openworkflow.adapters.pekko-enabled", () -> "false");
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
              + " OperationAdapterSpringBinding.runtime()'s KafkaOperationAdapterRuntime bean was"
              + " never actually constructed and started during application boot - the same"
              + " silently-inert shape as the confirmed, since-fixed Pekko-engine binding bug."
              + " Tenant HTTP/protocol operation outboxes for this deployment would never start in"
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
