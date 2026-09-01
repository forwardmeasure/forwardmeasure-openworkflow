/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.adapter.pekko.PekkoOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksTopics;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.time.Duration;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class OperationAdapterQuarkusBinding {
  @Produces
  @ApplicationScoped
  AuthorizationService authorization(
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.authorization.issuer") URI issuer,
      @ConfigProperty(name = "openworkflow.authorization.client-id") String clientId,
      @ConfigProperty(name = "openworkflow.authorization.client-secret") String clientSecret,
      @ConfigProperty(name = "openworkflow.authorization.request-timeout") Duration requestTimeout,
      @ConfigProperty(name = "openworkflow.authorization.decision-ttl") Duration decisionTtl,
      @ConfigProperty(name = "openworkflow.authorization.maximum-cache-entries") int cacheEntries,
      @ConfigProperty(name = "openworkflow.authorization.policy-version") String policyVersion) {
    return AuthzenAuthorizationFactory.create(
        mapper,
        issuer,
        clientId,
        clientSecret,
        requestTimeout,
        decisionTtl,
        cacheEntries,
        policyVersion);
  }

  @Produces
  @ApplicationScoped
  PekkoOperationAdapterRuntime pekkoRuntime(
      AuthorizationService authorization, ObjectMapper mapper, Config config) {
    return new PekkoOperationAdapterRuntime(
        PekkoOperationAdapterRuntime.Configuration.from(
            name -> config.getOptionalValue(name, String.class).orElse(null)),
        authorization,
        mapper);
  }

  void close(@Disposes PekkoOperationAdapterRuntime runtime) {
    runtime.close();
  }

  @Produces
  @ApplicationScoped
  KafkaOperationAdapterRuntime runtime(
      AuthorizationService authorization,
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.kafka.bootstrap-servers") String bootstrap,
      @ConfigProperty(name = "openworkflow.kafka.topic-prefix") String prefix,
      @ConfigProperty(name = "openworkflow.adapters.consumer-group") String group,
      @ConfigProperty(name = "openworkflow.adapters.instance-id") String instance,
      @ConfigProperty(name = "openworkflow.adapters.secret-directory") String secrets,
      @ConfigProperty(name = "openworkflow.adapters.http-egress-allowlist", defaultValue = "")
          String allowlist,
      @ConfigProperty(name = "openworkflow.operations.protocol.timeout-ms", defaultValue = "30000")
          long timeout,
      @ConfigProperty(name = "openworkflow.operations.mcp-command-allowlist", defaultValue = "")
          String mcp,
      @ConfigProperty(name = "openworkflow.operations.run.command-allowlist", defaultValue = "")
          String commands,
      @ConfigProperty(name = "openworkflow.operations.run.interpreter-allowlist", defaultValue = "")
          String interpreters,
      @ConfigProperty(name = "openworkflow.operations.run.image-allowlist", defaultValue = "")
          String images,
      @ConfigProperty(name = "openworkflow.operations.run.volume-allowlist", defaultValue = "")
          String volumes,
      @ConfigProperty(name = "openworkflow.operations.run.port-allowlist", defaultValue = "")
          String ports,
      @ConfigProperty(name = "openworkflow.operations.run.oci-runtime", defaultValue = "podman")
          String oci) {
    OksTopics topics = OksTopics.withPrefix(prefix);
    var runtime =
        new KafkaOperationAdapterRuntime(
            bootstrap,
            topics.effects(),
            topics.definitions(),
            topics.commands(),
            topics.deadLetters(),
            group,
            instance,
            authorization,
            mapper,
            secrets,
            allowlist,
            KafkaProtocolOperationExecutors.create(
                authorization,
                mapper,
                new KafkaProtocolOperationExecutors.Configuration(
                    timeout,
                    allowlist,
                    secrets,
                    mcp,
                    commands,
                    interpreters,
                    images,
                    volumes,
                    ports,
                    oci)));
    runtime.start();
    return runtime;
  }

  void close(@Disposes KafkaOperationAdapterRuntime runtime) {
    runtime.close();
  }

  /**
   * CDI producer beans are lazily constructed by default - without something forcing it, neither
   * {@code pekkoRuntime} nor {@code runtime} above would ever actually run unless some other bean
   * happens to depend on them first, and nothing else in this deployment does (verified by grepping
   * the whole deployable surface for production, non-test callers of {@code
   * PekkoOperationAdapterRuntime}/{@code KafkaOperationAdapterRuntime} - there were none, anywhere,
   * before this). Until this observer forced them, this binding built correct production wiring -
   * the per-tenant-schema Postgres outboxes started by {@code PekkoOperationAdapterRuntime} and the
   * Kafka consumer threads started by {@code KafkaOperationAdapterRuntime.start()} - that was never
   * actually reachable on a real Quarkus deployment, the exact same silently-inert shape as the
   * pre-fix {@code PekkoEngineQuarkusBinding.CloudEventIngress} gap (see {@code
   * eagerlyStartEventing} there). This is that forcing trigger: a genuine observer method (no
   * {@code @Produces}) with both runtimes as ordinary injected parameters.
   */
  void eagerlyStartAdapters(
      @Observes StartupEvent event,
      PekkoOperationAdapterRuntime pekko,
      KafkaOperationAdapterRuntime kafka) {}
}
