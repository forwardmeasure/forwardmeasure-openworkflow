/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.adapter.pekko.PekkoOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksTopics;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;

@Factory
public class OperationAdapterMicronautBinding {
  @Singleton
  AuthorizationService authorization(
      ObjectMapper mapper,
      @Value("${openworkflow.authorization.issuer}") URI issuer,
      @Value("${openworkflow.authorization.client-id}") String id,
      @Value("${openworkflow.authorization.client-secret}") String secret,
      @Value("${openworkflow.authorization.request-timeout}") Duration timeout,
      @Value("${openworkflow.authorization.decision-ttl}") Duration ttl,
      @Value("${openworkflow.authorization.maximum-cache-entries}") int entries,
      @Value("${openworkflow.authorization.policy-version}") String version) {
    return AuthzenAuthorizationFactory.create(
        mapper, issuer, id, secret, timeout, ttl, entries, version);
  }

  @Singleton
  PekkoOperationAdapterRuntime pekkoRuntime(
      AuthorizationService authorization, ObjectMapper mapper, Environment environment) {
    return new PekkoOperationAdapterRuntime(
        PekkoOperationAdapterRuntime.Configuration.from(
            name -> environment.getProperty(name, String.class).orElse(null)),
        authorization,
        mapper);
  }

  @Singleton
  KafkaOperationAdapterRuntime runtime(
      AuthorizationService authorization,
      ObjectMapper mapper,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.topic-prefix}") String prefix,
      @Value("${openworkflow.adapters.consumer-group}") String group,
      @Value("${openworkflow.adapters.instance-id}") String instance,
      @Value("${openworkflow.adapters.secret-directory}") String secrets,
      @Value("${openworkflow.adapters.http-egress-allowlist:}") String allowlist,
      @Value("${openworkflow.operations.protocol.timeout-ms:30000}") long timeout,
      @Value("${openworkflow.operations.mcp-command-allowlist:}") String mcp,
      @Value("${openworkflow.operations.run.command-allowlist:}") String commands,
      @Value("${openworkflow.operations.run.interpreter-allowlist:}") String interpreters,
      @Value("${openworkflow.operations.run.image-allowlist:}") String images,
      @Value("${openworkflow.operations.run.volume-allowlist:}") String volumes,
      @Value("${openworkflow.operations.run.port-allowlist:}") String ports,
      @Value("${openworkflow.operations.run.oci-runtime:podman}") String oci) {
    var topics = OksTopics.withPrefix(prefix);
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
}
