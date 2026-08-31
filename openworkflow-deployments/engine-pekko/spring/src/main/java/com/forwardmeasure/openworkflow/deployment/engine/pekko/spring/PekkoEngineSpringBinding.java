/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.actor.PekkoClusterRuntime;
import com.forwardmeasure.openworkflow.actor.PekkoEngineRuntime;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.actor.ScheduledExecutionDispatcher;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.TenantProjectionSupervisor;
import com.forwardmeasure.openworkflow.actor.WorkflowScheduleSharding;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.eventing.CloudEventHttpDecoder;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngressGateway;
import com.forwardmeasure.openworkflow.eventing.CloudEventPublisher;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.HttpCloudEventPublisher;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventOutbox;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventSubscriptionProjection;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraSubworkflowOutbox;
import com.forwardmeasure.openworkflow.eventing.jaxrs.AuthenticatedActorProvider;
import com.forwardmeasure.openworkflow.eventing.jaxrs.AuthzenAuthenticatedActorProvider;
import com.forwardmeasure.openworkflow.eventing.jaxrs.CloudEventIngressResource;
import com.forwardmeasure.openworkflow.eventing.persistence.HibernateSessionExecutionQueryRepository;
import com.forwardmeasure.openworkflow.eventing.persistence.HibernateSessionSubworkflowPlanResolver;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventOutbox;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventSubscriptionProjection;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlSubworkflowOutbox;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.persistence.PersistenceConfigLoader;
import com.forwardmeasure.openworkflow.persistence.PersistenceProfile;
import com.typesafe.config.ConfigFactory;
import jakarta.persistence.EntityManagerFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * publish:emit: CloudEvents, run:workflow: subworkflows, and receive:/listen: steps all durably
 * record a pending interaction and then rely on a separate at-least-once Pekko Projection (the
 * "outbox"/"subscription projection") reading the same journal to actually dispatch/launch/route
 * them - unlike Kafka-Streams, where the topology itself is the always-running dispatch mechanism,
 * Pekko needs these started explicitly. Until this class started them, none of the three were
 * reachable on a real Pekko deployment: the actor correctly recorded the pending state and then
 * waited forever, since nothing was running to pick it up (verified by grepping the whole
 * deployable surface for production, non-test callers - there were none, anywhere, before this). A
 * fourth, closely related gap found the same way: event-triggered schedules had no production
 * consumer for the {@code ScheduledExecutionRequest} they emit, so a matched CloudEvent would have
 * silently never launched the schedule's execution - see {@link ScheduledExecutionDispatcher}.
 */
@Configuration(proxyBeanMethods = false)
public class PekkoEngineSpringBinding {
  @Bean
  ExecutionEventSink events(
      ObjectMapper mapper,
      @Value("${openworkflow.execution-events.url}") URI url,
      @Value("${openworkflow.execution-events.timeout}") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  @Bean
  CloudEventPublisher cloudEventPublisher(
      ObjectMapper mapper,
      @Value("${openworkflow.cloud-events.publish-url}") URI url,
      @Value("${openworkflow.cloud-events.timeout}") Duration timeout) {
    return new HttpCloudEventPublisher(url, mapper, timeout);
  }

  @Bean
  ExecutionQueryRepository executionQueries(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper mapper) {
    return new HibernateSessionExecutionQueryRepository(tenants, entityManagerFactory, mapper);
  }

  @Bean
  HibernateSessionSubworkflowPlanResolver subworkflowPlanResolver(
      TenantScope tenants, EntityManagerFactory entityManagerFactory) {
    return new HibernateSessionSubworkflowPlanResolver(tenants, entityManagerFactory);
  }

  @Bean
  AuthenticatedActorProvider authenticatedActorProvider(
      ActiveOrganizationProvider organizations, AuthorizationService authorization) {
    return new AuthzenAuthenticatedActorProvider(organizations, authorization);
  }

  @Bean
  CloudEventHttpDecoder cloudEventHttpDecoder(ObjectMapper mapper) {
    return new CloudEventHttpDecoder(mapper);
  }

  @Bean(destroyMethod = "close")
  PekkoEngineRuntime runtime(
      ExecutionEventSink events,
      @Value("${openworkflow.pekko.system-name}") String systemName,
      @Value("${openworkflow.pekko.ask-timeout}") Duration askTimeout,
      @Value("${openworkflow.persistence.profile}") String profileName,
      @Value("${openworkflow.persistence.endpoint}") String endpoint,
      @Value("${openworkflow.persistence.username:}") String username,
      @Value("${openworkflow.persistence.password:}") String password,
      @Value("${openworkflow.persistence.local-datacenter:datacenter1}") String datacenter,
      @Value("${openworkflow.cluster.discovery-service:}") String discovery,
      @Value("${openworkflow.cluster.pod-ip:}") String podIp,
      @Value("${openworkflow.cluster.artery-port}") int arteryPort,
      @Value("${openworkflow.cluster.management-port}") int managementPort,
      @Value("${openworkflow.cluster.required-contact-points}") int contacts,
      @Value("${openworkflow.cluster.role:workflow-engine}") String role) {
    PersistenceProfile profile = PersistenceProfile.parse(profileName);
    var configuration =
        PersistenceConfigLoader.withConnection(
            PersistenceConfigLoader.select(ConfigFactory.load(), profile),
            profile,
            endpoint,
            username,
            password,
            datacenter);
    Optional<PostgresConnectionSettings> postgresConnection =
        profile == PersistenceProfile.POSTGRESQL
            ? Optional.of(new PostgresConnectionSettings(endpoint, username, password))
            : Optional.empty();
    return new PekkoEngineRuntime(
        systemName,
        configuration,
        new PekkoClusterRuntime.Settings(
            discovery, podIp, arteryPort, managementPort, contacts, role),
        askTimeout,
        events,
        postgresConnection);
  }

  /**
   * Spring {@code @Bean} methods resolve their parameters as dependencies, constructing them
   * (fully) before this method runs - so this doesn't need an {@code ApplicationReadyEvent}
   * listener the way one might expect; it just needs to depend on {@code runtime}, same as {@code
   * provider} below already does. {@code ShardedDaemonProcess} needs {@code runtime}'s already-live
   * actor system, not a fresh one, so this reuses {@code runtime.actorSystem()}/{@code
   * runtime.workflows()} rather than constructing its own.
   */
  @Bean
  CloudEventIngress startEventing(
      PekkoEngineRuntime runtime,
      CloudEventPublisher publisher,
      ExecutionQueryRepository executions,
      HibernateSessionSubworkflowPlanResolver subworkflows,
      DataSource dataSource,
      @Value("${openworkflow.persistence.profile}") String profileName,
      @Value("${openworkflow.persistence.endpoint:}") String endpoint,
      @Value("${openworkflow.persistence.local-datacenter:datacenter1}") String datacenter,
      @Value("${openworkflow.eventing.ask-timeout}") Duration askTimeout,
      @Value("${openworkflow.eventing.tenant-rescan-interval:3m}") Duration tenantRescanInterval) {
    var system = runtime.actorSystem();
    var workflows = runtime.workflows();
    var coordinators =
        SubworkflowCoordinatorSharding.initialize(system, workflows, runtime.postgresConnection());
    var dispatch = ScheduledExecutionDispatcher.spawn(system, workflows);
    var schedules =
        WorkflowScheduleSharding.initialize(system, dispatch, runtime.postgresConnection());
    // Not its own Spring bean deliberately - only exists at all for the Cassandra profile, and
    // not explicitly closed on shutdown - this process only ever terminates by the JVM exiting,
    // which closes the socket; nothing else in this class needs the session directly.
    CloudEventSubscriptionRepository subscriptions;
    if (PersistenceProfile.parse(profileName) == PersistenceProfile.CASSANDRA) {
      CqlSession session =
          CqlSession.builder()
              .addContactPoint(contactPoint(endpoint))
              .withLocalDatacenter(datacenter)
              .build();
      subscriptions = new CassandraCloudEventSubscriptionRepository(session);
      CassandraCloudEventOutbox.start(system, workflows, publisher, askTimeout, executions);
      CassandraSubworkflowOutbox.start(system, subworkflows, coordinators, askTimeout);
      CassandraCloudEventSubscriptionProjection.start(system, subscriptions);
    } else {
      subscriptions = new PostgresqlCloudEventSubscriptionRepository(dataSource);
      PostgresConnectionSettings connection =
          runtime
              .postgresConnection()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Postgres persistence profile requires connection settings"));
      TenantProjectionSupervisor.start(
          system,
          dataSource,
          connection,
          tenantRescanInterval,
          List.of(
              (sys, ds, schema, conn) ->
                  PostgresqlCloudEventOutbox.start(
                      sys, ds, schema, conn, workflows, publisher, askTimeout, executions),
              (sys, ds, schema, conn) ->
                  PostgresqlSubworkflowOutbox.start(
                      sys, ds, schema, conn, subworkflows, coordinators, askTimeout),
              (sys, ds, schema, conn) ->
                  PostgresqlCloudEventSubscriptionProjection.start(
                      sys, ds, schema, conn, subscriptions)));
    }
    return new CloudEventIngressGateway(
        workflows, schedules, system, askTimeout, subscriptions, 10_000);
  }

  @Bean
  CloudEventIngressResource cloudEventIngress(
      CloudEventIngress ingress, AuthenticatedActorProvider actors, CloudEventHttpDecoder decoder) {
    return new CloudEventIngressResource(ingress, actors, decoder);
  }

  @Bean
  ExecutionEngineProvider provider(PekkoEngineRuntime runtime) {
    return runtime.provider();
  }

  @Bean
  EngineCommandResource commands(ExecutionEngineProvider provider) {
    return new EngineCommandResource(provider);
  }

  @Bean
  ResourceConfigCustomizer resources(
      EngineCommandResource commands, CloudEventIngressResource cloudEvents) {
    return resourceConfig -> resourceConfig.register(commands).register(cloudEvents);
  }

  private static InetSocketAddress contactPoint(String endpoint) {
    int separator = endpoint.lastIndexOf(':');
    if (separator < 0) {
      throw new IllegalArgumentException("Cassandra endpoint must be host:port: " + endpoint);
    }
    return new InetSocketAddress(
        endpoint.substring(0, separator), Integer.parseInt(endpoint.substring(separator + 1)));
  }
}
