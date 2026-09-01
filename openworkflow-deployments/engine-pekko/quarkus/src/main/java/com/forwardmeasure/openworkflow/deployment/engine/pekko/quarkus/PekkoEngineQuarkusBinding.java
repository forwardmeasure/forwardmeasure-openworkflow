/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.quarkus;

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
import com.forwardmeasure.openworkflow.eventing.kafka.KafkaCloudEventConsumer;
import com.forwardmeasure.openworkflow.eventing.kafka.KafkaCloudEventPublisher;
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
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.persistence.EntityManagerFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
@ApplicationScoped
public class PekkoEngineQuarkusBinding {
  @Produces
  @ApplicationScoped
  ExecutionEventSink events(
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.execution-events.url") URI url,
      @ConfigProperty(name = "openworkflow.execution-events.timeout") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  /**
   * {@code transport} selects the wire carrier for {@code publish:emit:}/{@code receive:}/{@code
   * listen:} CloudEvents; {@code http} (unset/default) preserves the original behavior exactly -
   * {@code publish-url} is still required and still has no safe default, since guessing it wrong
   * would silently misroute events. {@code kafka} instead needs no subscriber URL at all (see
   * {@link #kafkaCloudEventConsumer} for the read side of that same topic), which is the whole
   * point of adding it: {@code publish-url}'s hard requirement with no safe default has caused a
   * real production outage by crashing pod startup for deployments that only wanted Kafka. {@code
   * publish-url}/{@code timeout} therefore can no longer be unconditionally-required
   * {@code @ConfigProperty} parameters (that would crash Kafka-transport startup on the same
   * missing value this change exists to route around) - {@code publish-url} is validated inside the
   * {@code http} branch instead, once {@code transport} is known.
   *
   * <p>{@code publish-url} is typed {@link Optional}&lt;{@link String}&gt;, not {@code String} with
   * {@code defaultValue = ""}: MicroProfile Config treats a resolved empty string as equivalent to
   * "no value present" ({@code Config#getValue} semantics), so an empty-string default does not
   * actually satisfy Quarkus's eager startup validation of every {@code @ConfigProperty} injection
   * point ({@code io.quarkus.arc.runtime.ConfigRecorder}) - it still crashes pod startup with
   * "Failed to load config value ... for: openworkflow.cloud-events .publish-url" regardless of
   * transport, reproducing the exact outage this whole change exists to prevent. {@code
   * Optional<String>} is the correct, spec-idiomatic way to mark a {@code @ConfigProperty} as
   * genuinely optional - an absent {@link Optional} satisfies eager validation with no default
   * needed.
   */
  @Produces
  @ApplicationScoped
  CloudEventPublisher cloudEventPublisher(
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.cloud-events.transport", defaultValue = "http")
          String transport,
      @ConfigProperty(name = "openworkflow.cloud-events.publish-url") Optional<String> publishUrl,
      @ConfigProperty(name = "openworkflow.cloud-events.timeout", defaultValue = "30S")
          Duration timeout,
      @ConfigProperty(
              name = "openworkflow.kafka.bootstrap-servers",
              defaultValue = "localhost:9092")
          String bootstrap,
      @ConfigProperty(name = "openworkflow.kafka.topic-prefix", defaultValue = "openworkflow")
          String topicPrefix) {
    return switch (transport) {
      case "kafka" ->
          new KafkaCloudEventPublisher(
              kafkaProperties(bootstrap), cloudEventsTopic(topicPrefix), mapper);
      case "http" -> new HttpCloudEventPublisher(requirePublishUrl(publishUrl), mapper, timeout);
      default ->
          throw new IllegalArgumentException(
              "Unknown openworkflow.cloud-events.transport: " + transport);
    };
  }

  /**
   * Only the Kafka transport's publisher actually holds a closeable resource (a producer network
   * client and I/O thread) - the HTTP transport's {@link HttpCloudEventPublisher} does not, so this
   * checks rather than assuming, unlike the concretely-typed {@link #close(PekkoEngineRuntime)}
   * disposer below.
   */
  void close(@Disposes CloudEventPublisher publisher) {
    if (publisher instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception failure) {
        throw new RuntimeException("Unable to close CloudEventPublisher", failure);
      }
    }
  }

  @Produces
  @ApplicationScoped
  ExecutionQueryRepository executionQueries(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper mapper) {
    return new HibernateSessionExecutionQueryRepository(tenants, entityManagerFactory, mapper);
  }

  @Produces
  @ApplicationScoped
  HibernateSessionSubworkflowPlanResolver subworkflowPlanResolver(
      TenantScope tenants, EntityManagerFactory entityManagerFactory) {
    return new HibernateSessionSubworkflowPlanResolver(tenants, entityManagerFactory);
  }

  @Produces
  @ApplicationScoped
  AuthenticatedActorProvider authenticatedActorProvider(
      ActiveOrganizationProvider organizations, AuthorizationService authorization) {
    return new AuthzenAuthenticatedActorProvider(organizations, authorization);
  }

  @Produces
  @ApplicationScoped
  CloudEventHttpDecoder cloudEventHttpDecoder(ObjectMapper mapper) {
    return new CloudEventHttpDecoder(mapper);
  }

  @Produces
  @ApplicationScoped
  PekkoEngineRuntime runtime(
      ExecutionEventSink events,
      @ConfigProperty(name = "openworkflow.pekko.system-name") String systemName,
      @ConfigProperty(name = "openworkflow.pekko.ask-timeout") Duration askTimeout,
      @ConfigProperty(name = "openworkflow.persistence.profile") String profileName,
      @ConfigProperty(name = "openworkflow.persistence.endpoint") String endpoint,
      @ConfigProperty(name = "openworkflow.persistence.username") Optional<String> username,
      @ConfigProperty(name = "openworkflow.persistence.password") Optional<String> password,
      @ConfigProperty(
              name = "openworkflow.persistence.local-datacenter",
              defaultValue = "datacenter1")
          String datacenter,
      @ConfigProperty(name = "openworkflow.cluster.discovery-service") Optional<String> discovery,
      @ConfigProperty(name = "openworkflow.cluster.pod-ip") Optional<String> podIp,
      @ConfigProperty(name = "openworkflow.cluster.artery-port") int arteryPort,
      @ConfigProperty(name = "openworkflow.cluster.management-port") int managementPort,
      @ConfigProperty(name = "openworkflow.cluster.required-contact-points") int contacts,
      @ConfigProperty(name = "openworkflow.cluster.role", defaultValue = "workflow-engine")
          String role) {
    PersistenceProfile profile = PersistenceProfile.parse(profileName);
    var configuration =
        PersistenceConfigLoader.withConnection(
            PersistenceConfigLoader.select(ConfigFactory.load(), profile),
            profile,
            endpoint,
            username.orElse(""),
            password.orElse(""),
            datacenter);
    Optional<PostgresConnectionSettings> postgresConnection =
        profile == PersistenceProfile.POSTGRESQL
            ? Optional.of(
                new PostgresConnectionSettings(endpoint, username.orElse(""), password.orElse("")))
            : Optional.empty();
    return new PekkoEngineRuntime(
        systemName,
        configuration,
        new PekkoClusterRuntime.Settings(
            discovery.orElse(""), podIp.orElse(""), arteryPort, managementPort, contacts, role),
        askTimeout,
        events,
        postgresConnection);
  }

  /**
   * Starts after the CDI producers above so {@code runtime}'s actor system already exists -
   * ShardedDaemonProcess needs the live system, not a fresh one, so this must reuse {@code
   * runtime.actorSystem()}/{@code runtime.workflows()} rather than constructing its own.
   *
   * <p>No {@code @Observes} parameter here - Quarkus ARC's CDI validator rejects a producer method
   * that is also an observer method ("Producer method must not have an @Observes parameter"),
   * caught only by a real {@code quarkus:build} (bean-processing validation, not plain javac).
   * {@link #eagerlyStartEventing} below forces this producer to run at startup instead, by
   * injecting its result as an ordinary parameter on a genuine observer method.
   */
  @Produces
  @ApplicationScoped
  CloudEventIngress ingress(
      PekkoEngineRuntime runtime,
      CloudEventPublisher publisher,
      ExecutionQueryRepository executions,
      HibernateSessionSubworkflowPlanResolver subworkflows,
      DataSource dataSource,
      @ConfigProperty(name = "openworkflow.persistence.profile") String profileName,
      @ConfigProperty(name = "openworkflow.persistence.endpoint") Optional<String> endpoint,
      @ConfigProperty(
              name = "openworkflow.persistence.local-datacenter",
              defaultValue = "datacenter1")
          String datacenter,
      @ConfigProperty(name = "openworkflow.eventing.ask-timeout") Duration askTimeout,
      @ConfigProperty(name = "openworkflow.eventing.tenant-rescan-interval", defaultValue = "3m")
          Duration tenantRescanInterval) {
    var system = runtime.actorSystem();
    var workflows = runtime.workflows();
    var coordinators =
        SubworkflowCoordinatorSharding.initialize(system, workflows, runtime.postgresConnection());
    var dispatch = ScheduledExecutionDispatcher.spawn(system, workflows);
    var schedules =
        WorkflowScheduleSharding.initialize(system, dispatch, runtime.postgresConnection());
    // Not its own CDI bean deliberately - a producer for a normal-scoped (ApplicationScoped)
    // bean cannot return null, and this session only exists at all for the Cassandra profile.
    // Not explicitly closed on shutdown either - this process only ever terminates by the JVM
    // exiting, which closes the socket; nothing else in this class needs the session directly.
    CloudEventSubscriptionRepository subscriptions;
    if (PersistenceProfile.parse(profileName) == PersistenceProfile.CASSANDRA) {
      CqlSession session =
          CqlSession.builder()
              .addContactPoint(contactPoint(endpoint.orElse("")))
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

  /**
   * The read side of the Kafka transport: consumes the same topic {@link #cloudEventPublisher}
   * writes to when {@code transport=kafka} and routes each CloudEvent via {@code ingress}. Always
   * constructed (CDI producers for a normal-scoped bean cannot return null, same reasoning as
   * {@code subscriptions} above) but only actually {@code start()}ed for the Kafka transport - for
   * {@code http} it stays a harmless, never-started object whose {@link
   * #close(KafkaCloudEventConsumer)} disposer is then also a no-op.
   */
  @Produces
  @ApplicationScoped
  KafkaCloudEventConsumer kafkaCloudEventConsumer(
      CloudEventIngress ingress,
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.cloud-events.transport", defaultValue = "http")
          String transport,
      @ConfigProperty(
              name = "openworkflow.kafka.bootstrap-servers",
              defaultValue = "localhost:9092")
          String bootstrap,
      @ConfigProperty(name = "openworkflow.kafka.topic-prefix", defaultValue = "openworkflow")
          String topicPrefix,
      @ConfigProperty(
              name = "openworkflow.eventing.consumer-group",
              defaultValue = "openworkflow-pekko-cloud-events")
          String group,
      @ConfigProperty(name = "openworkflow.eventing.instance-id", defaultValue = "local")
          String instanceId,
      @ConfigProperty(name = "openworkflow.eventing.ask-timeout") Duration askTimeout) {
    var consumer =
        new KafkaCloudEventConsumer(
            kafkaProperties(bootstrap),
            cloudEventsTopic(topicPrefix),
            group,
            instanceId,
            ingress,
            mapper,
            askTimeout);
    if ("kafka".equals(transport)) {
      consumer.start();
    }
    return consumer;
  }

  void close(@Disposes KafkaCloudEventConsumer consumer) {
    consumer.close();
  }

  /**
   * CDI producer beans are lazily constructed by default - without something forcing it, {@code
   * ingress} above (and everything it starts) would never actually run unless some other bean
   * happens to depend on it first. This is that forcing trigger: a genuine observer method (no
   * {@code @Produces}) with {@code ingress} as an ordinary injected parameter. {@code
   * kafkaCloudEventConsumer} is listed here too so its (conditional) {@code start()} also runs at
   * boot rather than sitting unreachable, same reasoning.
   */
  void eagerlyStartEventing(
      @Observes StartupEvent event,
      CloudEventIngress ingress,
      KafkaCloudEventConsumer kafkaCloudEventConsumer) {}

  @Produces
  @ApplicationScoped
  CloudEventIngressResource cloudEventIngress(
      CloudEventIngress ingress, AuthenticatedActorProvider actors, CloudEventHttpDecoder decoder) {
    return new CloudEventIngressResource(ingress, actors, decoder);
  }

  void close(@Disposes PekkoEngineRuntime runtime) {
    runtime.close();
  }

  @Produces
  @ApplicationScoped
  ExecutionEngineProvider provider(PekkoEngineRuntime runtime) {
    return runtime.provider();
  }

  @Produces
  @ApplicationScoped
  EngineCommandResource commands(ExecutionEngineProvider provider) {
    return new EngineCommandResource(provider);
  }

  private static InetSocketAddress contactPoint(String endpoint) {
    int separator = endpoint.lastIndexOf(':');
    if (separator < 0) {
      throw new IllegalArgumentException("Cassandra endpoint must be host:port: " + endpoint);
    }
    return new InetSocketAddress(
        endpoint.substring(0, separator), Integer.parseInt(endpoint.substring(separator + 1)));
  }

  private static Properties kafkaProperties(String bootstrap) {
    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    return properties;
  }

  /** Distinct from Kafka-Streams' own {@code <prefix>.inbound-events}/{@code .emitted-events}. */
  private static String cloudEventsTopic(String prefix) {
    return prefix + ".pekko-cloud-events";
  }

  private static URI requirePublishUrl(Optional<String> value) {
    if (value.isEmpty() || value.get().isBlank()) {
      throw new IllegalStateException(
          "openworkflow.cloud-events.publish-url is required when "
              + "openworkflow.cloud-events.transport=http");
    }
    return URI.create(value.get());
  }
}
