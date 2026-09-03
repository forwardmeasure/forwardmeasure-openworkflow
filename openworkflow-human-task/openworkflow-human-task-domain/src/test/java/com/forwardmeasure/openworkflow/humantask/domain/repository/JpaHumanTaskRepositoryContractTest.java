/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskApplicationService;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskCommandResult;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery.Direction;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskQueryService;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskRepository;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskTransactionExecutor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActionTransition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Actor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActorKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.DispositionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Presentation;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.PresentationKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewAction;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewPlan;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewStage;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.SourceKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TaskSource;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TransitionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskCommandReceiptEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskContentRevisionEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEventEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskOutboxEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskReviewSessionEntity;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Real PostgreSQL contract for atomic snapshot, evidence, receipt, lease, and outbox writes. */
class JpaHumanTaskRepositoryContractTest {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
  private static final HumanTaskId TASK_ID = new HumanTaskId("task-1");
  private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
  private static final String TOKEN = "a".repeat(64);
  private static final Actor REVIEWER =
      new Actor("reviewer-1", ActorKind.HUMAN, Set.of("reviewers"), Set.of());
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  @Test
  void persistsAndReplaysTheCompleteSixTableContractAtomically() {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.start();
      PGSimpleDataSource admin = dataSource(postgres);
      var migrator = new OpenWorkflowTenantMigrator(admin, "openworkflow_runtime");
      migrator.ensureRuntimeRole(postgres.getPassword());
      migrator.provisionAndMigrate(TENANT);
      String schema = TenantSchema.forTenant(TENANT).value();

      try (var sessions = sessionFactory(postgres, schema)) {
        HumanTaskApplicationService service =
            new HumanTaskApplicationService(new JpaTransactions(sessions));
        HumanTaskCommandResult created = service.handle(create(), "1".repeat(64));
        HumanTaskState createdState = created.state();
        HumanTaskCommandResult claimed =
            service.handle(
                new HumanTaskCommand.BeginReview(
                    metadata("claim", createdState),
                    "session-1",
                    TOKEN,
                    NOW.plus(10, ChronoUnit.MINUTES)),
                "2".repeat(64));
        HumanTaskState claimedState = claimed.state();
        HumanTaskCommandResult revised =
            service.handle(
                new HumanTaskCommand.SaveRevision(
                    metadata("revise", claimedState),
                    "session-1",
                    TOKEN,
                    0,
                    DataReferences.inline(JsonNodeFactory.instance.objectNode().put("amount", 20)),
                    DataReferences.inline(JsonNodeFactory.instance.arrayNode()),
                    "Correct amount"),
                "3".repeat(64));
        HumanTaskState revisedState = revised.state();
        HumanTaskCommandResult approved =
            service.handle(
                new HumanTaskCommand.SubmitDecision(
                    metadata("approve", revisedState), "session-1", TOKEN, 1, "approve", null),
                "4".repeat(64));

        assertTrue(approved.state() instanceof HumanTaskState.Approved);
        HumanTaskCommandResult replay = service.handle(create(), "1".repeat(64));
        assertTrue(replay.replayed());
        assertEquals(
            createdState.snapshot().definition().taskId(),
            replay.state().snapshot().definition().taskId());
        assertEquals(
            createdState.snapshot().definition().source(),
            replay.state().snapshot().definition().source());
        assertEquals(
            createdState.snapshot().definition().originalContent(),
            replay.state().snapshot().definition().originalContent());
        assertEquals(
            createdState.snapshot().definition().presentation(),
            replay.state().snapshot().definition().presentation());
        assertEquals(
            createdState.snapshot().definition().reviewPlan(),
            replay.state().snapshot().definition().reviewPlan());
        assertEquals(
            createdState.snapshot().definition().blotterFields(),
            replay.state().snapshot().definition().blotterFields());
        assertEquals(createdState.snapshot().definition(), replay.state().snapshot().definition());
        assertEquals(
            createdState.snapshot().currentContent(), replay.state().snapshot().currentContent());
        assertEquals(createdState.snapshot().decisions(), replay.state().snapshot().decisions());
        assertEquals(createdState.snapshot(), replay.state().snapshot());
        assertEquals(createdState, replay.state());

        HumanTaskQueryService queries = new HumanTaskQueryService(new JpaTransactions(sessions));
        assertTrue(queries.find(TASK_ID).orElseThrow().state() instanceof HumanTaskState.Approved);
        var page =
            queries.list(
                new HumanTaskListQuery(
                    Set.of("APPROVED"),
                    "trade-review",
                    null,
                    null,
                    false,
                    null,
                    10,
                    "receivedAt",
                    Direction.ASC,
                    NOW));
        assertEquals(1, page.items().size());
        assertEquals(5, queries.history(TASK_ID, -1, 100).items().size());
        assertEquals(2, queries.revisions(TASK_ID).size());

        try (EntityManager entityManager = sessions.createEntityManager()) {
          assertEquals(1L, count(entityManager, schema, "human_task"));
          assertEquals(5L, count(entityManager, schema, "human_task_event"));
          assertEquals(2L, count(entityManager, schema, "human_task_content_revision"));
          assertEquals(1L, count(entityManager, schema, "human_task_review_session"));
          assertEquals(4L, count(entityManager, schema, "human_task_command_receipt"));
          assertEquals(2L, count(entityManager, schema, "human_task_outbox"));
          var correlations =
              entityManager
                  .createNativeQuery(
                      "select workflow_correlation, task_path from "
                          + schema
                          + ".human_task_outbox order by id")
                  .getResultList();
          assertEquals(2, correlations.size());
          assertEquals("corr-1", ((Object[]) correlations.get(0))[0]);
          assertEquals("/do/1", ((Object[]) correlations.get(0))[1]);
          assertEquals("corr-1", ((Object[]) correlations.get(1))[0]);
          assertEquals("/do/1", ((Object[]) correlations.get(1))[1]);
          Object releasedAt =
              entityManager
                  .createNativeQuery(
                      "select released_at from "
                          + schema
                          + ".human_task_review_session "
                          + "where review_session_id = 'session-1'")
                  .getSingleResult();
          assertTrue(releasedAt != null);
        }
      }
    }
  }

  private static HumanTaskCommand.Create create() {
    return new HumanTaskCommand.Create(metadata("create", null), definition());
  }

  private static CommandMetadata metadata(String commandId, HumanTaskState state) {
    return new CommandMetadata(
        TASK_ID, commandId, REVIEWER, NOW, state == null ? 0 : state.revision());
  }

  private static HumanTaskDefinition definition() {
    ReviewAction approve =
        new ReviewAction(
            "approve",
            "Approve",
            DispositionKind.APPROVE,
            new ActionTransition(TransitionKind.RESOLVE, null),
            false);
    ReviewStage stage =
        new ReviewStage(
            "review", "Review", Set.of(), Set.of("reviewers"), Set.of(), List.of(approve));
    return new HumanTaskDefinition(
        TASK_ID,
        "trade-review",
        "Review trade",
        "Check economics",
        10,
        new TaskSource(SourceKind.WORKFLOW, "workflow-1", "execution-1", "/do/1", "corr-1"),
        DataReferences.inline(JsonNodeFactory.instance.objectNode().put("amount", 10)),
        new Presentation(PresentationKind.RAW_JSON, null, null, null, null),
        new ReviewPlan(List.of(stage)),
        null,
        NOW.plus(1, ChronoUnit.HOURS),
        null,
        Map.of("amount", JsonNodeFactory.instance.numberNode(10)));
  }

  private static long count(EntityManager entityManager, String schema, String table) {
    return ((Number)
            entityManager
                .createNativeQuery("select count(*) from " + schema + "." + table)
                .getSingleResult())
        .longValue();
  }

  private static PGSimpleDataSource dataSource(PostgreSQLContainer postgres) {
    PGSimpleDataSource result = new PGSimpleDataSource();
    result.setURL(postgres.getJdbcUrl());
    result.setUser(postgres.getUsername());
    result.setPassword(postgres.getPassword());
    return result;
  }

  private static EntityManagerFactory sessionFactory(PostgreSQLContainer postgres, String schema) {
    Configuration configuration = new Configuration();
    configuration.addAnnotatedClass(HumanTaskEntity.class);
    configuration.addAnnotatedClass(HumanTaskEventEntity.class);
    configuration.addAnnotatedClass(HumanTaskContentRevisionEntity.class);
    configuration.addAnnotatedClass(HumanTaskReviewSessionEntity.class);
    configuration.addAnnotatedClass(HumanTaskCommandReceiptEntity.class);
    configuration.addAnnotatedClass(HumanTaskOutboxEntity.class);
    configuration.setProperty("jakarta.persistence.jdbc.url", postgres.getJdbcUrl());
    configuration.setProperty("jakarta.persistence.jdbc.user", postgres.getUsername());
    configuration.setProperty("jakarta.persistence.jdbc.password", postgres.getPassword());
    configuration.setProperty("hibernate.default_schema", schema);
    configuration.setProperty("hibernate.hbm2ddl.auto", "validate");
    return configuration.buildSessionFactory();
  }

  private record JpaTransactions(EntityManagerFactory sessions)
      implements HumanTaskTransactionExecutor {
    @Override
    public <T> T execute(Function<HumanTaskRepository, T> work) {
      try (EntityManager entityManager = sessions.createEntityManager()) {
        var transaction = entityManager.getTransaction();
        transaction.begin();
        try {
          T result = work.apply(new JpaHumanTaskRepository(entityManager, JSON));
          transaction.commit();
          return result;
        } catch (RuntimeException failure) {
          if (transaction.isActive()) {
            transaction.rollback();
          }
          throw failure;
        }
      }
    }
  }
}
