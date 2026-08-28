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
package com.forwardmeasure.openworkflow.definition.management;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.jpa.identity.entity.IdentityType;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowDefinitionException;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleHistory;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowPublication;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowReview;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowDefinitionRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowLifecycleHistoryRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowPublicationRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowRepository;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowReviewRepository;
import com.forwardmeasure.openworkflow.definition.domain.service.impl.WorkflowGovernanceServiceImpl;
import com.forwardmeasure.openworkflow.definition.domain.service.impl.WorkflowManagementServiceImpl;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.ReviewDecisionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowDefinitionRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowDefinitionManagementServiceTest {
  private static final TenantId TENANT = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final ActiveOrganization AUTHOR = actor("author");
  private static final ActiveOrganization REVIEWER = actor("reviewer");

  private MemoryStore store;
  private WorkflowManagementServiceImpl workflows;
  private WorkflowGovernanceServiceImpl governance;

  @BeforeEach
  void setUp() {
    store = new MemoryStore();
    workflows = new WorkflowManagementServiceImpl(store, new PermitAllAuthorization());
    governance =
        new WorkflowGovernanceServiceImpl(
            store,
            store,
            store,
            store,
            store,
            new PermitAllAuthorization(),
            new OpenWorkflowCompiler(),
            request -> {
              throw new AssertionError("The test workflow has no external resources");
            });
  }

  @Test
  void createsTenantUniqueWorkflowAndImmutableCompiledRevision() {
    Workflow workflow =
        workflows.createWorkflow(
            AUTHOR,
            "create-workflow",
            new CreateWorkflowRequest("orders", "Orders").description("Order processing"));
    WorkflowDefinition first = createDefinition(workflow, "one", "1.0.0");
    WorkflowDefinition second = createDefinition(workflow, "two", "2.0.0");

    assertEquals(1, first.getRevisionNumber());
    assertEquals(2, second.getRevisionNumber());
    assertNotEquals(first.getSourceDigest(), second.getSourceDigest());
    assertTrue(first.getResolvedDigest().matches("[0-9a-f]{64}"));
    assertEquals(
        2,
        governance
            .listWorkflowDefinitions(AUTHOR, "list", workflow.getUuid(), null, 0, 20)
            .totalItems());
    assertThrows(
        DefinitionManagementException.class,
        () ->
            workflows.createWorkflow(
                AUTHOR, "duplicate", new CreateWorkflowRequest("orders", "Duplicate")));
  }

  @Test
  void enforcesLifecycleAndMakerCheckerRules() {
    Workflow workflow =
        workflows.createWorkflow(
            AUTHOR, "create-workflow", new CreateWorkflowRequest("orders", "Orders"));
    WorkflowDefinition definition = createDefinition(workflow, "one", "1.0.0");

    governance.submitWorkflowDefinition(
        AUTHOR, "submit", workflow.getUuid(), definition.getUuid(), 0);
    assertEquals(WorkflowLifecycleState.IN_REVIEW, definition.getLifecycleState());
    assertThrows(
        DefinitionManagementException.class,
        () ->
            governance.approveWorkflowDefinition(
                AUTHOR,
                "self-approve",
                workflow.getUuid(),
                definition.getUuid(),
                0,
                new ReviewDecisionRequest().reason("not independent")));

    governance.approveWorkflowDefinition(
        REVIEWER,
        "approve",
        workflow.getUuid(),
        definition.getUuid(),
        0,
        new ReviewDecisionRequest().reason("reviewed"));
    governance.publishWorkflowDefinition(
        REVIEWER, "publish", workflow.getUuid(), definition.getUuid(), 0);

    assertEquals(WorkflowLifecycleState.PUBLISHED, definition.getLifecycleState());
    assertEquals(1, store.reviews.size());
    assertEquals(1, store.publications.size());
    assertEquals(4, store.history.size());
  }

  @Test
  void savesAndUpdatesADraftThatDoesNotCompileInsteadOfBlockingTheSave() {
    Workflow workflow =
        workflows.createWorkflow(
            AUTHOR, "create-workflow", new CreateWorkflowRequest("orders", "Orders"));
    WorkflowDefinition definition =
        governance.createWorkflowDefinition(
            AUTHOR,
            "create-definition",
            workflow.getUuid(),
            new CreateWorkflowDefinitionRequest("1.0.0", invalidWorkflowSource("one", "1.0.0")));

    // Persisted anyway - the point of this whole change. documentVersion is the one exception,
    // populated from the request even though nothing compiled, since it's how "find the draft for
    // version X" lookups work before there's anything to compile.
    assertEquals(WorkflowLifecycleState.DRAFT, definition.getLifecycleState());
    assertEquals("1.0.0", definition.getDocumentVersion());
    assertNull(definition.getResolvedDocument());
    assertNull(definition.getResolvedResources());
    assertNull(definition.getNamespace());
    assertNull(definition.getSpecificationVersion());
    assertNull(definition.getCompilerProfile());
    assertNull(definition.getResolvedDigest());
    assertTrue(definition.getSourceDigest().matches("[0-9a-f]{64}"));

    // Editing it while still broken (setSourceOnly) doesn't touch the (already-null) compiled
    // fields, and doesn't throw either - same "never blocks" behavior on update.
    WorkflowDefinition updated =
        governance.updateWorkflowDefinition(
            AUTHOR,
            "update-still-broken",
            workflow.getUuid(),
            definition.getUuid(),
            0,
            new UpdateWorkflowDefinitionRequest(
                invalidWorkflowSource("one", "1.0.0") + "  extra: true\n"));
    assertNull(updated.getResolvedDigest());

    // Submitting is the actual enforcement point - a definition that still doesn't compile stays
    // DRAFT, the exception now propagating as WorkflowDefinitionException (a clean 422 for a real
    // caller, via WorkflowDefinitionExceptionMapper - not exercised here, this is the JAX-RS
    // layer's job).
    assertThrows(
        WorkflowDefinitionException.class,
        () ->
            governance.submitWorkflowDefinition(
                AUTHOR, "submit-still-broken", workflow.getUuid(), definition.getUuid(), 0));
    assertEquals(WorkflowLifecycleState.DRAFT, updated.getLifecycleState());

    // Fix it, then submit again - now it compiles for the first time, populating every
    // compiled-derived field and actually transitioning to IN_REVIEW.
    governance.updateWorkflowDefinition(
        AUTHOR,
        "update-fixed",
        workflow.getUuid(),
        definition.getUuid(),
        0,
        new UpdateWorkflowDefinitionRequest(workflowSource("one", "1.0.0")));
    WorkflowDefinition submitted =
        governance.submitWorkflowDefinition(
            AUTHOR, "submit-fixed", workflow.getUuid(), definition.getUuid(), 0);
    assertEquals(WorkflowLifecycleState.IN_REVIEW, submitted.getLifecycleState());
    assertTrue(submitted.getResolvedDigest().matches("[0-9a-f]{64}"));
    assertEquals("tests", submitted.getNamespace());
  }

  private WorkflowDefinition createDefinition(Workflow workflow, String name, String version) {
    return governance.createWorkflowDefinition(
        AUTHOR,
        "create-definition-" + name,
        workflow.getUuid(),
        new CreateWorkflowDefinitionRequest(version, workflowSource(name, version)));
  }

  // "raise" with neither an inline error object nor a use.errors reference - fails the compiler's
  // oneOf schema check the exact way the reported production crash did (an empty/malformed task),
  // not a YAML syntax error - the point of this whole feature is a definition that PARSES fine but
  // doesn't satisfy the workflow schema.
  private static String invalidWorkflowSource(String name, String version) {
    return """
    document:
      dsl: '1.0.3'
      namespace: tests
      name: %s
      version: '%s'
    do:
      - broken:
          raise: {}
    """
        .formatted(name, version);
  }

  private static ActiveOrganization actor(String actorId) {
    return new ActiveOrganization(TENANT, "organization", actorId, Set.of("definition-author"));
  }

  private static Actor persistentActor(String actorId) {
    return Actor.builder()
        .id((long) actorId.hashCode())
        .uuid(UUID.nameUUIDFromBytes(actorId.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        .subjectIdentifier(actorId)
        .type(IdentityType.HUMAN)
        .identityProvider("test")
        .build();
  }

  private static String workflowSource(String name, String version) {
    return """
    document:
      dsl: '1.0.3'
      namespace: tests
      name: %s
      version: '%s'
    do:
      - initialize:
          set:
            ready: true
    """
        .formatted(name, version);
  }

  private static final class PermitAllAuthorization implements AuthorizationService {
    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      return new AuthorizationDecision(true, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }

  private static final class MemoryStore
      implements WorkflowRepository,
          WorkflowDefinitionRepository,
          WorkflowReviewRepository,
          WorkflowPublicationRepository,
          WorkflowLifecycleHistoryRepository {
    private final List<Workflow> workflows = new ArrayList<>();
    private final List<WorkflowDefinition> definitions = new ArrayList<>();
    private final List<WorkflowReview> reviews = new ArrayList<>();
    private final List<WorkflowPublication> publications = new ArrayList<>();
    private final List<WorkflowLifecycleHistory> history = new ArrayList<>();

    @Override
    public Workflow create(String ownerActorId, String name, String title, String description) {
      Workflow value = new Workflow(name, title, description);
      value.setUuid(UUID.randomUUID());
      value.setVersion(0);
      value.setOwner(persistentActor(ownerActorId));
      workflows.add(value);
      return value;
    }

    @Override
    public boolean existsByName(String name) {
      return workflows.stream().anyMatch(value -> value.getName().equals(name));
    }

    @Override
    public Optional<Workflow> findById(UUID workflowId) {
      return workflows.stream().filter(value -> value.getUuid().equals(workflowId)).findFirst();
    }

    @Override
    public Page<Workflow> list(int offset, int limit) {
      return page(workflows, offset, limit);
    }

    @Override
    public void delete(Workflow workflow) {
      workflows.remove(workflow);
    }

    @Override
    public int nextRevisionNumber(Workflow workflow) {
      return (int) definitions.stream().filter(value -> value.getWorkflow() == workflow).count()
          + 1;
    }

    @Override
    public WorkflowDefinition create(
        Workflow workflow,
        int revisionNumber,
        String authorActorId,
        String sourceDocument,
        String resolvedDocument,
        String resolvedResources,
        String namespace,
        String documentVersion,
        String specificationVersion,
        String compilerProfile,
        String sourceDigest,
        String resolvedDigest) {
      WorkflowDefinition value =
          new WorkflowDefinition(
              workflow,
              revisionNumber,
              sourceDocument,
              resolvedDocument,
              resolvedResources,
              namespace,
              documentVersion,
              specificationVersion,
              compilerProfile,
              sourceDigest,
              resolvedDigest,
              persistentActor(authorActorId));
      value.setUuid(UUID.randomUUID());
      value.setVersion(0);
      definitions.add(value);
      return value;
    }

    @Override
    public WorkflowDefinition create(
        Workflow workflow,
        int revisionNumber,
        String authorActorId,
        String sourceDocument,
        String sourceDigest,
        String documentVersion) {
      WorkflowDefinition value =
          new WorkflowDefinition(
              workflow,
              revisionNumber,
              sourceDocument,
              sourceDigest,
              documentVersion,
              persistentActor(authorActorId));
      value.setUuid(UUID.randomUUID());
      value.setVersion(0);
      definitions.add(value);
      return value;
    }

    @Override
    public Optional<WorkflowDefinition> findByWorkflowAndUuid(
        Workflow workflow, UUID definitionId) {
      return definitions.stream()
          .filter(value -> value.getWorkflow() == workflow && value.getUuid().equals(definitionId))
          .findFirst();
    }

    @Override
    public Page<WorkflowDefinition> listByWorkflow(
        Workflow workflow, WorkflowLifecycleState status, int offset, int limit) {
      return page(
          definitions.stream()
              .filter(value -> value.getWorkflow() == workflow)
              .filter(value -> status == null || value.getLifecycleState() == status)
              .toList(),
          offset,
          limit);
    }

    @Override
    public Page<WorkflowDefinition> search(WorkflowLifecycleState status, int offset, int limit) {
      return page(
          definitions.stream()
              .filter(value -> status == null || value.getLifecycleState() == status)
              .toList(),
          offset,
          limit);
    }

    @Override
    public void delete(WorkflowDefinition definition) {
      definitions.remove(definition);
    }

    @Override
    public WorkflowReview record(
        WorkflowDefinition definition,
        String action,
        String actorId,
        String digest,
        String reason) {
      WorkflowReview value =
          new WorkflowReview(definition, action, persistentActor(actorId), digest, reason);
      reviews.add(value);
      return value;
    }

    @Override
    public WorkflowPublication publish(
        WorkflowDefinition definition, String actorId, String definitionDigest) {
      WorkflowPublication value =
          new WorkflowPublication(definition, persistentActor(actorId), definitionDigest);
      publications.add(value);
      return value;
    }

    @Override
    public WorkflowLifecycleHistory record(
        WorkflowDefinition definition,
        WorkflowLifecycleState fromState,
        WorkflowLifecycleState toState,
        String actorId,
        String correlationId) {
      WorkflowLifecycleHistory value =
          new WorkflowLifecycleHistory(
              definition, fromState, toState, persistentActor(actorId), correlationId);
      history.add(value);
      return value;
    }

    private static <T> Page<T> page(List<T> values, int offset, int limit) {
      int from = Math.min(offset, values.size());
      int to = Math.min(from + limit, values.size());
      return new Page<>(values.subList(from, to), values.size(), offset, limit);
    }
  }
}
