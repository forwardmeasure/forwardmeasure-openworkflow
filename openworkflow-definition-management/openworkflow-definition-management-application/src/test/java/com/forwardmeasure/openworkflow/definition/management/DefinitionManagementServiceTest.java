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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefinitionManagementServiceTest {
  private static final TenantId TENANT = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final ActiveOrganization AUTHOR = actor("author");
  private static final ActiveOrganization REVIEWER = actor("reviewer");
  private InMemoryRepository repository;
  private DefinitionManagementService service;

  @BeforeEach
  void setUp() {
    repository = new InMemoryRepository();
    service =
        new DefinitionManagementService(
            repository, new PermitAllAuthorization(), new OpenWorkflowCompiler());
  }

  @Test
  void supportsCreateValidateReviseRetrieveAndListWithImmutableDigestBinding() {
    DefinitionValidation validation =
        service.validate(AUTHOR, "validate", "orders", workflow("one"));
    ManagedWorkflowRevision first =
        service.create(AUTHOR, "create", "orders", "Orders", workflow("one"));
    ManagedWorkflowRevision second =
        service.revise(AUTHOR, "revise", "orders", "Orders v2", workflow("two"));

    assertEquals(validation.sourceDigest(), first.sourceDigest());
    assertEquals(validation.resolvedDigest(), first.resolvedDigest());
    assertEquals(1, first.revisionNumber());
    assertEquals(2, second.revisionNumber());
    assertNotEquals(first.sourceDigest(), second.sourceDigest());
    assertEquals(second, service.retrieve(AUTHOR, "retrieve", "orders", 2));
    assertEquals(List.of(first, second), service.list(AUTHOR, "list"));
    assertThrows(
        DefinitionManagementException.class,
        () -> service.create(AUTHOR, "duplicate", "orders", "Orders", workflow("three")));
    assertThrows(
        RuntimeException.class,
        () -> service.validate(AUTHOR, "invalid", "bad", "not-a-workflow: true"));
  }

  @Test
  void enforcesLifecycleTransitionMatrix() {
    service.create(AUTHOR, "create", "orders", "Orders", workflow("one"));
    assertState(service.submit(AUTHOR, "submit", "orders", 1), WorkflowLifecycleState.IN_REVIEW);
    assertState(service.withdraw(AUTHOR, "withdraw", "orders", 1), WorkflowLifecycleState.DRAFT);
    service.submit(AUTHOR, "resubmit", "orders", 1);
    assertState(service.reject(REVIEWER, "reject", "orders", 1), WorkflowLifecycleState.REJECTED);
    assertThrows(
        DefinitionManagementException.class,
        () -> service.publish(REVIEWER, "bad-publish", "orders", 1));

    service.revise(AUTHOR, "revise", "orders", "Orders v2", workflow("two"));
    service.submit(AUTHOR, "submit-2", "orders", 2);
    assertState(service.approve(REVIEWER, "approve", "orders", 2), WorkflowLifecycleState.APPROVED);
    assertState(
        service.publish(REVIEWER, "publish", "orders", 2), WorkflowLifecycleState.PUBLISHED);
    assertState(
        service.deprecate(REVIEWER, "deprecate", "orders", 2), WorkflowLifecycleState.DEPRECATED);
    assertThrows(
        DefinitionManagementException.class,
        () -> service.submit(AUTHOR, "bad-submit", "orders", 2));
  }

  @Test
  void preventsAuthorFromApprovingOrPublishingOwnRevision() {
    service.create(AUTHOR, "create", "orders", "Orders", workflow("one"));
    service.submit(AUTHOR, "submit", "orders", 1);
    assertThrows(
        DefinitionManagementException.class,
        () -> service.approve(AUTHOR, "self-approve", "orders", 1));
    service.approve(REVIEWER, "approve", "orders", 1);
    assertThrows(
        DefinitionManagementException.class,
        () -> service.publish(AUTHOR, "self-publish", "orders", 1));
    assertTrue(
        service
            .retrieve(REVIEWER, "retrieve", "orders", 1)
            .resolvedDigest()
            .matches("[0-9a-f]{64}"));
  }

  @Test
  void resolvesAndRetainsExternalResourcesAtThePublicationBoundary() {
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: tests
          name: external-script
          version: '1.0.0'
        do:
          - execute:
              run:
                script:
                  language: python
                  source: {endpoint: https://contracts.test/job.py}
        """;
    service =
        new DefinitionManagementService(
            repository,
            new PermitAllAuthorization(),
            new OpenWorkflowCompiler(),
            request ->
                ResolvedWorkflowResource.of(request.uri(), "text/x-python", "print('pinned')"));

    ManagedWorkflowRevision revision =
        service.create(AUTHOR, "external", "external-script", "External script", source);

    assertEquals(1, revision.resolvedResources().size());
    assertEquals("print('pinned')", revision.resolvedResources().getFirst().content());
    assertEquals(
        revision.resolvedDigest(),
        new OpenWorkflowCompiler()
            .compile(
                source.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                revision.resolvedResources())
            .definitionSha256());
  }

  private static void assertState(
      ManagedWorkflowRevision revision, WorkflowLifecycleState expected) {
    assertEquals(expected, revision.lifecycleState());
  }

  private static ActiveOrganization actor(String id) {
    return new ActiveOrganization(TENANT, "organization", id, Set.of("definition-author"));
  }

  private static String workflow(String name) {
    return """
    document:
      dsl: '1.0.3'
      namespace: tests
      name: %s
      version: '1.0.0'
    do:
      - initialize:
          set:
            ready: true
    """
        .formatted(name);
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

  private static final class InMemoryRepository implements DefinitionRepository {
    private final Map<TenantId, List<ManagedWorkflowRevision>> revisions = new HashMap<>();

    @Override
    public boolean exists(TenantId tenantId, String definitionKey) {
      return list(tenantId).stream().anyMatch(item -> item.definitionKey().equals(definitionKey));
    }

    @Override
    public int nextRevisionNumber(TenantId tenantId, String definitionKey) {
      return list(tenantId).stream()
              .filter(item -> item.definitionKey().equals(definitionKey))
              .mapToInt(ManagedWorkflowRevision::revisionNumber)
              .max()
              .orElse(0)
          + 1;
    }

    @Override
    public void save(
        TenantId tenantId,
        ManagedWorkflowRevision revision,
        String actingActorId,
        String correlationId) {
      List<ManagedWorkflowRevision> tenant =
          revisions.computeIfAbsent(tenantId, ignored -> new ArrayList<>());
      tenant.removeIf(
          item ->
              item.definitionKey().equals(revision.definitionKey())
                  && item.revisionNumber() == revision.revisionNumber());
      tenant.add(revision);
      tenant.sort(
          java.util.Comparator.comparing(ManagedWorkflowRevision::definitionKey)
              .thenComparingInt(ManagedWorkflowRevision::revisionNumber));
    }

    @Override
    public Optional<ManagedWorkflowRevision> find(
        TenantId tenantId, String definitionKey, int revisionNumber) {
      return list(tenantId).stream()
          .filter(
              item ->
                  item.definitionKey().equals(definitionKey)
                      && item.revisionNumber() == revisionNumber)
          .findFirst();
    }

    @Override
    public List<ManagedWorkflowRevision> list(TenantId tenantId) {
      return List.copyOf(revisions.getOrDefault(tenantId, List.of()));
    }
  }
}
