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
package com.forwardmeasure.openworkflow.execution.persistence;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementException;
import com.forwardmeasure.openworkflow.execution.management.PublishedWorkflow;
import com.forwardmeasure.openworkflow.execution.management.PublishedWorkflowResolver;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Resolves the immutable active publication from the caller's tenant schema. */
public final class JpaPublishedWorkflowResolver implements PublishedWorkflowResolver {
  private final com.forwardmeasure.openworkflow.engine.api.TenantId tenantId;
  private final String schema;
  private final EntityManager entityManager;
  private final OpenWorkflowCompiler compiler;

  public JpaPublishedWorkflowResolver(TenantId tenantId, EntityManager entityManager) {
    Objects.requireNonNull(tenantId, "tenantId");
    this.tenantId = new com.forwardmeasure.openworkflow.engine.api.TenantId(tenantId.value());
    this.schema = TenantSchema.forTenant(tenantId).value();
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.compiler = new OpenWorkflowCompiler();
  }

  @Override
  public PublishedWorkflow resolveAuthorizedPublished(
      TenantActorContext context, UUID revisionId, String correlationId) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(revisionId, "revisionId");
    if (!tenantId.equals(context.tenantId())) {
      throw notPublished();
    }
    var rows =
        entityManager
            .createNativeQuery(
                "select"
                    + " r.source_document,r.resolved_document,r.resolved_resources,r.resolved_digest,p.revision_digest"
                    + " from "
                    + schema
                    + ".workflow_revision r join "
                    + schema
                    + ".workflow_publication p on p.revision_id=r.id"
                    + " where r.uuid=?1 and r.lifecycle_state='PUBLISHED'"
                    + " and p.deprecated_at is null")
            .setParameter(1, revisionId)
            .setMaxResults(1)
            .getResultList();
    if (rows.isEmpty()) {
      throw notPublished();
    }
    Object[] row = (Object[]) rows.getFirst();
    String resolvedDigest = row[3].toString();
    if (!resolvedDigest.equals(row[4].toString())) {
      throw new ExecutionManagementException(
          ExecutionManagementException.Kind.NOT_PUBLISHED,
          "publication digest does not match its immutable revision");
    }
    var plan =
        compiler.compile(
            row[0].toString().getBytes(StandardCharsets.UTF_8),
            WorkflowResourceBundleCodec.decode(row[2].toString()));
    if (!resolvedDigest.equals(plan.definitionSha256())
        || !row[1].toString().equals(plan.definition().toString())) {
      throw new ExecutionManagementException(
          ExecutionManagementException.Kind.NOT_PUBLISHED,
          "stored immutable revision does not match its publication digest");
    }
    return new PublishedWorkflow(
        DefinitionRevision.from(revisionId, plan), plan, row[0].toString());
  }

  private static ExecutionManagementException notPublished() {
    return new ExecutionManagementException(
        ExecutionManagementException.Kind.NOT_PUBLISHED,
        "revision is not an active published immutable revision");
  }
}
