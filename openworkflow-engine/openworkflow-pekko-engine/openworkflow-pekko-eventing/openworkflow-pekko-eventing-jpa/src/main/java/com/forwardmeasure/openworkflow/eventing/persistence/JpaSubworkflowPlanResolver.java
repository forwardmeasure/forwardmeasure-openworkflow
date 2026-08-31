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
package com.forwardmeasure.openworkflow.eventing.persistence;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.SubworkflowPlanResolver;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Resolves the exact immutable child plan pinned into an admitted parent workflow, straight from
 * the tenant schema - mirrors {@code JpaPublishedWorkflowResolver} exactly (raw native query
 * against {@code workflow}/{@code workflow_definition}/{@code workflow_publication}, recompile via
 * {@link OpenWorkflowCompiler}, verify the recompiled digest against the stored one), except keyed
 * by {@link ResolvedSubflow}'s coordinates+digest pair instead of a {@code revisionId} - a parent's
 * pinned subflow reference doesn't carry the child's revision UUID, only what it looked like when
 * the parent was compiled.
 */
public final class JpaSubworkflowPlanResolver implements SubworkflowPlanResolver {
  private final EntityManager entityManager;
  private final OpenWorkflowCompiler compiler = new OpenWorkflowCompiler();

  public JpaSubworkflowPlanResolver(EntityManager entityManager) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
  }

  @Override
  public WorkflowPlan resolve(TenantId tenantId, ActorIdentity actor, ResolvedSubflow subflow) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(subflow, "subflow");
    String schema =
        TenantSchema.forTenant(new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value()))
            .value();
    List<?> rows =
        entityManager
            .createNativeQuery(
                "select r.source_document,r.resolved_resources,r.resolved_digest,r.source_digest"
                    + " from "
                    + schema
                    + ".workflow_definition r join "
                    + schema
                    + ".workflow_publication p on p.definition_id=r.id join "
                    + schema
                    + ".workflow w on w.id=r.workflow_id"
                    + " where w.name=?1 and r.namespace=?2 and r.document_version=?3"
                    + " and r.specification_version=?4 and r.lifecycle_state='PUBLISHED'"
                    + " and p.deprecated_at is null")
            .setParameter(1, subflow.coordinates().name())
            .setParameter(2, subflow.coordinates().namespace())
            .setParameter(3, subflow.coordinates().version())
            .setParameter(4, subflow.coordinates().dsl())
            .setMaxResults(1)
            .getResultList();
    if (rows.isEmpty()) {
      throw new SubworkflowNotPublishedException(
          "no active published revision for pinned subflow " + subflow.coordinates());
    }
    Object[] row = (Object[]) rows.getFirst();
    String resolvedDigest = row[2].toString();
    String sourceDigest = row[3].toString();
    if (!resolvedDigest.equals(subflow.definitionSha256())
        || !sourceDigest.equals(subflow.sourceSha256())) {
      throw new SubworkflowNotPublishedException(
          "currently published revision no longer matches the parent's pinned subflow digest for "
              + subflow.coordinates());
    }
    WorkflowPlan plan =
        compiler.compile(
            row[0].toString().getBytes(StandardCharsets.UTF_8),
            WorkflowResourceBundleCodec.decode(row[1].toString()));
    if (!resolvedDigest.equals(plan.definitionSha256())
        || !sourceDigest.equals(plan.sourceSha256())) {
      throw new SubworkflowNotPublishedException(
          "stored immutable revision does not match its own digest for " + subflow.coordinates());
    }
    return plan;
  }

  public static final class SubworkflowNotPublishedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SubworkflowNotPublishedException(String message) {
      super(message);
    }
  }
}
