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
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.util.Objects;
import java.util.Optional;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

/** Tenant-safe Pekko Cluster Sharding registry for durable workflow entities. */
public final class WorkflowSharding {
  public static final EntityTypeKey<WorkflowCommand> TYPE_KEY =
      EntityTypeKey.create(WorkflowCommand.class, "openworkflow-execution");

  private final ClusterSharding sharding;

  private WorkflowSharding(ClusterSharding sharding) {
    this.sharding = sharding;
  }

  public static WorkflowSharding initialize(ActorSystem<?> system) {
    return initialize(system, "", Optional.empty());
  }

  /** Registers a cluster-wide proxy while restricting entity hosting to the requested role. */
  public static WorkflowSharding initialize(ActorSystem<?> system, String role) {
    return initialize(system, role, Optional.empty());
  }

  /**
   * @param postgresConnection empty for the Cassandra profile (or a test harness with no per-tenant
   *     Postgres routing needed); present for the Postgres profile, so each entity resolves its own
   *     tenant-scoped journal/snapshot plugin - see {@link WorkflowEntity}'s plugin overrides.
   */
  public static WorkflowSharding initialize(
      ActorSystem<?> system, String role, Optional<PostgresConnectionSettings> postgresConnection) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(postgresConnection, "postgresConnection");
    ClusterSharding sharding = ClusterSharding.get(system);
    Entity<
            WorkflowCommand,
            org.apache.pekko.cluster.sharding.typed.ShardingEnvelope<WorkflowCommand>>
        entity =
            Entity.of(
                    TYPE_KEY,
                    context ->
                        WorkflowEntity.create(
                            ExecutionId.fromEntityId(context.getEntityId()), postgresConnection))
                .withEntityProps(
                    Props.empty().withMailboxFromConfig("openworkflow.entity-mailbox"));
    if (role != null && !role.isBlank()) entity = entity.withRole(role);
    sharding.init(entity);
    return new WorkflowSharding(sharding);
  }

  public EntityRef<WorkflowCommand> entityRef(ExecutionId executionId) {
    Objects.requireNonNull(executionId, "executionId");
    return sharding.entityRefFor(TYPE_KEY, executionId.entityId());
  }
}
