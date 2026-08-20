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
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

/** Cluster-sharded registry for durable child workflow coordinators. */
public final class SubworkflowCoordinatorSharding {
  public static final EntityTypeKey<SubworkflowCoordinatorCommand> TYPE_KEY =
      EntityTypeKey.create(SubworkflowCoordinatorCommand.class, "openworkflow-subflow-coordinator");

  private final ClusterSharding sharding;

  private SubworkflowCoordinatorSharding(ClusterSharding sharding) {
    this.sharding = sharding;
  }

  public static SubworkflowCoordinatorSharding initialize(
      ActorSystem<?> system, WorkflowSharding workflows) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(workflows, "workflows");
    ClusterSharding sharding = ClusterSharding.get(system);
    var settings =
        ClusterShardingSettings.create(system)
            .withRememberEntities(true)
            .withRememberEntitiesStoreMode(
                ClusterShardingSettings.rememberEntitiesStoreModeEventSourced());
    sharding.init(
        Entity.of(
                TYPE_KEY,
                context ->
                    SubworkflowCoordinatorEntity.create(
                        ExecutionId.fromEntityId(context.getEntityId()), workflows))
            .withSettings(settings)
            .withEntityProps(Props.empty().withMailboxFromConfig("openworkflow.entity-mailbox")));
    return new SubworkflowCoordinatorSharding(sharding);
  }

  public EntityRef<SubworkflowCoordinatorCommand> entityRef(ExecutionId childExecutionId) {
    Objects.requireNonNull(childExecutionId, "childExecutionId");
    return sharding.entityRefFor(TYPE_KEY, childExecutionId.entityId());
  }
}
