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

import java.util.Objects;
import java.util.Optional;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

/** Cluster Sharding registry for tenant-qualified durable schedule entities. */
public final class WorkflowScheduleSharding {
  public static final EntityTypeKey<ScheduleCommand> TYPE_KEY =
      EntityTypeKey.create(ScheduleCommand.class, "openworkflow-schedule");

  private final ClusterSharding sharding;

  private WorkflowScheduleSharding(ClusterSharding sharding) {
    this.sharding = sharding;
  }

  public static WorkflowScheduleSharding initialize(
      ActorSystem<?> system, ActorRef<ScheduledExecutionRequest> dispatch) {
    return initialize(system, dispatch, Optional.empty());
  }

  public static WorkflowScheduleSharding initialize(
      ActorSystem<?> system,
      ActorRef<ScheduledExecutionRequest> dispatch,
      Optional<PostgresConnectionSettings> postgresConnection) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(dispatch, "dispatch");
    Objects.requireNonNull(postgresConnection, "postgresConnection");
    ClusterSharding sharding = ClusterSharding.get(system);
    sharding.init(
        Entity.of(
                TYPE_KEY,
                context ->
                    WorkflowScheduleEntity.create(
                        ScheduleId.fromEntityId(context.getEntityId()),
                        dispatch,
                        postgresConnection))
            .withEntityProps(Props.empty().withMailboxFromConfig("openworkflow.entity-mailbox")));
    return new WorkflowScheduleSharding(sharding);
  }

  public EntityRef<ScheduleCommand> entityRef(ScheduleId scheduleId) {
    Objects.requireNonNull(scheduleId, "scheduleId");
    return sharding.entityRefFor(TYPE_KEY, scheduleId.entityId());
  }
}
