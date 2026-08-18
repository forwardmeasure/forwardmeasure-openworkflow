package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

/** Remembered cluster-sharded registry for recoverable protocol transports. */
public final class ProtocolOperationCoordinatorSharding {
  public static final EntityTypeKey<ProtocolOperationCoordinatorCommand> TYPE_KEY =
      EntityTypeKey.create(
          ProtocolOperationCoordinatorCommand.class, "openworkflow-protocol-operation");
  private final ClusterSharding sharding;

  private ProtocolOperationCoordinatorSharding(ClusterSharding sharding) {
    this.sharding = sharding;
  }

  public static ProtocolOperationCoordinatorSharding initialize(
      ActorSystem<?> system, WorkflowSharding workflows, ProtocolTransport transport) {
    Objects.requireNonNull(system, "system");
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
                    ProtocolOperationCoordinatorEntity.create(
                        Coordinates.fromEntityId(context.getEntityId()), workflows, transport))
            .withSettings(settings)
            .withEntityProps(Props.empty().withMailboxFromConfig("openworkflow.entity-mailbox")));
    return new ProtocolOperationCoordinatorSharding(sharding);
  }

  public EntityRef<ProtocolOperationCoordinatorCommand> entityRef(
      ExecutionId executionId, String operationId) {
    return sharding.entityRefFor(TYPE_KEY, new Coordinates(executionId, operationId).entityId());
  }

  public record Coordinates(ExecutionId executionId, String operationId) {
    public Coordinates {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(operationId, "operationId");
      if (operationId.isBlank())
        throw new IllegalArgumentException("operationId must not be blank");
    }

    String entityId() {
      return Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(executionId.entityId().getBytes(StandardCharsets.UTF_8))
          + "."
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(operationId.getBytes(StandardCharsets.UTF_8));
    }

    static Coordinates fromEntityId(String entityId) {
      String[] parts = entityId.split("\\.", 2);
      if (parts.length != 2)
        throw new IllegalArgumentException("Invalid protocol coordinator entity ID");
      return new Coordinates(
          ExecutionId.fromEntityId(
              new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8)),
          new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
    }
  }
}
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
