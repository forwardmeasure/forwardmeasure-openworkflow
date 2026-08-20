/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.definition.domain.entity;

import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import com.forwardmeasure.jpa.identity.entity.Actor;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.io.Serial;
import java.time.OffsetDateTime;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** One full audit trail entry for every lifecycle transition a definition goes through. */
@Entity
@Table(name = "workflow_lifecycle_history")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WorkflowLifecycleHistory extends AbstractBaseEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_lifecycle_history_ids")
  @SequenceGenerator(
      name = "workflow_lifecycle_history_ids",
      sequenceName = "workflow_lifecycle_history_id_seq",
      allocationSize = 50)
  @Setter
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "definition_id", nullable = false)
  private WorkflowDefinition definition;

  @Enumerated(EnumType.STRING)
  @Column(name = "from_state", length = 32)
  private WorkflowLifecycleState fromState;

  @Enumerated(EnumType.STRING)
  @Column(name = "to_state", nullable = false, length = 32)
  private WorkflowLifecycleState toState;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "actor_id", nullable = false)
  private Actor actor;

  @Column(name = "correlation_id", nullable = false, length = 160)
  private String correlationId;

  @Column(name = "created_at", nullable = false)
  private OffsetDateTime createdAt;

  public WorkflowLifecycleHistory(
      WorkflowDefinition definition,
      WorkflowLifecycleState fromState,
      WorkflowLifecycleState toState,
      Actor actor,
      String correlationId) {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.fromState = fromState;
    this.toState = Objects.requireNonNull(toState, "toState");
    this.actor = Objects.requireNonNull(actor, "actor");
    this.correlationId = required(correlationId, "correlationId");
    this.createdAt = OffsetDateTime.now();
  }

  private static String required(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
