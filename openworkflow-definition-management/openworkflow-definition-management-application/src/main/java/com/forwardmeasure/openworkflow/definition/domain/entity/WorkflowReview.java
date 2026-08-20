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
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** One maker-checker approve/reject decision, bound to the definition's exact resolved digest. */
@Entity
@Table(name = "workflow_review")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WorkflowReview extends AbstractBaseEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;
  private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_review_ids")
  @SequenceGenerator(
      name = "workflow_review_ids",
      sequenceName = "workflow_review_id_seq",
      allocationSize = 50)
  @Setter
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "definition_id", nullable = false)
  private WorkflowDefinition definition;

  @Column(name = "review_action", nullable = false, length = 32)
  private String reviewAction;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "actor_id", nullable = false)
  private Actor actor;

  @Column(name = "definition_digest", nullable = false, length = 64)
  private String definitionDigest;

  @Column(name = "reason", length = 2000)
  private String reason;

  @Column(name = "created_at", nullable = false)
  private OffsetDateTime createdAt;

  public WorkflowReview(
      WorkflowDefinition definition,
      String reviewAction,
      Actor actor,
      String definitionDigest,
      String reason) {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.reviewAction = required(reviewAction, "reviewAction");
    this.actor = Objects.requireNonNull(actor, "actor");
    this.definitionDigest = digest(definitionDigest, "definitionDigest");
    this.reason = reason;
    this.createdAt = OffsetDateTime.now();
  }

  private static String required(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String digest(String value, String name) {
    if (!SHA_256.matcher(required(value, name)).matches()) {
      throw new IllegalArgumentException(name + " must be a lowercase SHA-256 digest");
    }
    return value;
  }
}
