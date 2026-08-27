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

import com.forwardmeasure.jpa.core.entity.AuditedEntity;
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
import jakarta.persistence.OneToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.io.Serial;
import java.util.Objects;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** One governed, versioned revision of a {@link Workflow}'s Open Workflow source document. */
@Entity
@Table(name = "workflow_definition")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class WorkflowDefinition extends AuditedEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;
  private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_definition_ids")
  @SequenceGenerator(
      name = "workflow_definition_ids",
      sequenceName = "workflow_definition_id_seq",
      allocationSize = 50)
  @Setter
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "workflow_id", nullable = false)
  private Workflow workflow;

  @Column(name = "revision_number", nullable = false)
  private int revisionNumber;

  @Enumerated(EnumType.STRING)
  @Column(name = "lifecycle_state", nullable = false, length = 32)
  private WorkflowLifecycleState lifecycleState;

  @Column(name = "source_document", nullable = false, columnDefinition = "text")
  private String sourceDocument;

  @Column(name = "resolved_document", nullable = false, columnDefinition = "text")
  private String resolvedDocument;

  @Column(name = "resolved_resources", nullable = false, columnDefinition = "text")
  private String resolvedResources;

  @Column(name = "namespace", nullable = false, length = 255)
  private String namespace;

  @Column(name = "document_version", nullable = false, length = 128)
  private String documentVersion;

  @Column(name = "specification_version", nullable = false, length = 32)
  private String specificationVersion;

  @Column(name = "compiler_profile", nullable = false, length = 80)
  private String compilerProfile;

  @Column(name = "source_digest", nullable = false, length = 64)
  private String sourceDigest;

  @Column(name = "resolved_digest", nullable = false, length = 64)
  private String resolvedDigest;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "author_actor_id", nullable = false)
  private Actor author;

  /**
   * Inverse side of {@link WorkflowPublication#getDefinition()}. Lets the response mapper reach
   * {@code published_by}/{@code published_at}/{@code deprecated_at} off the entity it already has,
   * instead of {@code -jaxrs} querying {@link
   * com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowPublicationRepository}
   * directly.
   */
  @OneToOne(mappedBy = "definition", fetch = FetchType.LAZY)
  private WorkflowPublication publication;

  public WorkflowDefinition(
      Workflow workflow,
      int revisionNumber,
      String sourceDocument,
      String resolvedDocument,
      String resolvedResources,
      String namespace,
      String documentVersion,
      String specificationVersion,
      String compilerProfile,
      String sourceDigest,
      String resolvedDigest,
      Actor author) {
    if (revisionNumber < 1) {
      throw new IllegalArgumentException("revisionNumber must be positive");
    }
    this.workflow = Objects.requireNonNull(workflow, "workflow");
    this.revisionNumber = revisionNumber;
    this.lifecycleState = WorkflowLifecycleState.DRAFT;
    this.author = Objects.requireNonNull(author, "author");
    this.sourceDocument = required(sourceDocument, "sourceDocument");
    this.resolvedDocument = required(resolvedDocument, "resolvedDocument");
    this.resolvedResources = required(resolvedResources, "resolvedResources");
    this.namespace = required(namespace, "namespace");
    this.documentVersion = required(documentVersion, "documentVersion");
    this.specificationVersion = required(specificationVersion, "specificationVersion");
    this.compilerProfile = required(compilerProfile, "compilerProfile");
    this.sourceDigest = digest(sourceDigest, "sourceDigest");
    this.resolvedDigest = digest(resolvedDigest, "resolvedDigest");
  }

  /**
   * Replaces this revision's compiled content in place. All nine fields change together because
   * they are all derived from compiling one {@code sourceDocument} — unlike independent metadata
   * fields, there is no meaningful way to change one without the rest, so this stays one method
   * rather than individual setters. The database trigger enforces this is only ever effective while
   * the revision is {@code DRAFT}; callers still guard the transition themselves for a clean
   * 409/422 instead of relying on the trigger's exception.
   */
  public void setContent(
      String sourceDocument,
      String resolvedDocument,
      String resolvedResources,
      String namespace,
      String documentVersion,
      String specificationVersion,
      String compilerProfile,
      String sourceDigest,
      String resolvedDigest) {
    this.sourceDocument = required(sourceDocument, "sourceDocument");
    this.resolvedDocument = required(resolvedDocument, "resolvedDocument");
    this.resolvedResources = required(resolvedResources, "resolvedResources");
    this.namespace = required(namespace, "namespace");
    this.documentVersion = required(documentVersion, "documentVersion");
    this.specificationVersion = required(specificationVersion, "specificationVersion");
    this.compilerProfile = required(compilerProfile, "compilerProfile");
    this.sourceDigest = digest(sourceDigest, "sourceDigest");
    this.resolvedDigest = digest(resolvedDigest, "resolvedDigest");
  }

  public void transitionTo(WorkflowLifecycleState state) {
    this.lifecycleState = Objects.requireNonNull(state, "state");
  }

  /**
   * Keeps this side of the {@code publication} association in sync within the same persistence
   * context - {@code publication} is the inverse ({@code mappedBy}) side, so persisting a new
   * {@link WorkflowPublication} elsewhere never updates it here on its own; JPA requires the caller
   * to maintain both sides of a bidirectional association itself.
   */
  public void attachPublication(WorkflowPublication publication) {
    this.publication = Objects.requireNonNull(publication, "publication");
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
