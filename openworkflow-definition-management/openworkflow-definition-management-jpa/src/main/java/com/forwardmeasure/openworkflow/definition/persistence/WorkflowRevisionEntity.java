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
package com.forwardmeasure.openworkflow.definition.persistence;

import com.forwardmeasure.jpa.core.entity.AuditedEntity;
import com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState;
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
import java.util.Objects;
import java.util.regex.Pattern;

@Entity
@Table(name = "workflow_revision")
public class WorkflowRevisionEntity extends AuditedEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;
  private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_revision_ids")
  @SequenceGenerator(
      name = "workflow_revision_ids",
      sequenceName = "workflow_revision_id_seq",
      allocationSize = 1)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "definition_id", nullable = false, updatable = false)
  private WorkflowDefinitionEntity definition;

  @Column(name = "revision_number", nullable = false, updatable = false)
  private int revisionNumber;

  @Enumerated(EnumType.STRING)
  @Column(name = "lifecycle_state", nullable = false, length = 32)
  private WorkflowLifecycleState lifecycleState;

  @Column(name = "source_document", nullable = false, updatable = false, columnDefinition = "text")
  private String sourceDocument;

  @Column(
      name = "resolved_document",
      nullable = false,
      updatable = false,
      columnDefinition = "text")
  private String resolvedDocument;

  @Column(
      name = "resolved_resources",
      nullable = false,
      updatable = false,
      columnDefinition = "text")
  private String resolvedResources;

  @Column(name = "specification_version", nullable = false, updatable = false, length = 32)
  private String specificationVersion;

  @Column(name = "compiler_profile", nullable = false, updatable = false, length = 80)
  private String compilerProfile;

  @Column(name = "source_digest", nullable = false, updatable = false, length = 64)
  private String sourceDigest;

  @Column(name = "resolved_digest", nullable = false, updatable = false, length = 64)
  private String resolvedDigest;

  @Column(name = "author_actor_id", nullable = false, updatable = false)
  private long authorActorId;

  protected WorkflowRevisionEntity() {}

  public WorkflowRevisionEntity(
      WorkflowDefinitionEntity definition,
      int revisionNumber,
      String sourceDocument,
      String resolvedDocument,
      String resolvedResources,
      String specificationVersion,
      String compilerProfile,
      String sourceDigest,
      String resolvedDigest,
      long authorActorId) {
    if (revisionNumber < 1 || authorActorId < 1) {
      throw new IllegalArgumentException("revisionNumber and authorActorId must be positive");
    }
    this.definition = Objects.requireNonNull(definition, "definition");
    this.revisionNumber = revisionNumber;
    this.lifecycleState = WorkflowLifecycleState.DRAFT;
    this.sourceDocument = required(sourceDocument, "sourceDocument");
    this.resolvedDocument = required(resolvedDocument, "resolvedDocument");
    this.resolvedResources = required(resolvedResources, "resolvedResources");
    this.specificationVersion = required(specificationVersion, "specificationVersion");
    this.compilerProfile = required(compilerProfile, "compilerProfile");
    this.sourceDigest = digest(sourceDigest, "sourceDigest");
    this.resolvedDigest = digest(resolvedDigest, "resolvedDigest");
    this.authorActorId = authorActorId;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public void setId(Long id) {
    this.id = id;
  }

  public WorkflowDefinitionEntity getDefinition() {
    return definition;
  }

  public int getRevisionNumber() {
    return revisionNumber;
  }

  public WorkflowLifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public String getSourceDocument() {
    return sourceDocument;
  }

  public String getResolvedDocument() {
    return resolvedDocument;
  }

  public String getResolvedResources() {
    return resolvedResources;
  }

  public String getSourceDigest() {
    return sourceDigest;
  }

  public String getSpecificationVersion() {
    return specificationVersion;
  }

  public String getCompilerProfile() {
    return compilerProfile;
  }

  public String getResolvedDigest() {
    return resolvedDigest;
  }

  public long getAuthorActorId() {
    return authorActorId;
  }

  public void transitionTo(WorkflowLifecycleState state) {
    lifecycleState = Objects.requireNonNull(state, "state");
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
