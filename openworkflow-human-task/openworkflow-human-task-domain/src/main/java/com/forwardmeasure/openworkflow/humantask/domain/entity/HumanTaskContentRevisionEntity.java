/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain.entity;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** Immutable full-result content revision plus optional change evidence. */
@Entity
@Table(name = "human_task_content_revision")
@Getter
@Setter
public class HumanTaskContentRevisionEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "human_task_revision_ids")
  @SequenceGenerator(
      name = "human_task_revision_ids",
      sequenceName = "human_task_content_revision_id_seq",
      allocationSize = 50)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "task_id", nullable = false)
  private HumanTaskEntity task;

  @Column(name = "content_revision", nullable = false)
  private long contentRevision;

  @Column(name = "based_on_revision", nullable = false)
  private long basedOnRevision;

  @Column(name = "created_by", nullable = false, length = 512)
  private String createdBy;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "review_session_id", length = 255)
  private String reviewSessionId;

  @Column(name = "before_sha256", length = 64)
  private String beforeSha256;

  @Column(name = "after_sha256", nullable = false, length = 64)
  private String afterSha256;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "json_patch_reference", columnDefinition = "jsonb")
  private JsonNode jsonPatchReference;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "result_content_reference", nullable = false, columnDefinition = "jsonb")
  private JsonNode resultContentReference;

  @Column(columnDefinition = "text")
  private String comment;

  protected HumanTaskContentRevisionEntity() {}

  public HumanTaskContentRevisionEntity(
      HumanTaskEntity task,
      long contentRevision,
      long basedOnRevision,
      String createdBy,
      Instant createdAt,
      String reviewSessionId,
      String beforeSha256,
      String afterSha256,
      JsonNode jsonPatchReference,
      JsonNode resultContentReference,
      String comment) {
    this.task = task;
    this.contentRevision = contentRevision;
    this.basedOnRevision = basedOnRevision;
    this.createdBy = createdBy;
    this.createdAt = createdAt;
    this.reviewSessionId = reviewSessionId;
    this.beforeSha256 = beforeSha256;
    this.afterSha256 = afterSha256;
    this.jsonPatchReference = jsonPatchReference == null ? null : jsonPatchReference.deepCopy();
    this.resultContentReference = resultContentReference.deepCopy();
    this.comment = comment;
  }

  public long contentRevision() {
    return contentRevision;
  }

  public long basedOnRevision() {
    return basedOnRevision;
  }

  public String createdBy() {
    return createdBy;
  }

  public Instant createdAt() {
    return createdAt;
  }

  public String reviewSessionId() {
    return reviewSessionId;
  }

  public String beforeSha256() {
    return beforeSha256;
  }

  public String afterSha256() {
    return afterSha256;
  }

  public JsonNode jsonPatchReference() {
    return jsonPatchReference == null ? null : jsonPatchReference.deepCopy();
  }

  public JsonNode resultContentReference() {
    return resultContentReference.deepCopy();
  }

  public String comment() {
    return comment;
  }
}
