/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.jpa.core.entity.AbstractBaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** Optimistically locked current projection of one Human Task aggregate. */
@Entity
@Table(name = "human_task")
@Getter
@Setter
@SuppressWarnings("serial")
public class HumanTaskEntity extends AbstractBaseEntity<Long> {
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "human_task_ids")
  @SequenceGenerator(
      name = "human_task_ids",
      sequenceName = "human_task_id_seq",
      allocationSize = 50)
  private Long id;

  @Column(name = "task_id", nullable = false, unique = true, length = 255)
  private String taskId;

  @Column(name = "domain_revision", nullable = false)
  private long domainRevision;

  @Column(nullable = false, length = 32)
  private String status;

  @Column(name = "task_type", nullable = false, length = 255)
  private String taskType;

  @Column(nullable = false, length = 1024)
  private String title;

  @Column(nullable = false)
  private int priority;

  @Column(name = "source_kind", nullable = false, length = 32)
  private String sourceKind;

  @Column(name = "source_id", nullable = false, length = 512)
  private String sourceId;

  @Column(name = "stage_id", nullable = false, length = 255)
  private String stageId;

  @Column(name = "assignment_kind", length = 32)
  private String assignmentKind;

  @Column(name = "assignment_principal", length = 512)
  private String assignmentPrincipal;

  @Column(name = "reviewer_id", length = 512)
  private String reviewerId;

  @Column(name = "received_at", nullable = false)
  private Instant receivedAt;

  @Column(name = "due_at")
  private Instant dueAt;

  @Column(name = "expires_at")
  private Instant expiresAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(nullable = false, columnDefinition = "jsonb")
  private JsonNode snapshot;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "blotter_fields", nullable = false, columnDefinition = "jsonb")
  private JsonNode blotterFields;

  public HumanTaskEntity() {}

  public Long id() {
    return id;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public void setId(Long id) {
    this.id = id;
  }

  public String taskId() {
    return taskId;
  }

  public long domainRevision() {
    return domainRevision;
  }

  public JsonNode snapshot() {
    return snapshot.deepCopy();
  }

  public Instant receivedAt() {
    return receivedAt;
  }

  public Instant updatedAt() {
    return updatedAt;
  }

  public String status() {
    return status;
  }

  public String taskType() {
    return taskType;
  }

  public String sourceId() {
    return sourceId;
  }

  public int priority() {
    return priority;
  }

  public String assignmentPrincipal() {
    return assignmentPrincipal;
  }

  public String reviewerId() {
    return reviewerId;
  }

  public Instant dueAt() {
    return dueAt;
  }

  public JsonNode blotterFields() {
    return blotterFields.deepCopy();
  }

  public void replace(
      String taskId,
      long domainRevision,
      String status,
      String taskType,
      String title,
      int priority,
      String sourceKind,
      String sourceId,
      String stageId,
      String assignmentKind,
      String assignmentPrincipal,
      String reviewerId,
      Instant receivedAt,
      Instant dueAt,
      Instant expiresAt,
      Instant updatedAt,
      JsonNode snapshot,
      JsonNode blotterFields) {
    this.taskId = taskId;
    this.domainRevision = domainRevision;
    this.status = status;
    this.taskType = taskType;
    this.title = title;
    this.priority = priority;
    this.sourceKind = sourceKind;
    this.sourceId = sourceId;
    this.stageId = stageId;
    this.assignmentKind = assignmentKind;
    this.assignmentPrincipal = assignmentPrincipal;
    this.reviewerId = reviewerId;
    this.receivedAt = receivedAt;
    this.dueAt = dueAt;
    this.expiresAt = expiresAt;
    this.updatedAt = updatedAt;
    this.snapshot = snapshot.deepCopy();
    this.blotterFields = blotterFields.deepCopy();
  }
}
