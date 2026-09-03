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

/** Transactional-outbox row for task-created and terminal outcome delivery. */
@Entity
@Table(name = "human_task_outbox")
@Getter
@Setter
public class HumanTaskOutboxEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "human_task_outbox_ids")
  @SequenceGenerator(
      name = "human_task_outbox_ids",
      sequenceName = "human_task_outbox_id_seq",
      allocationSize = 50)
  private Long id;

  @Column(name = "message_id", nullable = false, unique = true, length = 255)
  private String messageId;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "task_id", nullable = false)
  private HumanTaskEntity task;

  @Column(name = "workflow_correlation", length = 512)
  private String workflowCorrelation;

  @Column(name = "task_path", length = 1024)
  private String taskPath;

  @Column(name = "message_type", nullable = false, length = 64)
  private String messageType;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(nullable = false, columnDefinition = "jsonb")
  private JsonNode payload;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "published_at")
  private Instant publishedAt;

  @Column(name = "attempt_count", nullable = false)
  private int attemptCount;

  protected HumanTaskOutboxEntity() {}

  public HumanTaskOutboxEntity(
      HumanTaskEntity task,
      String messageId,
      String workflowCorrelation,
      String taskPath,
      String messageType,
      JsonNode payload,
      Instant createdAt) {
    this.task = task;
    this.messageId = messageId;
    this.workflowCorrelation = workflowCorrelation;
    this.taskPath = taskPath;
    this.messageType = messageType;
    this.payload = payload.deepCopy();
    this.createdAt = createdAt;
  }
}
