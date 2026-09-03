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
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** Durable command idempotency receipt including the exact response state. */
@Entity
@Table(name = "human_task_command_receipt")
@Getter
@Setter
public class HumanTaskCommandReceiptEntity {
  @Id
  @Column(name = "command_id", length = 255)
  private String commandId;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "task_id", nullable = false)
  private HumanTaskEntity task;

  @Column(name = "command_type", nullable = false, length = 64)
  private String commandType;

  @Column(name = "request_sha256", nullable = false, length = 64)
  private String requestSha256;

  @Column(name = "result_revision", nullable = false)
  private long resultRevision;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "resulting_state", nullable = false, columnDefinition = "jsonb")
  private JsonNode resultingState;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  protected HumanTaskCommandReceiptEntity() {}

  public HumanTaskCommandReceiptEntity(
      HumanTaskEntity task,
      String commandId,
      String commandType,
      String requestSha256,
      long resultRevision,
      JsonNode resultingState,
      Instant createdAt) {
    this.task = task;
    this.commandId = commandId;
    this.commandType = commandType;
    this.requestSha256 = requestSha256;
    this.resultRevision = resultRevision;
    this.resultingState = resultingState.deepCopy();
    this.createdAt = createdAt;
  }

  public String commandId() {
    return commandId;
  }

  public String requestSha256() {
    return requestSha256;
  }

  public JsonNode resultingState() {
    return resultingState.deepCopy();
  }

  public Instant createdAt() {
    return createdAt;
  }
}
