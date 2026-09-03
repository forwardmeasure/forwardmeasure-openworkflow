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

/** Append-only serialized domain event. */
@Entity
@Table(name = "human_task_event")
@Getter
@Setter
public class HumanTaskEventEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "human_task_event_ids")
  @SequenceGenerator(
      name = "human_task_event_ids",
      sequenceName = "human_task_event_id_seq",
      allocationSize = 50)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "task_id", nullable = false)
  private HumanTaskEntity task;

  @Column(name = "sequence_number", nullable = false)
  private long sequence;

  @Column(name = "command_id", nullable = false, length = 255)
  private String commandId;

  @Column(name = "event_type", nullable = false, length = 64)
  private String eventType;

  @Column(name = "actor_id", nullable = false, length = 512)
  private String actorId;

  @Column(name = "occurred_at", nullable = false)
  private Instant occurredAt;

  @JdbcTypeCode(SqlTypes.JSON)
  @Column(name = "event_data", nullable = false, columnDefinition = "jsonb")
  private JsonNode eventData;

  protected HumanTaskEventEntity() {}

  public HumanTaskEventEntity(
      HumanTaskEntity task,
      long sequence,
      String commandId,
      String eventType,
      String actorId,
      Instant occurredAt,
      JsonNode eventData) {
    this.task = task;
    this.sequence = sequence;
    this.commandId = commandId;
    this.eventType = eventType;
    this.actorId = actorId;
    this.occurredAt = occurredAt;
    this.eventData = eventData.deepCopy();
  }

  public long sequence() {
    return sequence;
  }

  public JsonNode eventData() {
    return eventData.deepCopy();
  }
}
