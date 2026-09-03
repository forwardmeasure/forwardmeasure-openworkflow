/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain.entity;

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

/** One finite review-ownership interval; closed intervals remain immutable audit evidence. */
@Entity
@Table(name = "human_task_review_session")
@Getter
@Setter
public class HumanTaskReviewSessionEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "human_task_review_session_ids")
  @SequenceGenerator(
      name = "human_task_review_session_ids",
      sequenceName = "human_task_review_session_id_seq",
      allocationSize = 50)
  private Long id;

  @Column(name = "review_session_id", nullable = false, unique = true, length = 255)
  private String reviewSessionId;

  @ManyToOne(fetch = FetchType.LAZY, optional = false)
  @JoinColumn(name = "task_id", nullable = false)
  private HumanTaskEntity task;

  @Column(name = "stage_id", nullable = false, length = 255)
  private String stageId;

  @Column(name = "held_by", nullable = false, length = 512)
  private String heldBy;

  @Column(name = "acquired_at", nullable = false)
  private Instant acquiredAt;

  @Column(name = "last_renewed_at", nullable = false)
  private Instant lastRenewedAt;

  @Column(name = "expires_at", nullable = false)
  private Instant expiresAt;

  @Column(name = "released_at")
  private Instant releasedAt;

  @Column(name = "lease_token_digest", nullable = false, length = 64)
  private String leaseTokenDigest;

  protected HumanTaskReviewSessionEntity() {}

  public HumanTaskReviewSessionEntity(
      HumanTaskEntity task,
      String reviewSessionId,
      String stageId,
      String heldBy,
      Instant acquiredAt,
      Instant lastRenewedAt,
      Instant expiresAt,
      String leaseTokenDigest) {
    this.task = task;
    this.reviewSessionId = reviewSessionId;
    this.stageId = stageId;
    this.heldBy = heldBy;
    this.acquiredAt = acquiredAt;
    this.lastRenewedAt = lastRenewedAt;
    this.expiresAt = expiresAt;
    this.leaseTokenDigest = leaseTokenDigest;
  }

  public void renew(Instant renewedAt, Instant newExpiresAt) {
    this.lastRenewedAt = renewedAt;
    this.expiresAt = newExpiresAt;
  }

  public void release(Instant at) {
    this.releasedAt = at;
  }
}
