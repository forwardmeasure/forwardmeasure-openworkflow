/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

/** Typed domain rejection; rejected commands emit no events. */
public final class HumanTaskTransitionException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Failure failure;

  public HumanTaskTransitionException(Failure failure, String message) {
    super(message);
    this.failure = failure;
  }

  public Failure failure() {
    return failure;
  }

  public enum Failure {
    NOT_FOUND,
    ALREADY_EXISTS,
    REVISION_CONFLICT,
    ILLEGAL_TRANSITION,
    INELIGIBLE_REVIEWER,
    ASSIGNMENT_CONFLICT,
    LEASE_CONFLICT,
    LEASE_EXPIRED,
    STALE_CONTENT,
    UNKNOWN_ACTION,
    VALIDATION
  }
}
