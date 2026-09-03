/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Transactional read application over the tenant-bound repository port. */
public final class HumanTaskQueryService {
  private final HumanTaskTransactionExecutor transactions;

  public HumanTaskQueryService(HumanTaskTransactionExecutor transactions) {
    this.transactions = Objects.requireNonNull(transactions, "transactions");
  }

  public Optional<HumanTaskView> find(HumanTaskId taskId) {
    return transactions.execute(repository -> repository.findView(taskId));
  }

  public HumanTaskPage list(HumanTaskListQuery query) {
    return transactions.execute(repository -> repository.list(query));
  }

  public HumanTaskHistoryPage history(HumanTaskId taskId, long afterSequence, int limit) {
    if (afterSequence < -1) throw new IllegalArgumentException("afterSequence must be at least -1");
    if (limit < 1 || limit > 500) throw new IllegalArgumentException("limit must be 1 to 500");
    return transactions.execute(repository -> repository.history(taskId, afterSequence, limit));
  }

  public List<HumanTaskContentRevisionRecord> revisions(HumanTaskId taskId) {
    return transactions.execute(repository -> repository.revisions(taskId));
  }
}
