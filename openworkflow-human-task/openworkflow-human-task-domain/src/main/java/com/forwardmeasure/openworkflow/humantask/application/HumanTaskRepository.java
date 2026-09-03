/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.util.List;
import java.util.Optional;

/**
 * Tenant-bound persistence port; commit is atomic with event, receipt, revision, and outbox rows.
 */
public interface HumanTaskRepository {
  Optional<HumanTaskState> find(HumanTaskId taskId);

  Optional<HumanTaskView> findView(HumanTaskId taskId);

  HumanTaskPage list(HumanTaskListQuery query);

  HumanTaskHistoryPage history(HumanTaskId taskId, long afterSequence, int limit);

  List<HumanTaskContentRevisionRecord> revisions(HumanTaskId taskId);

  Optional<HumanTaskCommandReceipt> findReceipt(String commandId);

  void commit(
      HumanTaskState priorState,
      HumanTaskState resultingState,
      List<HumanTaskEvent> events,
      HumanTaskCommandReceipt receipt,
      List<HumanTaskOutboxMessage> outboxMessages);
}
