package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

public final class OksStores {
  public static final String EXECUTION_STORE_PREFIX = "oks-execution";
  public static final String EXECUTIONS = EXECUTION_STORE_PREFIX + "-state";
  public static final String EXECUTION_METADATA = EXECUTION_STORE_PREFIX + "-metadata";
  public static final String EXECUTION_COMMAND_RECEIPTS =
      EXECUTION_STORE_PREFIX + "-command-receipts";
  public static final String EXECUTION_COMMAND_OUTCOMES =
      EXECUTION_STORE_PREFIX + "-command-outcomes";
  public static final String DEFINITIONS = "oks-definitions";
  public static final String DEFINITION_ADMISSIONS = "oks-definition-admissions";
  public static final String DEFINITION_COMMANDS = "oks-definition-commands";
  public static final String EVENT_SUBSCRIPTIONS = "oks-event-subscriptions";
  public static final String INBOUND_EVENTS = "oks-inbound-events";
  public static final String SCHEDULE_EVENT_STATES = "oks-schedule-event-states";
  public static final String SCHEDULE_EVENT_RECEIPTS = "oks-schedule-event-receipts";
  public static final String TIMERS = "oks-timers";
  public static final String HISTORY = "oks-history-query";
  public static final String EFFECTS = "oks-effects-query";
  public static final String DEFINITION_HISTORY = "oks-definition-history-query";

  private OksStores() {}
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
