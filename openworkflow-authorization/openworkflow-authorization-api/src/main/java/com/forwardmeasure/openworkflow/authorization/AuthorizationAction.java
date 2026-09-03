/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.authorization;

public enum AuthorizationAction {
  DEFINITION_CREATE("definition:create"),
  DEFINITION_READ("definition:read"),
  DEFINITION_LIST("definition:list"),
  DEFINITION_UPDATE("definition:update"),
  DEFINITION_DELETE("definition:delete"),
  DEFINITION_VALIDATE("definition:validate"),
  DEFINITION_SUBMIT("definition:submit"),
  DEFINITION_WITHDRAW("definition:withdraw"),
  DEFINITION_APPROVE("definition:approve"),
  DEFINITION_REJECT("definition:reject"),
  DEFINITION_PUBLISH("definition:publish"),
  DEFINITION_DEPRECATE("definition:deprecate"),
  EXECUTION_START("execution:start"),
  EXECUTION_READ("execution:read"),
  EXECUTION_LIST("execution:list"),
  EXECUTION_PAUSE("execution:pause"),
  EXECUTION_RESUME("execution:resume"),
  EXECUTION_CANCEL("execution:cancel"),
  OPERATION_EXECUTE("operation:execute"),
  EVENT_ROUTE("event:route"),
  EVENT_DELIVER("event:deliver"),
  AUDIT_READ("audit:read"),
  AUDIT_QUERY("audit:query"),
  HUMAN_TASK_LIST("human-task:list"),
  HUMAN_TASK_READ("human-task:read"),
  HUMAN_TASK_READ_HISTORY("human-task:read-history"),
  HUMAN_TASK_BEGIN_REVIEW("human-task:begin-review"),
  HUMAN_TASK_RENEW_REVIEW("human-task:renew-review"),
  HUMAN_TASK_EDIT("human-task:edit"),
  HUMAN_TASK_COMMENT("human-task:comment"),
  HUMAN_TASK_RELEASE_REVIEW("human-task:release-review"),
  HUMAN_TASK_DECIDE("human-task:decide"),
  HUMAN_TASK_ASSIGN("human-task:assign"),
  HUMAN_TASK_REASSIGN("human-task:reassign"),
  HUMAN_TASK_CANCEL("human-task:cancel"),
  HUMAN_TASK_EXPIRE("human-task:expire");

  private final String scope;

  AuthorizationAction(String scope) {
    this.scope = scope;
  }

  public String scope() {
    return scope;
  }
}
