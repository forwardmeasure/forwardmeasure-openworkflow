/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.execution.micronaut;

import io.micronaut.runtime.Micronaut;

public final class ExecutionManagementMicronautApplication {
  private ExecutionManagementMicronautApplication() {}

  public static void main(String[] arguments) {
    Micronaut.run(ExecutionManagementMicronautApplication.class, arguments);
  }
}
