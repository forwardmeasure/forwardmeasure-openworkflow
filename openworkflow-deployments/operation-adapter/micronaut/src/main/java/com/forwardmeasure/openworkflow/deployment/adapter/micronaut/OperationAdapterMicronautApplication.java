/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.adapter.micronaut;

import io.micronaut.runtime.Micronaut;

public final class OperationAdapterMicronautApplication {
  private OperationAdapterMicronautApplication() {}

  public static void main(String[] arguments) {
    Micronaut.run(OperationAdapterMicronautApplication.class, arguments);
  }
}
