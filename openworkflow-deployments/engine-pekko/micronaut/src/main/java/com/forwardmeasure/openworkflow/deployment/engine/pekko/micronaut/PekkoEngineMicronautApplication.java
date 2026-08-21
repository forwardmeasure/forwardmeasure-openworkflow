/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.micronaut;

import io.micronaut.runtime.Micronaut;

public final class PekkoEngineMicronautApplication {
  private PekkoEngineMicronautApplication() {}

  public static void main(String[] arguments) {
    Micronaut.run(PekkoEngineMicronautApplication.class, arguments);
  }
}
