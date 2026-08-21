/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.micronaut;

import io.micronaut.runtime.Micronaut;

public final class KafkaEngineMicronautApplication {
  private KafkaEngineMicronautApplication() {}

  public static void main(String[] arguments) {
    Micronaut.run(KafkaEngineMicronautApplication.class, arguments);
  }
}
