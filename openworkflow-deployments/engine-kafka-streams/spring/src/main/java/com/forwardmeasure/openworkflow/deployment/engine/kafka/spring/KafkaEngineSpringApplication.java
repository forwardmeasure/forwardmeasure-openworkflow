/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * {@code scanBasePackages} is required (not the default single-package scan) so Spring also picks
 * up {@code openworkflow-spring-binding}'s {@code @Component}/{@code @Configuration} classes
 * ({@code SpringActiveOrganizationProvider}, {@code OpenWorkflowSpringBinding}'s security filter
 * chain) - same convention already used correctly by {@code
 * openworkflow-definition-management-spring} and {@code openworkflow-execution-management-spring}
 * for the identical need.
 */
@SpringBootApplication(scanBasePackages = "com.forwardmeasure.openworkflow")
public class KafkaEngineSpringApplication {
  public static void main(String[] arguments) {
    SpringApplication.run(KafkaEngineSpringApplication.class, arguments);
  }
}
