/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.execution.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;

@SpringBootApplication(scanBasePackages = "com.forwardmeasure.openworkflow")
@EntityScan(
    basePackages = {
      "com.forwardmeasure.jpa.identity.entity",
      "com.forwardmeasure.openworkflow.execution.management.persistence"
    })
public class ExecutionManagementSpringApplication {
  public static void main(String[] arguments) {
    SpringApplication.run(ExecutionManagementSpringApplication.class, arguments);
  }
}
