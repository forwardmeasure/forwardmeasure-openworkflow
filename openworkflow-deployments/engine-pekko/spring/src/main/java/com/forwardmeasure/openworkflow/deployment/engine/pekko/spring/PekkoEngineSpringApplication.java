/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PekkoEngineSpringApplication {
  public static void main(String[] arguments) {
    SpringApplication.run(PekkoEngineSpringApplication.class, arguments);
  }
}
