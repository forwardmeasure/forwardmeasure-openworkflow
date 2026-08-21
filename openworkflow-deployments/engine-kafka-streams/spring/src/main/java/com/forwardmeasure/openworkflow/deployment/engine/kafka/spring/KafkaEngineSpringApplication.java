/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaEngineSpringApplication {
  public static void main(String[] arguments) {
    SpringApplication.run(KafkaEngineSpringApplication.class, arguments);
  }
}
