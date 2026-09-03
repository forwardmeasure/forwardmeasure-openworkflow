/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

/** The requested Human Task is absent from the active tenant or is not visible. */
public final class HumanTaskNotFoundException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public HumanTaskNotFoundException(String message) {
    super(message);
  }
}
