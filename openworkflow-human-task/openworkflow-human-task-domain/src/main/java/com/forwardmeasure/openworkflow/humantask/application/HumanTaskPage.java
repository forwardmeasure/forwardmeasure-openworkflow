/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.util.List;

/** One bounded Human Task page and an opaque continuation cursor. */
public record HumanTaskPage(List<HumanTaskView> items, String nextCursor) {
  public HumanTaskPage {
    items = List.copyOf(items);
  }
}
