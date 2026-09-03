/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.util.concurrent.CompletionStage;

/** Port used by workflow engines to submit idempotent Human Task requests. */
public interface HumanTaskRequestPort {
  CompletionStage<HumanTaskAcceptance> request(HumanTaskRequest request);
}
