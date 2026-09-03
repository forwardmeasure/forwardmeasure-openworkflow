/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.util.concurrent.CompletionStage;

/** Port used to publish an accepted terminal Human Task outcome. */
public interface HumanTaskOutcomePort {
  CompletionStage<Void> publish(HumanTaskOutcome outcome);
}
