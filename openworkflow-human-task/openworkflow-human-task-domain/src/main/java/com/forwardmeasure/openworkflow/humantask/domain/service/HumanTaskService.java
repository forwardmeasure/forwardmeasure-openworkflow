/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.domain.service;

import com.forwardmeasure.jpa.core.service.AbstractBaseService;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEntity;

/** Standard CRUD service contract for the Human Task persistence entity. */
public interface HumanTaskService extends AbstractBaseService<HumanTaskEntity, Long> {}
