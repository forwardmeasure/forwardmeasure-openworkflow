/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.domain.service.impl;

import com.forwardmeasure.jpa.core.service.impl.AbstractBaseServiceImpl;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEntity;
import com.forwardmeasure.openworkflow.humantask.domain.repository.JpaHumanTaskRepository;
import com.forwardmeasure.openworkflow.humantask.domain.service.HumanTaskService;

/** Default ForwardMeasure base service for Human Task entities. */
public class HumanTaskServiceImpl
    extends AbstractBaseServiceImpl<HumanTaskEntity, Long, JpaHumanTaskRepository>
    implements HumanTaskService {

  public HumanTaskServiceImpl(JpaHumanTaskRepository repository) {
    super(repository);
  }
}
