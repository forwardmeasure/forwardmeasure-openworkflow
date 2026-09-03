/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.domain.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskView;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEntity;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

/** MapStruct boundary from the domain-owned persistence entity to the application view model. */
@Mapper
public interface HumanTaskEntityMapper {
  HumanTaskEntityMapper INSTANCE = Mappers.getMapper(HumanTaskEntityMapper.class);

  @Mapping(target = "state", source = "entity.snapshot")
  HumanTaskView toView(HumanTaskEntity entity, @Context ObjectMapper json);

  default HumanTaskState map(JsonNode snapshot, @Context ObjectMapper json) {
    return json.convertValue(snapshot, HumanTaskState.class);
  }
}
