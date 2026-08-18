package com.forwardmeasure.openworkflow.studio.spring;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Controller
public final class StudioController {
  private final JsonMapper objectMapper;
  private final String apiBasePath;
  private final String environmentName;

  StudioController(
      JsonMapper objectMapper,
      @Value("${openworkflow.studio.api-base-path:/api}") String apiBasePath,
      @Value("${openworkflow.studio.environment-name:Local}") String environmentName) {
    this.objectMapper = objectMapper;
    this.apiBasePath = apiBasePath;
    this.environmentName = environmentName;
  }

  @GetMapping("/")
  String root() {
    return "redirect:/studio/";
  }

  @GetMapping("/studio/")
  String studio() {
    return "forward:/studio/index.html";
  }

  @GetMapping(value = "/studio/config.js", produces = "application/javascript")
  @ResponseBody
  ResponseEntity<String> config() throws JacksonException {
    var json =
        objectMapper.writeValueAsString(
            Map.of("apiBasePath", apiBasePath, "environmentName", environmentName));
    return ResponseEntity.ok()
        .contentType(MediaType.parseMediaType("application/javascript"))
        .cacheControl(CacheControl.noStore())
        .body("window.__OPENWORKFLOW_STUDIO_CONFIG__ = " + json + ";\n");
  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
