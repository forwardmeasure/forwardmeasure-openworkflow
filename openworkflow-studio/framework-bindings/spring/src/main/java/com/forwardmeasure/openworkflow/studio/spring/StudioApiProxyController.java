package com.forwardmeasure.openworkflow.studio.spring;

import com.forwardmeasure.openworkflow.studio.StudioApiProxy;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
final class StudioApiProxyController {
  private final StudioApiProxy proxy;

  StudioApiProxyController(
      @Value("${openworkflow.studio.api-upstream:http://127.0.0.1:8081}") String upstream) {
    this.proxy = new StudioApiProxy(upstream);
  }

  @RequestMapping("/api/**")
  ResponseEntity<byte[]> forward(
      HttpServletRequest request, @RequestBody(required = false) byte[] body) throws Exception {
    String path = request.getRequestURI().substring((request.getContextPath() + "/api/").length());
    if (request.getQueryString() != null) path += "?" + request.getQueryString();
    Map<String, List<String>> headers =
        Collections.list(request.getHeaderNames()).stream()
            .collect(
                Collectors.toMap(name -> name, name -> Collections.list(request.getHeaders(name))));
    var response =
        proxy.forward(request.getMethod(), path, headers, body == null ? new byte[0] : body);
    var builder = ResponseEntity.status(response.status());
    response
        .headers()
        .forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
    return builder.body(response.body());
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
