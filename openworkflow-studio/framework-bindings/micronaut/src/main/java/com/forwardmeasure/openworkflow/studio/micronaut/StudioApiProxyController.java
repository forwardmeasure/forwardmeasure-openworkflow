package com.forwardmeasure.openworkflow.studio.micronaut;

import com.forwardmeasure.openworkflow.studio.StudioApiProxy;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Patch;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Put;
import java.nio.charset.StandardCharsets;

@Controller
final class StudioApiProxyController {
  private final StudioApiProxy proxy;

  StudioApiProxyController(
      @Value("${openworkflow.studio.api-upstream:http://127.0.0.1:8081}") String upstream) {
    this.proxy = new StudioApiProxy(upstream);
  }

  @Get("/api/{+path}")
  HttpResponse<byte[]> get(HttpRequest<?> request) throws Exception {
    return forward(request, null);
  }

  @Post("/api/{+path}")
  HttpResponse<byte[]> post(HttpRequest<?> request, @Nullable @Body String body) throws Exception {
    return forward(request, body);
  }

  @Put("/api/{+path}")
  HttpResponse<byte[]> put(HttpRequest<?> request, @Nullable @Body String body) throws Exception {
    return forward(request, body);
  }

  @Patch("/api/{+path}")
  HttpResponse<byte[]> patch(HttpRequest<?> request, @Nullable @Body String body) throws Exception {
    return forward(request, body);
  }

  @Delete("/api/{+path}")
  HttpResponse<byte[]> delete(HttpRequest<?> request, @Nullable @Body String body)
      throws Exception {
    return forward(request, body);
  }

  private HttpResponse<byte[]> forward(HttpRequest<?> request, String body) throws Exception {
    String path = request.getUri().getRawPath().substring("/api/".length());
    if (request.getUri().getRawQuery() != null) path += "?" + request.getUri().getRawQuery();
    var response =
        proxy.forward(
            request.getMethodName(),
            path,
            request.getHeaders().asMap(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    var builder = HttpResponse.<byte[]>status(response.status(), null).body(response.body());
    response
        .headers()
        .forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
    return builder;
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
