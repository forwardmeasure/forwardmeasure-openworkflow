package com.forwardmeasure.openworkflow.studio.micronaut;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.json.JsonMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

@Controller
public final class StudioController {
  private final JsonMapper jsonMapper;
  private final String apiBasePath;
  private final String environmentName;

  StudioController(
      JsonMapper jsonMapper,
      @Value("${openworkflow.studio.api-base-path:/api}") String apiBasePath,
      @Value("${openworkflow.studio.environment-name:Local}") String environmentName) {
    this.jsonMapper = jsonMapper;
    this.apiBasePath = apiBasePath;
    this.environmentName = environmentName;
  }

  @Get("/")
  HttpResponse<?> root() {
    return HttpResponse.redirect(URI.create("/studio/"));
  }

  @Get(value = "/studio/config.js", produces = "application/javascript")
  HttpResponse<String> config() throws IOException {
    var json =
        jsonMapper.writeValueAsString(
            Map.of("apiBasePath", apiBasePath, "environmentName", environmentName));
    return HttpResponse.ok("window.__OPENWORKFLOW_STUDIO_CONFIG__ = " + json + ";\n")
        .contentType(MediaType.of("application/javascript"))
        .header("Cache-Control", "no-store");
  }

  @Get(value = "/studio/", produces = MediaType.TEXT_HTML)
  byte[] index() throws IOException {
    return resource("META-INF/resources/studio/index.html");
  }

  @Get(value = "/studio/index.html", produces = MediaType.TEXT_HTML)
  byte[] indexDocument() throws IOException {
    return index();
  }

  @Get("/studio/assets/{file}")
  HttpResponse<byte[]> asset(String file) throws IOException {
    var type = file.endsWith(".css") ? MediaType.TEXT_CSS_TYPE : MediaType.of("text/javascript");
    return HttpResponse.ok(resource("META-INF/resources/studio/assets/" + file))
        .contentType(type)
        .header("Cache-Control", "public, max-age=31536000, immutable");
  }

  private byte[] resource(String name) throws IOException {
    try (InputStream input = getClass().getClassLoader().getResourceAsStream(name)) {
      if (input == null) throw new IOException("Missing Studio resource: " + name);
      return input.readAllBytes();
    }
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
