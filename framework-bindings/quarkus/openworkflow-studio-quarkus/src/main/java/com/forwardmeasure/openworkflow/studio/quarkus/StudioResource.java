package com.forwardmeasure.openworkflow.studio.quarkus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Path("/")
public final class StudioResource {
  @Inject ObjectMapper objectMapper;

  @ConfigProperty(name = "openworkflow.studio.api-base-path", defaultValue = "/api")
  String apiBasePath;

  @ConfigProperty(name = "openworkflow.studio.environment-name", defaultValue = "Local")
  String environmentName;

  @GET
  public Response root() {
    return Response.seeOther(URI.create("/studio/")).build();
  }

  @GET
  @Path("studio/config.js")
  @Produces("application/javascript")
  public Response config() throws JsonProcessingException {
    var json =
        objectMapper.writeValueAsString(
            Map.of("apiBasePath", apiBasePath, "environmentName", environmentName));
    var cache = new CacheControl();
    cache.setNoStore(true);
    return Response.ok("window.__OPENWORKFLOW_STUDIO_CONFIG__ = " + json + ";\n")
        .cacheControl(cache)
        .build();
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
