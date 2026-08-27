package com.forwardmeasure.openworkflow.studio.quarkus;

import com.forwardmeasure.openworkflow.studio.StudioApiProxy;
import jakarta.annotation.PostConstruct;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.io.IOException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Mapped under "studio/api", not bare "/api" - the Gateway strips a "/owf" prefix before forwarding
 * but keeps "studio", so a public request to "/owf/studio/api/xyz" (what StudioResource's config()
 * now advertises as apiBasePath) arrives here as "/studio/api/xyz". A bare "/api/{path}" mapping
 * would only ever be reached by a request this Gateway route can never actually produce.
 */
@Path("studio/api/{path: .+}")
public final class StudioApiProxyResource {
  @ConfigProperty(name = "openworkflow.studio.api-upstream", defaultValue = "http://127.0.0.1:8081")
  String upstream;

  @ConfigProperty(
      name = "openworkflow.studio.execution-api-upstream",
      defaultValue = "http://127.0.0.1:8081")
  String executionUpstream;

  private StudioApiProxy proxy;

  @PostConstruct
  void initialize() {
    proxy = new StudioApiProxy(upstream, executionUpstream);
  }

  @GET
  public Response get(@Context UriInfo uri, @Context HttpHeaders headers) throws Exception {
    return forward(HttpMethod.GET, uri, headers, new byte[0]);
  }

  @POST
  public Response post(@Context UriInfo uri, @Context HttpHeaders headers, byte[] body)
      throws Exception {
    return forward(HttpMethod.POST, uri, headers, body);
  }

  @PUT
  public Response put(@Context UriInfo uri, @Context HttpHeaders headers, byte[] body)
      throws Exception {
    return forward(HttpMethod.PUT, uri, headers, body);
  }

  @PATCH
  public Response patch(@Context UriInfo uri, @Context HttpHeaders headers, byte[] body)
      throws Exception {
    return forward(HttpMethod.PATCH, uri, headers, body);
  }

  @DELETE
  public Response delete(@Context UriInfo uri, @Context HttpHeaders headers, byte[] body)
      throws Exception {
    return forward(HttpMethod.DELETE, uri, headers, body);
  }

  private Response forward(String method, UriInfo uri, HttpHeaders headers, byte[] body)
      throws IOException, InterruptedException {
    var raw = uri.getRequestUri().getRawPath().substring("/studio/api/".length());
    if (uri.getRequestUri().getRawQuery() != null) raw += "?" + uri.getRequestUri().getRawQuery();
    var proxied =
        proxy.forward(method, raw, headers.getRequestHeaders(), body == null ? new byte[0] : body);
    var response = Response.status(proxied.status()).entity(proxied.body());
    proxied
        .headers()
        .forEach((name, values) -> values.forEach(value -> response.header(name, value)));
    return response.build();
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
