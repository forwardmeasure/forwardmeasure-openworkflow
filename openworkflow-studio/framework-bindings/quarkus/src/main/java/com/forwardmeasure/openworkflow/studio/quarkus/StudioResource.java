package com.forwardmeasure.openworkflow.studio.quarkus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Path("/")
public final class StudioResource {
  @Context private HttpHeaders headers;

  @Inject ObjectMapper objectMapper;

  @ConfigProperty(name = "openworkflow.studio.environment-name", defaultValue = "Local")
  String environmentName;

  @ConfigProperty(name = "openworkflow.studio.oidc-url", defaultValue = "http://localhost:8180")
  String oidcUrl;

  @ConfigProperty(name = "openworkflow.studio.oidc-realm", defaultValue = "forwardmeasure")
  String oidcRealm;

  @ConfigProperty(
      name = "openworkflow.studio.oidc-client-id",
      defaultValue = "forwardmeasure-public")
  String oidcClientId;

  @GET
  public Response root() {
    return Response.seeOther(URI.create("/studio/")).build();
  }

  /**
   * Vert.x's classpath-backed StaticHandler never resolves the bare "studio" segment as a directory
   * (confirmed by inspecting its bytecode: sendDirectory()'s FileProps.isDirectory() check doesn't
   * fire for a JAR/classpath resource root), so a request for exactly "/studio" - with no trailing
   * slash, the natural way to type or paste the public URL - falls through to a plain 404 instead
   * of redirecting the way a real filesystem-backed static server would.
   *
   * <p>Builds a fully absolute https URL by hand from the request's own Host header, rather than
   * handing JAX-RS a relative reference and trusting it to leave it alone: confirmed live (curl -D-
   * against the public URL, twice) that both an absolute "/studio/" AND a relative "studio/" come
   * out of {@code Response.seeOther(URI)} identically wrong - "http://lux.kriyagentic.com/studio/"
   * - RESTEasy Reactive resolves whatever URI it's given against its own request-base-URI
   * understanding before writing the Location header, and that base URI has no idea this pod sits
   * behind a Gateway that terminates TLS and strips a "/owf" prefix before forwarding. A relative
   * Java URI object does not survive as a relative Location header - it gets absolutized
   * server-side regardless, using the wrong scheme and missing the external prefix. Host is read
   * the same way PlatformTenantContextResource (a sibling app) reads it for the identical reason -
   * the Gateway forwards it unchanged, confirmed by that resource's own passing tenant-resolution
   * tests.
   */
  @GET
  @Path("studio")
  public Response studio() {
    String host = headers.getHeaderString(HttpHeaders.HOST);
    return Response.seeOther(URI.create("https://" + host + "/owf/studio/")).build();
  }

  /**
   * apiBasePath is computed here, not read from a static config property: the frontend's generated
   * API clients build request URLs by plain string concatenation ({@code basePath + endpointPath},
   * see api.ts), handed straight to {@code fetch()} - and StudioApiProxyResource (this same
   * package) only receives requests the Gateway actually forwards, which - same as this class's own
   * studio()/root() redirects - only ever arrive prefixed "/owf/studio", never bare "/api". A
   * static "/api" value here would send every API call to a path with no backing Gateway route at
   * all, exactly the class of bug studio() was fixed for above; building the same kind of
   * fully-qualified absolute URL from the request's own Host header removes the guesswork entirely
   * instead of relying on the browser resolving a relative basePath correctly, which nothing here
   * currently proves either way.
   */
  @GET
  @Path("studio/config.js")
  @Produces("application/javascript")
  public Response config() throws JsonProcessingException {
    String host = headers.getHeaderString(HttpHeaders.HOST);
    String apiBasePath = "https://" + host + "/owf/studio/api";
    var json =
        objectMapper.writeValueAsString(
            Map.of(
                "apiBasePath", apiBasePath,
                "environmentName", environmentName,
                "oidcUrl", oidcUrl,
                "oidcRealm", oidcRealm,
                "oidcClientId", oidcClientId));
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
