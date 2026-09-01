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
package com.forwardmeasure.openworkflow.definition.infrastructure.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises every egress control implemented by {@link AllowlistedHttpWorkflowResourceLoader}:
 * scheme allowlisting, exact-host allowlisting, refusal to follow redirects (the classic
 * SSRF-via-redirect bypass), the bounded response body read, and content-type parsing.
 */
class AllowlistedHttpWorkflowResourceLoaderTest {
  private HttpServer server;
  private String baseUri;
  private String host;
  private final AtomicInteger redirectTargetHits = new AtomicInteger();

  @BeforeEach
  void startRealHttpServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/resource.json", exchange -> respond(exchange, 200, "application/json", "{\"a\":1}"));
    server.createContext(
        "/redirect",
        exchange -> {
          exchange.getResponseHeaders().add("Location", baseUri + "/redirect-target");
          respond(exchange, 302, "text/plain", "moved");
        });
    server.createContext(
        "/redirect-target",
        exchange -> {
          redirectTargetHits.incrementAndGet();
          respond(exchange, 200, "text/plain", "should never be reached");
        });
    server.createContext(
        "/oversized",
        exchange -> {
          byte[] body =
              "x"
                  .repeat(ResolvedWorkflowResource.MAX_CONTENT_BYTES + 1)
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "text/plain");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/with-parameters",
        exchange -> respond(exchange, 200, "application/json; charset=utf-8", "{\"ok\":true}"));
    server.createContext(
        "/no-content-type",
        exchange -> {
          byte[] bytes = "plain body".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.createContext(
        "/malformed-content-type",
        exchange -> respond(exchange, 200, ";charset=utf-8", "irrelevant"));
    server.createContext("/not-found", exchange -> respond(exchange, 404, "text/plain", "gone"));
    server.start();
    host = "127.0.0.1";
    baseUri = "http://" + host + ":" + server.getAddress().getPort();
  }

  @AfterEach
  void stopRealHttpServer() {
    server.stop(0);
  }

  @Test
  void rejectsNonHttpSchemesEvenWhenTheHostIsAllowlisted() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of("example.com", host));

    IllegalArgumentException ftp =
        assertThrows(
            IllegalArgumentException.class,
            () -> loader.load(request("ftp://example.com/resource.json")));
    assertTrue(ftp.getMessage().contains("not allowlisted"));

    IllegalArgumentException file =
        assertThrows(
            IllegalArgumentException.class, () -> loader.load(request("file:///etc/passwd")));
    assertTrue(file.getMessage().contains("not allowlisted"));
  }

  @Test
  void rejectsAHostThatIsNotOnTheAllowlistEvenWithAValidScheme() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of("good.example.com"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> loader.load(request("https://evil.example.com/resource.json")));
    assertTrue(exception.getMessage().contains("not allowlisted"));
  }

  @Test
  void permitsAnAllowlistedHostAndReturnsTheExactBytesAndParsedMediaType() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    ResolvedWorkflowResource resource = loader.load(request(baseUri + "/resource.json"));

    assertEquals("{\"a\":1}", resource.content());
    assertEquals("application/json", resource.mediaType());
    assertEquals(URI.create(baseUri + "/resource.json"), resource.uri());
  }

  @Test
  void doesNotFollowARedirectAndNeverContactsTheRedirectTarget() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> loader.load(request(baseUri + "/redirect")));

    assertTrue(exception.getMessage().contains("302"));
    assertEquals(
        0,
        redirectTargetHits.get(),
        "the redirect target must never be fetched: HttpClient is configured Redirect.NEVER and "
            + "a 3xx status is treated as a hard failure, not auto-followed");
  }

  @Test
  void rejectsOtherNonSuccessStatusCodesTooNotJustRedirects() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> loader.load(request(baseUri + "/not-found")));
    assertTrue(exception.getMessage().contains("404"));
  }

  @Test
  void rejectsAResponseBodyLargerThanThePublicationSizeCapInsteadOfBufferingItUnbounded() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> loader.load(request(baseUri + "/oversized")));
    assertTrue(exception.getMessage().contains("publication size limit"));
  }

  @Test
  void parsesAWellFormedContentTypeHeaderAndStripsParameters() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    ResolvedWorkflowResource resource = loader.load(request(baseUri + "/with-parameters"));

    assertEquals("application/json", resource.mediaType());
  }

  @Test
  void defaultsToOctetStreamWhenNoContentTypeHeaderIsPresent() {
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    ResolvedWorkflowResource resource = loader.load(request(baseUri + "/no-content-type"));

    assertEquals("application/octet-stream", resource.mediaType());
  }

  @Test
  void rejectsAMalformedContentTypeThatHasNoTypeBeforeItsParameters() {
    // A header of ";charset=utf-8" has nothing before the first ';', so the substring-before-';'
    // parse leaves an empty media type. ResolvedWorkflowResource then refuses to construct with a
    // blank mediaType, so the loader fails closed instead of fabricating a media type.
    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> loader.load(request(baseUri + "/malformed-content-type")));
    assertNotNull(exception.getMessage());
  }

  @Test
  void nullThrowsAndAllowlistIsNormalizedToLowerCase() {
    assertThrows(NullPointerException.class, () -> new AllowlistedHttpWorkflowResourceLoader(null));

    AllowlistedHttpWorkflowResourceLoader loader =
        new AllowlistedHttpWorkflowResourceLoader(Set.of(host.toUpperCase(java.util.Locale.ROOT)));
    // the allowlist is lower-cased at construction time, and the request host is lower-cased at
    // check time, so an operator-supplied allowlist entry in any case still matches.
    ResolvedWorkflowResource resource = loader.load(request(baseUri + "/resource.json"));
    assertFalse(resource.content().isEmpty());
  }

  private WorkflowResourceRequest request(String uri) {
    return new WorkflowResourceRequest(
        URI.create(uri), "resource", null, WorkflowResourceKind.DATA_SCHEMA);
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body)
      throws java.io.IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }
}
