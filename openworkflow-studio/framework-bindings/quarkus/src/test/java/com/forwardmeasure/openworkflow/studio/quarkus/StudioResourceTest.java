package com.forwardmeasure.openworkflow.studio.quarkus;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class StudioResourceTest {
  @Test
  void servesSharedWebappAndRuntimeConfig() {
    given()
        .redirects()
        .follow(false)
        .when()
        .get("/")
        .then()
        .statusCode(303)
        .header("Location", containsString("/studio/"));
    // Exact match, with an explicit Host header standing in for the public hostname a real
    // client would send through the Gateway - containsString("/studio/") previously passed for
    // BOTH the correct "https://lux.kriyagentic.com/owf/studio/" and the actual, broken,
    // shipped-to-production value "http://lux.kriyagentic.com/studio/" (wrong scheme, missing the
    // Gateway's "/owf" prefix), so it never could have caught that regression. This is why that
    // bug reached production twice in a row - once from an absolute-path redirect misresolving
    // against this pod's own unaware/plain-HTTP view of itself, and again from a relative-path
    // redirect that RESTEasy Reactive absolutized the exact same wrong way regardless.
    given()
        .header("Host", "lux.kriyagentic.com")
        .redirects()
        .follow(false)
        .when()
        .get("/studio")
        .then()
        .statusCode(303)
        .header("Location", equalTo("https://lux.kriyagentic.com/owf/studio/"));
    // Exact match on connect-src specifically, not just "the header exists" - CSP's default
    // connect-src 'self' silently blocks the browser's own token-exchange fetch() to the OIDC
    // provider (a background fetch, unlike the login redirect itself, which is a top-level
    // navigation CSP never restricts) before the request ever leaves the browser. Confirmed the
    // hard way: login got stuck on "Failed to fetch" 100% of the time, with curl reproducing the
    // identical request successfully and Keycloak's own CORS headers checking out fine - nothing
    // server-side was wrong, the browser just never sent the request.
    given()
        .when()
        .get("/studio/")
        .then()
        .statusCode(200)
        .header(
            "Content-Security-Policy", containsString("connect-src 'self' http://localhost:8180"))
        // MUI's Emotion styling engine inserts <style> elements at runtime for the canvas view -
        // style-src 'self' alone blocks those exactly like it would a disallowed stylesheet URL.
        .header("Content-Security-Policy", containsString("style-src 'self' 'unsafe-inline'"))
        .body(containsString("OpenWorkflow Studio"));
    given()
        .when()
        .get("/studio/config.js")
        .then()
        .statusCode(200)
        .header("Cache-Control", containsString("no-store"))
        .body(containsString("apiBasePath"))
        .body(containsString("oidcUrl"))
        .body(containsString("oidcRealm"))
        .body(containsString("oidcClientId"));
    given().when().get("/q/health/ready").then().statusCode(200);
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
