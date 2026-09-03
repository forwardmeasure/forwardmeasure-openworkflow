# Plan: real, live Keycloak-JWT round-trip tests for `ActiveOrganizationProvider`

## Status of this document

Written from a sibling repo (`forwardmeasure-entity-intelligence`, "fei") after building and
compiling (not yet test-executed) the identical capability there from scratch, adapting fowf's own
`ActiveOrganizationProvider` pattern. Every fact below — module layout, exact property keys, exact
Admin REST sequence, exact framework config idioms — was confirmed by reading real fowf/
forwardmeasure-platform source directly during that work, not assumed. Where something is still
open (mainly: whether the pinned Keycloak test image version actually works), it's flagged
explicitly rather than presented as settled. This document is self-contained: a fresh session
picking it up needs no other context from the fei work that produced it.

## The gap

fowf has three real, production `ActiveOrganizationProvider` implementations
(`QuarkusActiveOrganizationProvider`, `SpringActiveOrganizationProvider`,
`MicronautActiveOrganizationProvider`) plus the shared claim-parsing helper
`KeycloakOrganizationClaims.extract()`. All four are compile-verified and have exactly one test
between them: `KeycloakOrganizationClaimsTest`, which unit-tests `extract()` against a hand-built
Java `Map` of claims. **No test anywhere in fowf sends a real HTTP request carrying a real,
Keycloak-signed JWT through any of the three framework bindings.** The entire chain from "a browser
presents a Bearer token" to "a REST resource sees the right tenant/actor/roles" has never been
exercised end-to-end for real, on any framework. This plan closes that gap using a genuine
testcontainers-backed Keycloak instance with the Organizations feature actually enabled — no mocks,
no hand-built claims maps for the round-trip test.

## What already exists to build on

- `openworkflow-authorization/openworkflow-authorization-testkit/` **already exists as a module**
  but currently holds only `StubAuthorizationService` (a fake, non-Keycloak test double). It
  depends on `openworkflow-authorization-api` and nothing else. This is the natural home for the
  new fixture — extend this module in place rather than creating a new sibling module.
- `forwardmeasure-testcontainers-keycloak` (`com.forwardmeasure.testcontainers:forwardmeasure-testcontainers-keycloak`)
  already exists and is real, tested infrastructure: `KeycloakTestContainer(String realm, String
  realmJson)` imports a realm via `--import-realm` at container startup;
  `.start()`; `.issuer()` → the realm's issuer `URI`; `.passwordToken(clientId, username,
  password)` (ROPC grant); `.clientCredentialsToken(clientId, clientSecret)`. Bootstrap admin
  credentials are `admin` / `admin-integration-only` (test-only, container-scoped, set by the
  container itself). Image version is pinned via the `keycloak.version` property in the **central**
  `forwardmeasure-platform/pom.xml` (currently `26.7.1`) — wide blast radius, not scoped to this
  testkit; see the open item below before touching it.
- The real Keycloak Organizations claim shape `KeycloakOrganizationClaims.extract()` expects is
  already correctly implemented and already has production infrastructure standing behind it:
  realm-level `organizationsEnabled: true`, a client scope named `"organization"` carrying two
  protocol mappers (`oidc-organization-membership-mapper` with `addOrganizationAttributes=true,
  addOrganizationId=true`; `oidc-organization-group-membership-mapper` with
  `addGroupRoleMappings=true`), and the `forwardmeasure.tenant-id` Organization attribute — all
  provisioned for real in `forwardmeasure-platform`'s `bootstrap-admin.sh`
  (`configure_organization_claim()`, `configure_tenant_organization_membership()`). The fixture
  below reproduces this exact shape in a self-contained realm-export JSON, so it needs no live
  platform dependency.
- **Confirmed live on Keycloak 26.7.2** (not 26.7.1 - see open item): role-mapping and membership
  for an Organization must go through the *dedicated* `organizations/{orgId}/groups/...` Admin REST
  API, not the standard `/groups/{id}/role-mappings/...` or `/users/{id}/groups/{id}` endpoints —
  those reject Organization Groups outright (HTTP 400, "Cannot manage/access organization related
  group via non Organization API"). `HttpKeycloakOrganizationAdmin.java`'s own comment claiming
  `organizations/{id}/groups` 404s is stale (true on an older, unnamed build) — confirmed false on
  26.7.2. Member-add to an Organization wants the user ID as a bare JSON string body
  (`Content-Type: application/json`, body literally `"<user-id>"`) — `text/plain` gets HTTP 415.
- **The exact, correct config property name** — easy to get wrong, and a real, since-fixed mistake
  is already documented in the source of all three providers: it is
  `openworkflow.authorization.organization-client-id`, **never**
  `openworkflow.authorization.client-id` (that property is this service's own outbound-AuthZEN
  client-credentials identity, set to `"openworkflow"` — a completely different thing). All three
  providers' constructors carry an explicit comment warning about this exact conflation because an
  earlier pass at the fix got it wrong before shipping. In real deployment config
  (`openworkflow-deployments/*/{quarkus,spring,micronaut}/src/main/resources/application.yaml`,
  all 12 leaves) this resolves via `${OPENWORKFLOW_ORGANIZATION_CLIENT_ID:forwardmeasure-public}` —
  the test fixture below overrides it per-test to point at its own disposable test client instead.

## What to build

### 1. Extend `openworkflow-authorization-testkit`

Add to its `pom.xml`:

```xml
<dependency>
  <groupId>com.forwardmeasure.testcontainers</groupId>
  <artifactId>forwardmeasure-testcontainers-keycloak</artifactId>
</dependency>
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
</dependency>
<dependency>
  <groupId>org.junit.jupiter</groupId>
  <artifactId>junit-jupiter</artifactId>
  <scope>test</scope>
</dependency>
```

(Module stays main-scope, not test-scope, matching its existing `StubAuthorizationService` shape —
it's a reusable test-support library other modules depend on at `<scope>test</scope>`, not test
code of its own.)

Add a realm-export fixture resource, e.g.
`src/main/resources/openworkflow-authz-test-realm.json`:

```json
{
  "realm": "openworkflow-authz-test",
  "enabled": true,
  "sslRequired": "none",
  "organizationsEnabled": true,
  "clients": [
    {
      "clientId": "openworkflow-test-client",
      "enabled": true,
      "publicClient": true,
      "directAccessGrantsEnabled": true,
      "standardFlowEnabled": false,
      "implicitFlowEnabled": false,
      "serviceAccountsEnabled": false,
      "defaultClientScopes": ["organization"]
    }
  ],
  "clientScopes": [
    {
      "name": "organization",
      "protocol": "openid-connect",
      "attributes": {
        "include.in.token.scope": "true",
        "display.on.consent.screen": "false"
      },
      "protocolMappers": [
        {
          "name": "organization",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-organization-membership-mapper",
          "consentRequired": false,
          "config": {
            "id.token.claim": "true",
            "introspection.token.claim": "true",
            "access.token.claim": "true",
            "claim.name": "organization",
            "jsonType.label": "JSON",
            "multivalued": "true",
            "addOrganizationAttributes": "true",
            "addOrganizationId": "true"
          }
        },
        {
          "name": "organization-group-membership",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-organization-group-membership-mapper",
          "consentRequired": false,
          "config": {
            "id.token.claim": "true",
            "introspection.token.claim": "true",
            "access.token.claim": "true",
            "claim.name": "organization",
            "addGroupRoleMappings": "true"
          }
        }
      ]
    }
  ],
  "users": [
    {
      "username": "openworkflow-test-user",
      "enabled": true,
      "emailVerified": true,
      "credentials": [
        { "type": "password", "value": "openworkflow-test-password", "temporary": false }
      ]
    }
  ]
}
```

Add `src/main/java/com/forwardmeasure/openworkflow/authorization/testkit/KeycloakOrganizationFixture.java`.
This can be ported near-verbatim from fei's own new implementation (same package root, different
leaf), which itself only reproduces fowf's own real provisioning sequence — nothing about the REST
call sequence below is fei-specific:

```java
package com.forwardmeasure.openworkflow.authorization.testkit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.testcontainers.keycloak.KeycloakTestContainer;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A real, running Keycloak fixture with the Organizations feature genuinely wired for the
 * {@code organization} claim shape {@code KeycloakOrganizationClaims.extract()} expects - not a
 * stub, not a hand-built claims map. Provisions one Organization per call via the same Admin REST
 * sequence confirmed live against Keycloak 26.7.2 in forwardmeasure-platform's own
 * bootstrap-admin.sh, then mints a real signed JWT for a real user who is really a member of it.
 */
public final class KeycloakOrganizationFixture implements AutoCloseable {

  public static final String REALM = "openworkflow-authz-test";
  public static final String CLIENT_ID = "openworkflow-test-client";
  public static final String USERNAME = "openworkflow-test-user";
  public static final String PASSWORD = "openworkflow-test-password";

  private static final String REALM_RESOURCE = "/openworkflow-authz-test-realm.json";
  private static final Pattern ACCESS_TOKEN =
      Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

  private final KeycloakTestContainer container;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  private final ObjectMapper mapper = new ObjectMapper();
  private volatile String adminToken;
  private volatile String clientUuid;

  private KeycloakOrganizationFixture(KeycloakTestContainer container) {
    this.container = container;
  }

  public static KeycloakOrganizationFixture start() {
    KeycloakTestContainer container = new KeycloakTestContainer(REALM, realmJson()).start();
    return new KeycloakOrganizationFixture(container);
  }

  public URI issuer() {
    return container.issuer();
  }

  /**
   * Provisions one Organization (with the {@code forwardmeasure.tenant-id} attribute
   * {@code KeycloakOrganizationClaims} reads), one client role on {@value #CLIENT_ID}, one
   * Organization Group mapping that role, and adds {@value #USERNAME} as a member of both the
   * Organization and the group. Uses the real, dedicated Organization Groups API
   * ({@code organizations/{orgId}/groups/...}), not a plain realm Group.
   */
  public String provisionTenant(String organizationAlias, UUID tenantId, String roleName) {
    ensureClientRole(roleName);
    String organizationId = createOrganization(organizationAlias, tenantId);
    String groupId = createOrganizationGroup(organizationId, roleName);
    mapRoleOntoOrganizationGroup(organizationId, groupId, roleName);
    String userId = requireUserId();
    addOrganizationMember(organizationId, userId);
    addOrganizationGroupMember(organizationId, groupId, userId);
    return organizationId;
  }

  public String mintUserToken() {
    return container.passwordToken(CLIENT_ID, USERNAME, PASSWORD);
  }

  @Override
  public void close() {
    container.close();
  }

  private void ensureClientRole(String roleName) {
    Response existing =
        send("GET", adminBase().resolve("clients/" + clientUuid() + "/roles/" + roleName),
            null, 200, 404);
    if (existing.status() == 404) {
      send("POST", adminBase().resolve("clients/" + clientUuid() + "/roles"),
          Map.of("name", roleName), 201, 204, 409);
    }
  }

  private String createOrganization(String alias, UUID tenantId) {
    Response created =
        send("POST", adminBase().resolve("organizations"),
            Map.of("name", alias, "alias", alias, "enabled", true,
                "attributes", Map.of("forwardmeasure.tenant-id", List.of(tenantId.toString()))),
            201);
    return created.location().map(KeycloakOrganizationFixture::lastPathSegment)
        .orElseThrow(() -> new IllegalStateException(
            "Keycloak did not return the created Organization location"));
  }

  private String createOrganizationGroup(String organizationId, String groupName) {
    send("POST", adminBase().resolve("organizations/" + organizationId + "/groups"),
        Map.of("name", groupName), 201, 204, 409);
    JsonNode groups =
        send("GET", adminBase().resolve("organizations/" + organizationId + "/groups"), null, 200)
            .body();
    for (JsonNode group : groups) {
      if (groupName.equals(group.path("name").asText())) {
        return requiredText(group, "id");
      }
    }
    throw new IllegalStateException(
        "Keycloak did not return the created Organization Group " + groupName);
  }

  private void mapRoleOntoOrganizationGroup(String organizationId, String groupId, String roleName) {
    JsonNode role =
        send("GET", adminBase().resolve("clients/" + clientUuid() + "/roles/" + roleName), null, 200)
            .body();
    send("POST",
        adminBase().resolve("organizations/" + organizationId + "/groups/" + groupId
            + "/role-mappings/clients/" + clientUuid()),
        List.of(role), 201, 204, 409);
  }

  private String requireUserId() {
    String query = URLEncoder.encode(USERNAME, StandardCharsets.UTF_8);
    JsonNode users =
        send("GET",
            URI.create(adminBase().resolve("users").toString() + "?username=" + query + "&exact=true"),
            null, 200)
            .body();
    for (JsonNode user : users) {
      if (USERNAME.equals(user.path("username").asText())) {
        return requiredText(user, "id");
      }
    }
    throw new IllegalStateException(
        "Keycloak test user " + USERNAME + " was not found - realm import may have failed");
  }

  private void addOrganizationMember(String organizationId, String userId) {
    // Bare JSON string body, not a JSON object - confirmed live (any other shape returns HTTP 415).
    send("POST", adminBase().resolve("organizations/" + organizationId + "/members"),
        userId, 201, 204, 409);
  }

  private void addOrganizationGroupMember(String organizationId, String groupId, String userId) {
    send("PUT",
        adminBase().resolve("organizations/" + organizationId + "/groups/" + groupId + "/members/" + userId),
        null, 204, 409);
  }

  private String clientUuid() {
    String resolved = clientUuid;
    if (resolved != null) {
      return resolved;
    }
    String query = URLEncoder.encode(CLIENT_ID, StandardCharsets.UTF_8);
    JsonNode clients =
        send("GET", URI.create(adminBase().resolve("clients").toString() + "?clientId=" + query), null, 200)
            .body();
    for (JsonNode client : clients) {
      if (CLIENT_ID.equals(client.path("clientId").asText())) {
        resolved = requiredText(client, "id");
        clientUuid = resolved;
        return resolved;
      }
    }
    throw new IllegalStateException(
        "Keycloak client " + CLIENT_ID + " was not found - realm import may have failed");
  }

  private URI adminBase() {
    return URI.create(baseUrl() + "/admin/realms/" + REALM + "/");
  }

  private String baseUrl() {
    String issuer = issuer().toString();
    String suffix = "/realms/" + REALM;
    if (!issuer.endsWith(suffix)) {
      throw new IllegalStateException("Unexpected Keycloak issuer shape: " + issuer);
    }
    return issuer.substring(0, issuer.length() - suffix.length());
  }

  private String adminBearerToken() {
    String resolved = adminToken;
    if (resolved != null) {
      return resolved;
    }
    try {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(baseUrl() + "/realms/master/protocol/openid-connect/token"))
              .header("Content-Type", "application/x-www-form-urlencoded")
              .POST(HttpRequest.BodyPublishers.ofString(
                  "grant_type=password&client_id=admin-cli&username=admin&password=admin-integration-only"))
              .build();
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IllegalStateException(
            "Keycloak admin token request returned HTTP " + response.statusCode() + ": " + response.body());
      }
      Matcher matcher = ACCESS_TOKEN.matcher(response.body());
      if (!matcher.find()) {
        throw new IllegalStateException("Keycloak admin token response has no access_token");
      }
      resolved = matcher.group(1);
      adminToken = resolved;
      return resolved;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while obtaining a Keycloak admin token", interrupted);
    } catch (IOException failure) {
      throw new UncheckedIOException("Unable to obtain a Keycloak admin token", failure);
    }
  }

  private Response send(String method, URI uri, Object body, int... expectedStatuses) {
    try {
      HttpRequest.BodyPublisher publisher =
          body == null ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body));
      HttpRequest request =
          HttpRequest.newBuilder(uri)
              .timeout(Duration.ofSeconds(30))
              .header("Authorization", "Bearer " + adminBearerToken())
              .header("Content-Type", "application/json")
              .method(method, publisher)
              .build();
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      for (int expected : expectedStatuses) {
        if (response.statusCode() == expected) {
          JsonNode json = response.body().isBlank() ? mapper.createObjectNode() : mapper.readTree(response.body());
          return new Response(response.statusCode(), json, response.headers().firstValue("Location"));
        }
      }
      throw new IllegalStateException("Keycloak Admin REST " + method + " " + uri + " returned HTTP "
          + response.statusCode() + ": " + response.body());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during a Keycloak Admin REST call", interrupted);
    } catch (IOException failure) {
      throw new UncheckedIOException("Keycloak Admin REST is unavailable", failure);
    }
  }

  private static String requiredText(JsonNode node, String field) {
    String value = node.path(field).asText();
    if (value.isBlank()) {
      throw new IllegalStateException("Keycloak response is missing " + field);
    }
    return value;
  }

  private static String lastPathSegment(String uri) {
    return uri.substring(uri.lastIndexOf('/') + 1);
  }

  private static String realmJson() {
    try (InputStream stream = KeycloakOrganizationFixture.class.getResourceAsStream(REALM_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException("Missing " + REALM_RESOURCE);
      }
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException("Unable to load " + REALM_RESOURCE, failure);
    }
  }

  private record Response(int status, JsonNode body, Optional<String> location) {}
}
```

Note the one real bug this exact shape already caught once (in fei, while building this same
class): `ensureClientRole`'s idempotency check must branch on the HTTP **status code**
(`existing.status() == 404`), not on response body content — a non-blank 404 error body reads as
"truthy" if you check the body instead of the status, and silently skips role creation.

### 2. One smoke resource + test per framework, in one representative deployment leaf

Pick any single `openworkflow-deployments/{capability}/{framework}` leaf per framework — the wiring
under test (the shared, product-wide `ActiveOrganizationProvider`) is identical regardless of which
capability hosts it. This plan uses `definition-management` as the illustrative example (its
Micronaut JAX-RS wiring was directly confirmed during the fei work this plan is based on), but
`execution-management` or `engine-pekko`/`engine-kafka-streams` work identically.

Add to each leaf's `pom.xml` (test scope): `openworkflow-authorization-testkit`, `rest-assured`
(Quarkus/Spring) — Micronaut's own tests should instead use its native `@Client`-injected
`HttpClient` (see below; this matches fowf's own existing Micronaut test style, e.g.
`StudioControllerTest`, which never uses rest-assured).

**`AuthorizationSmokeResource`** (identical shape across all three frameworks - a plain JAX-RS
resource, framework-specific only in how it gets registered):

```java
@Path("/internal/v1/smoke/active-organization")
public class AuthorizationSmokeResource {
  private final ActiveOrganizationProvider organizations;

  public AuthorizationSmokeResource(ActiveOrganizationProvider organizations) {
    this.organizations = organizations;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public ActiveOrganizationView current() {
    ActiveOrganization organization = organizations.current();
    return new ActiveOrganizationView(
        organization.tenantId().value().toString(),
        organization.organizationId(),
        organization.actorId(),
        organization.organizationRoles());
  }

  public record ActiveOrganizationView(
      String tenantId, String organizationId, String actorId, Set<String> organizationRoles) {}
}
```

For Quarkus and Spring, a plain `@ApplicationScoped`/`@Component`-registered instance (Quarkus REST
discovers `@Path` automatically; Spring needs a `ResourceConfigCustomizer` bean registering the
instance into the shared Jersey `ResourceConfig` — register-by-instance is safe here since this
resource has no `EntityManager`/persistence dependency to force into premature construction).
Micronaut needs only `@Singleton` on the class — confirmed against fowf's own real
`MicronautWorkflowDefinitionResource`/`MicronautStudioAuthorizationResource` that **no
Micronaut-specific HTTP annotation is needed**; `micronaut-jaxrs-server` inherits routing straight
from the JAX-RS `@Path`/`@GET` annotations, and neither real example carries a per-resource security
annotation either (authentication is enforced by `micronaut-security`'s secure-by-default filter
chain, bridged to JAX-RS resources by `micronaut-jaxrs-server-security` — **both** dependencies are
required; without `-server-security` specifically, `micronaut-jaxrs-server`'s routes are not
intercepted by `micronaut-security` at all, so authentication is silently never enforced). Confirm
against whichever leaf you pick whether its Micronaut module already carries
`micronaut-jaxrs-server` + `micronaut-jaxrs-server-security` (it does for `definition-management`)
or needs them added.

**Test config per framework** (dynamic, test-scoped only — do not change checked-in
`application.yaml`):

- **Quarkus** (`@QuarkusTestResource`, `QuarkusTestResourceLifecycleManager.start()` returning a
  config map):
  ```java
  Map.of(
      "quarkus.oidc.auth-server-url", fixture.issuer().toString(),
      "quarkus.oidc.client-id", KeycloakOrganizationFixture.CLIENT_ID,
      "quarkus.oidc.application-type", "service",
      "openworkflow.authorization.organization-client-id", KeycloakOrganizationFixture.CLIENT_ID);
  ```
- **Spring** (`@DynamicPropertySource`):
  ```java
  registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> fixture.issuer().toString());
  registry.add("openworkflow.authorization.organization-client-id", () -> KeycloakOrganizationFixture.CLIENT_ID);
  ```
- **Micronaut** (`implements TestPropertyProvider`, `getProperties()` — called *before* the
  application context builds, so start the container here, not in a `@BeforeAll`). **Confirmed via
  fowf's own real `openworkflow-definition-management-micronaut/src/main/resources/application.yaml`:
  unlike Quarkus/Spring, `micronaut-security-jwt` does not do issuer-based OIDC auto-discovery — it
  needs an explicit JWKS URL:**
  ```java
  Map.of(
      "micronaut.security.enabled", "true",
      "micronaut.security.authentication", "bearer",
      "micronaut.security.token.jwt.signatures.jwks.keycloak.url",
      fixture.issuer().toString() + "/protocol/openid-connect/certs",
      "openworkflow.authorization.organization-client-id", KeycloakOrganizationFixture.CLIENT_ID);
  ```
  Inject the client with `@Inject @Client("/") HttpClient http;` and drive requests with
  `http.toBlocking().exchange(HttpRequest.GET(path).bearerAuth(token), ResponseType.class)`;
  assert a 401 via `assertThrows(HttpClientResponseException.class, ...)` on the unauthenticated
  call.

Two tests per framework: a success round-trip (assert `tenantId`/`organizationId`/`actorId`/
`organizationRoles` all match what `provisionTenant(...)` set up) and an unauthenticated-request
401.

### 3. Known build-tool gotchas to pre-empt (all confirmed real, this session, building the fei
equivalent — expect the same class of issue here)

- **spotless**: every new file will almost certainly fail `spotless:check` on first write (line-wrap
  formatting). Run `mvn -pl <module> spotless:apply` before every rebuild rather than hand-formatting.
- **hibernate-processor's annotation-processing classpath gap**: the root pom's globally-registered
  `hibernate-processor` (via `pluginManagement`) runs on every module by default, with a narrower
  annotation-processing classpath than the regular compile classpath. Any module newly gaining a
  dependency that references `jakarta.enterprise.context.NormalScope` (CDI), Kotlin metadata
  annotations, or `tools.jackson.*` (Jackson 3.x) annotations will hit a "class file not found"
  warning (fatal under `-Werror`). Fix per-module by adding the specific missing annotation artifact
  as an explicit (often `provided`-scope) dependency to whichever module actually hit the gap — this
  recurred independently in fei's Quarkus, Spring, *and* Micronaut equivalents of this exact change,
  so expect it here too, most likely wherever `openworkflow-authorization-testkit` or the smoke
  resource's own module first pulls in `jackson-databind`/JWT/security dependencies it didn't have
  before.
- **Micronaut's `default-testCompile` execution does not inherit the main-compile
  `annotationProcessorPaths` override** — confirmed by reading fowf's own real
  `openworkflow-deployments/studio/micronaut/pom.xml`, which explicitly repeats the
  `annotationProcessorPaths` config under a `default-testCompile` execution id, separately from the
  main-scope override. If the target leaf's test sources reference a new `@Serdeable` type
  (`ActiveOrganizationView`, if it lives in that leaf rather than being reused from elsewhere) or
  otherwise need Micronaut annotation processing, the leaf's `pom.xml` needs this same explicit
  `default-testCompile` override, not just the usual main-scope one.
- **`micronaut-serde-processor`**: only needed as an explicit `annotationProcessorPaths` entry in
  whichever module declares a *new* `@Serdeable`-annotated type of its own (confirmed:
  `openworkflow-definition-management-micronaut` needs no such override because it only reuses
  already-serde-processed generated model types; the top-level `openworkflow-micronaut-binding`
  declares it explicitly). If `ActiveOrganizationView` is a fresh record in the smoke-resource's own
  module, that module needs this processor path added.
- **duplicate-finder-maven-plugin**: adding Jersey (Spring's JAX-RS runtime) alongside
  `jakarta.enterprise.cdi-api` can produce a genuine "duplicate but equal" resource conflict on
  `beans_1_0.xsd`–`beans_4_1.xsd`, bundled identically by both artifacts at the jar root (not under
  `META-INF/`, so a `.*META-INF/.*` ignore pattern won't catch it). If this build already has an
  `<ignoredResourcePattern>beans_.*\.xsd</ignoredResourcePattern>` entry (check the root pom's
  duplicate-finder config first) this is already handled; if not, and the conflict surfaces, that's
  the fix.

## Open item: do not speculatively bump `keycloak.version`

The confirmed-working Organization Groups API sequence above was verified live on Keycloak
**26.7.2**. The platform's pinned version (`forwardmeasure-platform/pom.xml`, `keycloak.version`
property) is **26.7.1** — one patch behind. This property's blast radius is wide (every consumer
across the ecosystem, not just this testkit), and 26.7.1's actual behavior for the Organization
Groups API is genuinely unverified in either direction as of this writing. **Do not bump it
speculatively.** Build and run this plan's tests against 26.7.1 as pinned; if they fail specifically
on Organization Groups calls (HTTP 404/400 where 26.7.2 is confirmed to return 200/201/204), that's
real first-party evidence the version needs bumping — raise it as its own decision at that point,
not as a prerequisite assumption baked into this plan.

## Verification

Once built: `mvn -pl openworkflow-authorization/openworkflow-authorization-testkit,openworkflow-deployments/<capability>/{quarkus,spring,micronaut} -am test`
(adjust module list to the leaf actually chosen). Expect first-run failures to be spotless/classpath
gaps (see above), not logic failures — the REST sequence and config keys in this plan are
transcribed from a working implementation, not invented for this document.
