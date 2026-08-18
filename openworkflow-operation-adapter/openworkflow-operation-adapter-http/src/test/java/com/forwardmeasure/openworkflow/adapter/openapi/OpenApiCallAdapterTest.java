package com.forwardmeasure.openworkflow.adapter.openapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class OpenApiCallAdapterTest {
  @Test
  void invokesOperationFromPinnedDocumentWithTypedParameters() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/pets/42",
        exchange -> {
          ObjectNode response = JsonNodeFactory.instance.objectNode();
          response.put("query", exchange.getRequestURI().getRawQuery());
          response.put("trace", exchange.getRequestHeaders().getFirst("x-trace"));
          response.put("operationId", exchange.getRequestHeaders().getFirst("X-OKS-Operation-Id"));
          response.put("tenantDid", exchange.getRequestHeaders().getFirst("X-OKS-Tenant-DID"));
          response.put("actorDid", exchange.getRequestHeaders().getFirst("X-OKS-Actor-DID"));
          response.put(
              "identityProvider", exchange.getRequestHeaders().getFirst("X-OKS-Identity-Provider"));
          response.put(
              "subjectIdentifier",
              exchange.getRequestHeaders().getFirst("X-OKS-Subject-Identifier"));
          response.put(
              "workflowRunId", exchange.getRequestHeaders().getFirst("X-OKS-Workflow-Run-Id"));
          response.put(
              "correlationId", exchange.getRequestHeaders().getFirst("X-OKS-Correlation-Id"));
          byte[] body = response.toString().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      String document =
          """
          openapi: 3.1.0
          info:
            title: Pets
            version: 1.0.0
          servers:
            - url: http://127.0.0.1:%d/v1
          paths:
            /pets/{petId}:
              get:
                operationId: getPet
                x-oks-propagate-identity: true
                parameters:
                  - name: petId
                    in: path
                    required: true
                    schema:
                      type: integer
                  - name: status
                    in: query
                    required: true
                    schema:
                      type: string
                  - name: x-trace
                    in: header
                    required: true
                    schema:
                      type: string
                  - {name: X-OKS-Operation-Id, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Tenant-DID, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Actor-DID, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Identity-Provider, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Subject-Identifier, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Workflow-Run-Id, in: header, required: true, schema: {type: string}}
                  - {name: X-OKS-Correlation-Id, in: header, required: true, schema: {type: string}}
                responses:
                  '200':
                    description: OK
          """
              .formatted(server.getAddress().getPort());
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/pets.yaml"), "application/yaml", document);
      ObjectNode arguments = JsonNodeFactory.instance.objectNode();
      arguments.putObject("document").put("endpoint", resource.uri().toString());
      arguments.put("operationId", "getPet");
      arguments.put("output", "content");
      arguments
          .putObject("parameters")
          .put("petId", 42)
          .put("status", "available")
          .put("x-trace", "trace-1");
      ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
      descriptor.put("operationId", "operation-1");
      descriptor.put("operationKind", "call");
      descriptor.put("executionKey", executionKey());
      descriptor.put("definitionReference", "definition-reference-1");
      descriptor.put("callKind", "OPEN_API");
      descriptor.put("resourceKind", WorkflowResourceKind.OPEN_API_DOCUMENT.name());
      descriptor.put("resourceUri", resource.uri().toString());
      descriptor.put("resourceSha256", resource.sha256());
      descriptor.set("arguments", arguments);
      descriptor.set("taskInput", JsonNodeFactory.instance.objectNode());
      OperationRequest request =
          new OperationRequest(
              "operation-1", "call", "definition-reference-1", descriptor, resource, actor());

      var result =
          new OpenApiCallAdapter()
              .execute(request, progress -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          result.status(),
          () ->
              result.error() == null
                  ? "No workflow error was returned"
                  : result.error().toString());
      assertNull(result.error());
      assertEquals("status=available", result.output().inlineValue().required("query").textValue());
      assertEquals("trace-1", result.output().inlineValue().required("trace").textValue());
      assertEquals(
          "operation-1", result.output().inlineValue().required("operationId").textValue());
      assertEquals(
          tenant().toString(), result.output().inlineValue().required("tenantDid").textValue());
      assertEquals(
          "did:web:tenant.example.com:actors:user-1",
          result.output().inlineValue().required("actorDid").textValue());
      assertEquals(
          "https://auth.example.com/realms/forwardmeasure",
          result.output().inlineValue().required("identityProvider").textValue());
      assertEquals(
          "keycloak-subject-1",
          result.output().inlineValue().required("subjectIdentifier").textValue());
      assertEquals(
          "execution-1", result.output().inlineValue().required("workflowRunId").textValue());
      assertEquals(
          "correlation-1", result.output().inlineValue().required("correlationId").textValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void resolvesRelativeServerAgainstPinnedDocumentLocation() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/contracts/api/v1/extractions",
        exchange -> {
          byte[] response = "{\"status\":\"completed\"}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      URI documentUri =
          URI.create(
              "http://127.0.0.1:%d/contracts/openapi.yaml"
                  .formatted(server.getAddress().getPort()));
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              documentUri,
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Information Extraction
                version: 1.0.0
              servers:
                - url: ./api/v1
              paths:
                /extractions:
                  post:
                    operationId: submitExtractionBatch
                    requestBody:
                      required: true
                      content:
                        application/json:
                          schema:
                            type: object
                    responses:
                      '200':
                        description: Complete
                        content:
                          application/json:
                            schema:
                              type: object
                              required: [status]
                              properties:
                                status:
                                  const: completed
              """);
      ObjectNode parameters = JsonNodeFactory.instance.objectNode();
      parameters.putObject("requestBody").put("completion_mode", "sync_required");

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request("relative-server", resource, "submitExtractionBatch", parameters),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.SUCCEEDED, result.status());
      assertEquals("completed", result.output().inlineValue().required("status").textValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void invokesOperationWhosePathItemIsInAPinnedExternalDocument() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/pets/42",
        exchange -> {
          byte[] body = "{\"id\":42}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource root =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/openapi.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Pets
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d/v1
              paths:
                /pets/{petId}:
                  $ref: ./paths.yaml#/pets
              """
                  .formatted(server.getAddress().getPort()));
      ResolvedWorkflowResource paths =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/paths.yaml"),
              "application/yaml",
              """
              pets:
                get:
                  operationId: getPet
                  parameters:
                    - $ref: ./parameters.yaml#/petId
                  responses:
                    '200':
                      description: OK
              """);
      ResolvedWorkflowResource parameters =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/parameters.yaml"),
              "application/yaml",
              """
              petId:
                name: petId
                in: path
                required: true
                schema:
                  type: integer
              """);
      ObjectNode arguments = JsonNodeFactory.instance.objectNode();
      arguments.put("operationId", "getPet");
      arguments.putObject("parameters").put("petId", 42);
      ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
      descriptor.put("operationId", "operation-external");
      descriptor.put("operationKind", "call");
      descriptor.put("taskPath", "/do/0/invoke-http");
      descriptor.put("definitionReference", "definition-reference-1");
      descriptor.put("callKind", "OPEN_API");
      descriptor.put("resourceKind", WorkflowResourceKind.OPEN_API_DOCUMENT.name());
      descriptor.put("resourceUri", root.uri().toString());
      descriptor.put("resourceSha256", root.sha256());
      descriptor.set("arguments", arguments);
      descriptor.set("taskInput", JsonNodeFactory.instance.objectNode());
      OperationRequest request =
          new OperationRequest(
              "operation-external",
              "call",
              "definition-reference-1",
              descriptor,
              root,
              actor(),
              "effect-external",
              Instant.parse("2026-07-29T12:00:00Z"),
              null,
              List.of(root, paths, parameters));

      var result =
          new OpenApiCallAdapter()
              .execute(request, progress -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.SUCCEEDED, result.status());
      assertEquals(42, result.output().inlineValue().required("id").intValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void serializesOpenApiStylesOverridesAndJsonRequestBody() throws Exception {
    CompletableFuture<ObjectNode> received = new CompletableFuture<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/pets/.red.blue",
        exchange -> {
          ObjectNode request = JsonNodeFactory.instance.objectNode();
          request.put("query", exchange.getRequestURI().getRawQuery());
          request.put("filter", exchange.getRequestHeaders().getFirst("x-filter"));
          request.put("cookie", exchange.getRequestHeaders().getFirst("Cookie"));
          request.put("contentType", exchange.getRequestHeaders().getFirst("Content-Type"));
          request.set(
              "body",
              new com.fasterxml.jackson.databind.ObjectMapper()
                  .readTree(exchange.getRequestBody()));
          received.complete(request);
          byte[] response = "{\"accepted\":true}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/styles.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Styled Pets
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d/v1
              paths:
                /pets/{colors}:
                  parameters:
                    - name: x-filter
                      in: header
                      required: true
                      schema:
                        type: string
                  post:
                    operationId: createPet
                    parameters:
                      - name: colors
                        in: path
                        required: true
                        style: label
                        explode: true
                        schema:
                          type: array
                      - name: tags
                        in: query
                        style: form
                        explode: true
                        schema:
                          type: array
                      - name: criteria
                        in: query
                        style: deepObject
                        explode: true
                        schema:
                          type: object
                      - name: filterJson
                        in: query
                        content:
                          application/json:
                            schema:
                              type: object
                      - name: x-filter
                        in: header
                        required: true
                        style: simple
                        explode: true
                        schema:
                          type: object
                      - name: session
                        in: cookie
                        style: form
                        explode: true
                        schema:
                          type: array
                    requestBody:
                      required: true
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              name:
                                type: string
                              age:
                                type: integer
                    responses:
                      '200':
                        description: OK
              """
                  .formatted(server.getAddress().getPort()));
      ObjectNode parameters = JsonNodeFactory.instance.objectNode();
      parameters.putArray("colors").add("red").add("blue");
      parameters.putArray("tags").add("new").add("featured");
      parameters.putObject("criteria").put("status", "available").put("owner", "a/b");
      parameters.putObject("filterJson").put("kind", "cat");
      parameters.putObject("x-filter").put("kind", "cat").put("size", "small");
      parameters.putArray("session").add("one").add("two");
      parameters.put("name", "Milo");
      parameters.put("age", 4);

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request("operation-styles", resource, "createPet", parameters), progress -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.SUCCEEDED, result.status());
      ObjectNode invocation = received.get(10, TimeUnit.SECONDS);
      assertEquals(
          "tags=new&tags=featured"
              + "&criteria%5Bstatus%5D=available"
              + "&criteria%5Bowner%5D=a%2Fb"
              + "&filterJson=%7B%22kind%22%3A%22cat%22%7D",
          invocation.required("query").textValue());
      assertEquals("kind=cat,size=small", invocation.required("filter").textValue());
      assertEquals("session=one; session=two", invocation.required("cookie").textValue());
      assertEquals("application/json", invocation.required("contentType").textValue());
      assertEquals("Milo", invocation.required("body").required("name").textValue());
      assertEquals(4, invocation.required("body").required("age").intValue());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void encodesSwagger2UrlEncodedAndMultipartFormData() throws Exception {
    CompletableFuture<ObjectNode> urlEncoded = new CompletableFuture<>();
    CompletableFuture<ObjectNode> multipart = new CompletableFuture<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/forms/url",
        exchange -> {
          ObjectNode received = JsonNodeFactory.instance.objectNode();
          received.put("contentType", exchange.getRequestHeaders().getFirst("Content-Type"));
          received.put(
              "body", new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          urlEncoded.complete(received);
          byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.createContext(
        "/forms/multipart",
        exchange -> {
          ObjectNode received = JsonNodeFactory.instance.objectNode();
          received.put("contentType", exchange.getRequestHeaders().getFirst("Content-Type"));
          received.put(
              "body", new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          multipart.complete(received);
          byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/forms.yaml"),
              "application/yaml",
              """
              swagger: '2.0'
              info:
                title: Forms
                version: 1.0.0
              schemes: [http]
              host: 127.0.0.1:%d
              basePath: /forms
              paths:
                /url:
                  post:
                    operationId: submitUrl
                    consumes:
                      - application/x-www-form-urlencoded
                    parameters:
                      - name: label
                        in: formData
                        required: true
                        type: string
                      - name: tags
                        in: formData
                        type: array
                        items:
                          type: string
                        collectionFormat: multi
                    responses:
                      '200':
                        description: OK
                /multipart:
                  post:
                    operationId: submitMultipart
                    consumes:
                      - multipart/form-data
                    parameters:
                      - name: description
                        in: formData
                        required: true
                        type: string
                      - name: codes
                        in: formData
                        type: array
                        items:
                          type: string
                        collectionFormat: multi
                    responses:
                      '200':
                        description: OK
              """
                  .formatted(server.getAddress().getPort()));
      ObjectNode urlParameters = JsonNodeFactory.instance.objectNode();
      urlParameters.put("label", "Evidence A/B");
      urlParameters.putArray("tags").add("one").add("two");
      ObjectNode multipartParameters = JsonNodeFactory.instance.objectNode();
      multipartParameters.put("description", "Evidence description");
      multipartParameters.putArray("codes").add("A").add("B");

      var adapter = new OpenApiCallAdapter();
      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          adapter
              .execute(request("swagger-url", resource, "submitUrl", urlParameters), ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS)
              .status());
      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          adapter
              .execute(
                  request("swagger-multipart", resource, "submitMultipart", multipartParameters),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS)
              .status());

      ObjectNode receivedUrl = urlEncoded.get(10, TimeUnit.SECONDS);
      assertEquals(
          "application/x-www-form-urlencoded", receivedUrl.required("contentType").textValue());
      assertEquals(
          "label=Evidence%20A%2FB&tags=one&tags=two", receivedUrl.required("body").textValue());

      ObjectNode receivedMultipart = multipart.get(10, TimeUnit.SECONDS);
      String contentType = receivedMultipart.required("contentType").textValue();
      assertTrue(contentType.startsWith("multipart/form-data; boundary=oks-"));
      String body = receivedMultipart.required("body").textValue();
      assertTrue(body.contains("name=\"description\"\r\n\r\n" + "Evidence description\r\n"));
      assertTrue(body.contains("name=\"codes\"\r\n\r\nA\r\n"));
      assertTrue(body.contains("name=\"codes\"\r\n\r\nB\r\n"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void acceptsNullForAnOpenApi30NullableResponseProperty() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/extractions",
        exchange -> {
          byte[] response =
              ("""
              {"request_id":"request-1","profile":null,
               "profile_version":null,"status":"completed"}
              """)
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/ner.yaml"),
              "application/yaml",
              """
              openapi: 3.0.3
              info:
                title: Named Entity Recognition
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /extractions:
                  post:
                    operationId: extractEntities
                    responses:
                      '200':
                        description: Complete
                        content:
                          application/json:
                            schema:
                              type: object
                              required: [request_id, profile, profile_version, status]
                              properties:
                                request_id: {type: string}
                                profile:
                                  type: string
                                  nullable: true
                                profile_version:
                                  type: string
                                  nullable: true
                                status:
                                  type: string
                                  enum: [completed]
              """
                  .formatted(server.getAddress().getPort()));

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request(
                      "nullable-response",
                      resource,
                      "extractEntities",
                      JsonNodeFactory.instance.objectNode()),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          result.status(),
          () ->
              result.error() == null
                  ? "No workflow error was returned"
                  : result.error().toString());
      assertNull(result.error());
      assertTrue(result.output().inlineValue().required("profile").isNull());
      assertTrue(result.output().inlineValue().required("profile_version").isNull());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsAResponseThatViolatesTheSelectedOpenApiSchema() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/items/invalid",
        exchange -> {
          byte[] response = "{\"id\":\"not-an-integer\"}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json; charset=utf-8");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/response.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Response Validation
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /items/invalid:
                  get:
                    operationId: invalidItem
                    responses:
                      '200':
                        description: Item
                        content:
                          application/json:
                            schema:
                              type: object
                              required: [id]
                              properties:
                                id:
                                  type: integer
              """
                  .formatted(server.getAddress().getPort()));
      var result =
          new OpenApiCallAdapter()
              .execute(
                  request(
                      "invalid-response",
                      resource,
                      "invalidItem",
                      JsonNodeFactory.instance.objectNode()),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.FAILED, result.status());
      assertEquals(OpenApiCallAdapter.RESPONSE_VALIDATION_ERROR, result.error().type());
      assertTrue(result.error().detail().contains("integer"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void acceptsNullForAnOpenApi30NullableRequestBodyProperty() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/extractions/1/complete",
        exchange -> {
          requests.incrementAndGet();
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/control.yaml"),
              "application/yaml",
              """
              openapi: 3.0.3
              info:
                title: Extraction Control
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /extractions/1/complete:
                  post:
                    operationId: completeExtraction
                    requestBody:
                      required: true
                      content:
                        application/json:
                          schema:
                            type: object
                            required: [normalizedValue]
                            properties:
                              normalizedValue:
                                type: string
                                nullable: true
                    responses:
                      '204':
                        description: Complete
              """
                  .formatted(server.getAddress().getPort()));
      ObjectNode parameters = JsonNodeFactory.instance.objectNode();
      parameters.putObject("requestBody").putNull("normalizedValue");

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request("nullable-request-body", resource, "completeExtraction", parameters),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          result.status(),
          () ->
              result.error() == null
                  ? "No workflow error was returned"
                  : result.error().toString());
      assertEquals(1, requests.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rejectsInvalidParametersAndRequestBodiesBeforeNetworkIo() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/items/1",
        exchange -> {
          requests.incrementAndGet();
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/request.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Request Validation
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /items/{id}:
                  post:
                    operationId: createItem
                    parameters:
                      - name: id
                        in: path
                        required: true
                        schema:
                          type: integer
                    requestBody:
                      required: true
                      content:
                        application/json:
                          schema:
                            type: object
                            required: [count]
                            properties:
                              count:
                                type: integer
                                minimum: 1
                    responses:
                      '204':
                        description: Created
              """
                  .formatted(server.getAddress().getPort()));

      ObjectNode invalidParameter = JsonNodeFactory.instance.objectNode();
      invalidParameter.put("id", "not-an-integer");
      invalidParameter.putObject("requestBody").put("count", 1);
      var parameterResult =
          new OpenApiCallAdapter()
              .execute(
                  request("invalid-parameter", resource, "createItem", invalidParameter),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.FAILED, parameterResult.status());
      assertEquals(400, parameterResult.error().status());
      assertEquals(OpenApiCallAdapter.REQUEST_VALIDATION_ERROR, parameterResult.error().type());

      ObjectNode invalidBody = JsonNodeFactory.instance.objectNode();
      invalidBody.put("id", 1);
      invalidBody.putObject("requestBody").put("count", "not-an-integer");
      var bodyResult =
          new OpenApiCallAdapter()
              .execute(request("invalid-body", resource, "createItem", invalidBody), ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.FAILED, bodyResult.status());
      assertEquals(400, bodyResult.error().status());
      assertEquals(OpenApiCallAdapter.REQUEST_VALIDATION_ERROR, bodyResult.error().type());
      assertEquals(0, requests.get(), "Invalid requests must fail before network I/O");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void resolvesRelativeExternalReferencesFromAnOpenApi30ResponseSchema() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/plans/1",
        exchange -> {
          byte[] response = "{\"detail\":{\"id\":1}}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource root =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/control.yaml"),
              "application/yaml",
              """
              openapi: 3.0.3
              info:
                title: Extraction Control
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /plans/1:
                  get:
                    operationId: prepareExtraction
                    responses:
                      '200':
                        description: Plan
                        content:
                          application/json:
                            schema:
                              type: object
                              required: [detail]
                              properties:
                                detail:
                                  $ref: ./document-parsing.openapi.yaml#/definitions/Detail
              """
                  .formatted(server.getAddress().getPort()));
      ResolvedWorkflowResource parsing =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/" + "document-parsing.openapi.yaml"),
              "application/yaml",
              """
              definitions:
                Detail:
                  type: object
                  required: [id]
                  properties:
                    id: {type: integer}
              """);

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request(
                      "relative-external-response",
                      root,
                      "prepareExtraction",
                      JsonNodeFactory.instance.objectNode(),
                      List.of(root, parsing)),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(
          OperationObservationStatus.SUCCEEDED,
          result.status(),
          () ->
              result.error() == null
                  ? "No workflow error was returned"
                  : result.error().toString());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void validatesResponseAgainstPinnedExternalSchema() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/external-invalid",
        exchange -> {
          byte[] response = "{\"id\":\"wrong-type\"}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource root =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/external-response.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: External Response
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /external-invalid:
                  get:
                    operationId: externalInvalid
                    responses:
                      '200':
                        description: Item
                        content:
                          application/json:
                            schema:
                              $ref: ./schemas.yaml#/$defs/Item
              """
                  .formatted(server.getAddress().getPort()));
      ResolvedWorkflowResource schemas =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/schemas.yaml"),
              "application/yaml",
              """
              $schema: https://json-schema.org/draft/2020-12/schema
              $defs:
                Item:
                  type: object
                  required: [id]
                  properties:
                    id:
                      type: integer
              """);

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request(
                      "external-invalid-response",
                      root,
                      "externalInvalid",
                      JsonNodeFactory.instance.objectNode(),
                      List.of(root, schemas)),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.FAILED, result.status());
      assertEquals(OpenApiCallAdapter.RESPONSE_VALIDATION_ERROR, result.error().type());
      assertTrue(result.error().detail().contains("integer"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void resolvesDocumentLocalReferencesInsideAReferencedRequestSchema() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/ingestions",
        exchange -> {
          requests.incrementAndGet();
          byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    try {
      ResolvedWorkflowResource resource =
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/ingestion.yaml"),
              "application/yaml",
              """
              openapi: 3.1.0
              info:
                title: Ingestion
                version: 1.0.0
              servers:
                - url: http://127.0.0.1:%d
              paths:
                /ingestions:
                  post:
                    operationId: submitIngestion
                    requestBody:
                      required: true
                      content:
                        application/json:
                          schema:
                            $ref: '#/components/schemas/ingestionRequest'
                    responses:
                      '200':
                        description: Accepted
              components:
                schemas:
                  ingestionSource:
                    type: object
                    required: [uri]
                    properties:
                      uri: {type: string, format: uri}
                  ingestionRequest:
                    type: object
                    required: [source]
                    properties:
                      source:
                        $ref: '#/components/schemas/ingestionSource'
              """
                  .formatted(server.getAddress().getPort()));
      ObjectNode parameters = JsonNodeFactory.instance.objectNode();
      parameters
          .putObject("requestBody")
          .putObject("source")
          .put("uri", "s3://evidence/source.jsonl");

      var result =
          new OpenApiCallAdapter()
              .execute(
                  request("document-local-reference", resource, "submitIngestion", parameters),
                  ignored -> {})
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);

      assertEquals(OperationObservationStatus.SUCCEEDED, result.status());
      assertEquals(1, requests.get());
    } finally {
      server.stop(0);
    }
  }

  private OperationRequest request(
      String operationId,
      ResolvedWorkflowResource resource,
      String openApiOperationId,
      ObjectNode parameters) {
    return request(operationId, resource, openApiOperationId, parameters, List.of(resource));
  }

  private OperationRequest request(
      String operationId,
      ResolvedWorkflowResource resource,
      String openApiOperationId,
      ObjectNode parameters,
      List<ResolvedWorkflowResource> resources) {
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.put("operationId", openApiOperationId);
    arguments.set("parameters", parameters);
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", operationId);
    descriptor.put("operationKind", "call");
    descriptor.put("executionKey", executionKey());
    descriptor.put("taskPath", "/do/0/invoke-openapi");
    descriptor.put("definitionReference", "definition-reference-1");
    descriptor.put("callKind", "OPEN_API");
    descriptor.put("resourceKind", WorkflowResourceKind.OPEN_API_DOCUMENT.name());
    descriptor.put("resourceUri", resource.uri().toString());
    descriptor.put("resourceSha256", resource.sha256());
    descriptor.set("arguments", arguments);
    descriptor.set("taskInput", JsonNodeFactory.instance.objectNode());
    return new OperationRequest(
        operationId,
        "call",
        "definition-reference-1",
        descriptor,
        resource,
        actor(),
        "effect-" + operationId,
        Instant.parse("2026-07-29T12:00:00Z"),
        null,
        resources);
  }

  private ActorContext actor() {
    OksTenantId tenant = tenant();
    return new ActorContext(
        tenant,
        ActorId.parse("did:web:tenant.example.com:actors:user-1"),
        ActorType.HUMAN,
        "User One",
        "ssb-public",
        BusinessCorrelationId.parse("correlation-1"),
        Set.of(),
        null,
        Instant.parse("2026-07-29T12:00:00Z"),
        "https://auth.example.com/realms/forwardmeasure",
        "keycloak-subject-1");
  }

  private static OksTenantId tenant() {
    return OksTenantId.parse("did:web:tenant.example.com");
  }

  private static String executionKey() {
    return new ExecutionKey(tenant(), new WorkflowExecutionId("execution-1")).canonical();
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
