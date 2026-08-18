package com.forwardmeasure.openworkflow.adapter.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.adapter.api.OperationAdapter;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationDocumentGraph;
import com.forwardmeasure.openworkflow.adapter.api.OperationDocumentGraph.LocatedNode;
import com.forwardmeasure.openworkflow.adapter.api.OperationProgressSink;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.http.HttpCallAdapter;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidationException;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidator;
import com.forwardmeasure.openworkflow.definition.ResolvedDataSchema;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Resolves a digest-pinned OpenAPI/Swagger operation into a standards-native HTTP call without
 * fetching the document at execution time.
 */
public final class OpenApiCallAdapter implements OperationAdapter, AutoCloseable {
  public static final String RESPONSE_VALIDATION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/validation";
  public static final String REQUEST_VALIDATION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/validation";
  private static final List<String> METHODS =
      List.of("get", "put", "post", "delete", "options", "head", "patch", "trace");

  private final HttpCallAdapter http;
  private final OperationDataReferenceFactory dataReferences;

  public OpenApiCallAdapter() {
    this(OperationDataReferenceFactory.boundedInline());
  }

  public OpenApiCallAdapter(OperationDataReferenceFactory dataReferences) {
    this(
        new HttpCallAdapter(
            java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
                .connectTimeout(java.time.Duration.ofSeconds(30))
                .build(),
            new com.fasterxml.jackson.databind.ObjectMapper(),
            dataReferences),
        dataReferences);
  }

  public OpenApiCallAdapter(HttpCallAdapter http) {
    this(http, OperationDataReferenceFactory.boundedInline());
  }

  public OpenApiCallAdapter(HttpCallAdapter http, OperationDataReferenceFactory dataReferences) {
    this.http = Objects.requireNonNull(http, "http");
    this.dataReferences = Objects.requireNonNull(dataReferences, "dataReferences");
  }

  @Override
  public boolean supports(OperationRequest request) {
    return "call".equals(request.operationKind())
        && "OPEN_API".equals(request.descriptor().path("callKind").textValue());
  }

  @Override
  public CompletionStage<OperationObservation> execute(
      OperationRequest request, OperationProgressSink progress) {
    if (!supports(request)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Operation is not an OpenAPI call"));
    }
    try {
      PreparedOpenApi prepared = prepare(request);
      return http.execute(prepared.httpRequest(), progress)
          .thenApply(observation -> validateAndProject(prepared, observation));
    } catch (DataSchemaValidationException invalid) {
      return CompletableFuture.completedFuture(requestValidationFailure(request, invalid));
    } catch (RuntimeException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
  }

  @Override
  public CompletionStage<Void> cancel(OperationRequest request) {
    try {
      return http.cancel(prepare(request).httpRequest());
    } catch (RuntimeException invalid) {
      return CompletableFuture.failedFuture(invalid);
    }
  }

  @Override
  public void close() {
    http.close();
  }

  private PreparedOpenApi prepare(OperationRequest request) {
    if (request.resource() == null) {
      throw new IllegalStateException("OpenAPI call has no resolved immutable document");
    }
    JsonNode arguments = dataReferences.resolveDescriptorValue(request, "arguments");
    OperationDocumentGraph graph =
        new OperationDocumentGraph(request.resource(), request.resources());
    LocatedNode document = graph.root();
    requireOpenApiDocument(document.node());
    DataSchemaValidator schemas = openApiValidator(request);
    Operation operation = findOperation(graph, document, requiredText(arguments, "operationId"));
    ObjectNode parameters =
        arguments.has("parameters")
            ? requireObject(arguments.required("parameters"), "OpenAPI parameters").deepCopy()
            : JsonNodeFactory.instance.objectNode();
    ObjectNode headers = JsonNodeFactory.instance.objectNode();
    List<OpenApiParameterSerializer.QueryValue> query = new ArrayList<>();
    List<OpenApiParameterSerializer.QueryValue> cookies = new ArrayList<>();
    List<OpenApiParameterSerializer.QueryValue> formFields = new ArrayList<>();
    String path = operation.path();
    JsonNode body = null;
    List<Parameter> declarations = parameterDeclarations(graph, operation);
    boolean propagateTrustedIdentity =
        operation.definition().node().path("x-oks-propagate-identity").asBoolean(false);
    if (propagateTrustedIdentity) {
      addTrustedIdentityParameters(request, parameters, declarations);
    }
    boolean swagger2 = "2.0".equals(document.node().path("swagger").asText());
    for (Parameter declared : declarations) {
      JsonNode parameter = declared.declaration().node();
      String name = requiredText(parameter, "name");
      String location = requiredText(parameter, "in");
      JsonNode value = takeParameterValue(parameters, declared, declarations);
      if (value == null) {
        value = defaultValue(parameter);
      }
      if (value == null) {
        if (parameter.path("required").asBoolean(false) || "path".equals(location)) {
          throw new IllegalArgumentException("Missing required OpenAPI parameter " + name);
        }
        continue;
      }
      LocatedNode parameterSchema = parameterSchema(graph, declared, swagger2);
      if (parameterSchema != null) {
        validateRequestSchema(
            schemas,
            document,
            parameterSchema,
            value,
            operation.path() + "/" + operation.method() + "/parameters/" + location + "/" + name);
      }
      if (swagger2) {
        switch (location) {
          case "path" ->
              path =
                  replacePath(
                      path,
                      name,
                      OpenApiParameterSerializer.swagger2(name, location, value, parameter),
                      false);
          case "query" ->
              query.addAll(OpenApiParameterSerializer.swagger2Query(name, value, parameter));
          case "header" ->
              headers.put(
                  name, OpenApiParameterSerializer.swagger2(name, location, value, parameter));
          case "body" -> {
            if (body != null) {
              throw new IllegalArgumentException(
                  "OpenAPI operation has more than one " + "body parameter");
            }
            body = value;
          }
          case "formData" ->
              formFields.addAll(OpenApiParameterSerializer.swagger2Form(name, value, parameter));
          default ->
              throw new IllegalArgumentException(
                  "Unsupported Swagger 2 parameter location " + location);
        }
        continue;
      }
      if (parameter.has("content")) {
        if (parameter.has("schema") || parameter.has("style") || parameter.has("explode")) {
          throw new IllegalArgumentException(
              "OpenAPI content parameter "
                  + name
                  + " cannot also declare schema/style/"
                  + "explode");
        }
        String serialized = OpenApiParameterSerializer.content(name, location, value, parameter);
        switch (location) {
          case "path" -> path = replacePath(path, name, serialized, true);
          case "query" ->
              query.add(new OpenApiParameterSerializer.QueryValue(percentEncode(name), serialized));
          case "header" -> headers.put(name, serialized);
          case "cookie" ->
              cookies.add(
                  new OpenApiParameterSerializer.QueryValue(percentEncode(name), serialized));
          default ->
              throw new IllegalArgumentException(
                  "Unsupported OpenAPI parameter location " + location);
        }
        continue;
      }
      switch (location) {
        case "path" ->
            path =
                replacePath(
                    path, name, OpenApiParameterSerializer.path(name, value, parameter), true);
        case "query" -> query.addAll(OpenApiParameterSerializer.query(name, value, parameter));
        case "header" ->
            headers.put(name, OpenApiParameterSerializer.header(name, value, parameter));
        case "cookie" -> cookies.addAll(OpenApiParameterSerializer.cookie(name, value, parameter));
        default ->
            throw new IllegalArgumentException(
                "Unsupported OpenAPI parameter location " + location);
      }
    }
    BodyPayload requestBody =
        swagger2
            ? swaggerRequestBody(
                body, formFields, consumes(document.node(), operation), request.operationId())
            : requestBody(graph, operation, parameters);
    if (requestBody.value() != null) {
      if (body != null && !swagger2) {
        throw new IllegalArgumentException("OpenAPI operation has conflicting request bodies");
      }
      body = requestBody.value();
      if (requestBody.schema() != null) {
        validateRequestSchema(
            schemas,
            document,
            requestBody.schema(),
            body,
            operation.path() + "/" + operation.method() + "/requestBody");
      }
      if (requestBody.mediaType() != null) {
        headers.put("Content-Type", requestBody.mediaType());
      }
    }
    if (!parameters.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown OpenAPI operation parameters: "
              + String.join(",", iterable(parameters.fieldNames())));
    }
    if (!cookies.isEmpty()) {
      headers.put("Cookie", cookieHeader(cookies));
    }

    ObjectNode httpArguments = JsonNodeFactory.instance.objectNode();
    httpArguments.put("method", operation.method());
    httpArguments.put(
        "endpoint",
        appendQuery(join(server(document.documentUri(), document.node(), operation), path), query));
    if (!headers.isEmpty()) {
      httpArguments.set("headers", headers);
    }
    if (body != null) {
      if (!allowsBody(operation.method())) {
        throw new IllegalArgumentException(
            "OpenAPI " + operation.method() + " operation cannot carry a request body");
      }
      httpArguments.set("body", encodeBody(body, requestBody.mediaType()));
    }
    String requestedOutput = arguments.path("output").asText("content");
    /*
     * Response contract validation needs the HTTP status, headers, and
     * parsed content. Raw output cannot be reconstructed losslessly, so it
     * remains raw and is covered by HTTP status handling only.
     */
    httpArguments.put("output", "raw".equals(requestedOutput) ? "raw" : "response");
    httpArguments.put("redirect", arguments.path("redirect").asBoolean(false));

    ObjectNode descriptor = request.descriptor().deepCopy();
    descriptor.put("originCallKind", "OPEN_API");
    descriptor.put("callKind", "HTTP");
    descriptor.put("propagateTrustedIdentity", propagateTrustedIdentity);
    descriptor.set("arguments", httpArguments);
    OperationRequest httpRequest =
        new OperationRequest(
            request.operationId(),
            request.operationKind(),
            request.definitionReference(),
            descriptor,
            null,
            request.requestedBy(),
            request.effectId(),
            request.requestedAt(),
            request.authentication());
    return new PreparedOpenApi(
        httpRequest, request, graph, document, operation, schemas, requestedOutput);
  }

  private static void addTrustedIdentityParameters(
      OperationRequest request, ObjectNode parameters, List<Parameter> declarations) {
    if (request.requestedBy().identityProvider() == null
        || request.requestedBy().subjectIdentifier() == null) {
      throw new SecurityException("OpenAPI operation requires persisted IdP coordinates");
    }
    putTrustedParameter(
        parameters, declarations, HttpCallAdapter.OPERATION_ID_HEADER, request.operationId());
    putTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.TENANT_DID_HEADER,
        request.requestedBy().tenantId().toString());
    putTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.ACTOR_DID_HEADER,
        request.requestedBy().actorId().toString());
    putTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.IDENTITY_PROVIDER_HEADER,
        request.requestedBy().identityProvider());
    putTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.SUBJECT_IDENTIFIER_HEADER,
        request.requestedBy().subjectIdentifier());
    putTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.WORKFLOW_RUN_ID_HEADER,
        request.executionKey().executionId().value());
    putOptionalTrustedParameter(
        parameters,
        declarations,
        HttpCallAdapter.CORRELATION_ID_HEADER,
        request.requestedBy().correlationId() == null
            ? null
            : request.requestedBy().correlationId().value());
  }

  private static void putOptionalTrustedParameter(
      ObjectNode parameters, List<Parameter> declarations, String name, String value) {
    if (value == null) return;
    boolean declared =
        declarations.stream()
            .anyMatch(
                parameter ->
                    "header".equals(parameter.location())
                        && name.equalsIgnoreCase(parameter.name()));
    if (declared) {
      putTrustedParameter(parameters, declarations, name, value);
    }
  }

  private static void putTrustedParameter(
      ObjectNode parameters, List<Parameter> declarations, String name, String value) {
    boolean declared =
        declarations.stream()
            .anyMatch(
                parameter ->
                    "header".equals(parameter.location())
                        && name.equalsIgnoreCase(parameter.name()));
    if (!declared) {
      throw new SecurityException(
          "Trusted identity OpenAPI operation does not declare header " + name);
    }
    if (parameters.has(name) || parameters.has("header." + name)) {
      throw new SecurityException(
          "Workflow-authored parameters cannot override trusted header " + name);
    }
    parameters.put(name, value);
  }

  private OperationObservation validateAndProject(
      PreparedOpenApi prepared, OperationObservation observation) {
    if (observation == null
        || observation.status() != OperationObservationStatus.SUCCEEDED
        || "raw".equals(prepared.requestedOutput())) {
      return observation;
    }
    try {
      JsonNode response = dataReferences.resolve(prepared.originalRequest(), observation.output());
      int status = response.required("statusCode").intValue();
      LocatedNode declared = responseDeclaration(prepared.graph(), prepared.operation(), status);
      JsonNode content = response.required("content");
      LocatedNode schema =
          responseSchema(prepared.graph(), prepared.document(), declared, response.path("headers"));
      if (schema != null) {
        validateResponseSchema(prepared, schema, content, status);
      }
      JsonNode projected =
          switch (prepared.requestedOutput()) {
            case "content" -> content;
            case "response" -> response;
            default ->
                throw new IllegalArgumentException(
                    "Unsupported OpenAPI output " + prepared.requestedOutput());
          };
      return new OperationObservation(
          OperationObservationStatus.SUCCEEDED,
          dataReferences.capture(prepared.originalRequest(), projected),
          null,
          observation.metadata());
    } catch (RuntimeException invalid) {
      return new OperationObservation(
          OperationObservationStatus.FAILED,
          null,
          new WorkflowError(
              RESPONSE_VALIDATION_ERROR,
              502,
              prepared.originalRequest().operationId(),
              "OpenAPI response contract validation failed",
              rootMessage(invalid)),
          observation.metadata());
    }
  }

  private static LocatedNode responseDeclaration(
      OperationDocumentGraph graph, Operation operation, int status) {
    JsonNode responses = operation.definition().node().path("responses");
    if (!responses.isObject()) {
      throw new IllegalArgumentException("OpenAPI operation defines no responses");
    }
    JsonNode declaration = responses.get(Integer.toString(status));
    if (declaration == null) {
      String range = (status / 100) + "XX";
      declaration = responses.get(range);
      if (declaration == null) {
        declaration = responses.get(range.toLowerCase(Locale.ROOT));
      }
    }
    if (declaration == null) {
      declaration = responses.get("default");
    }
    if (declaration == null) {
      throw new IllegalArgumentException(
          "OpenAPI operation has no response contract for HTTP " + status);
    }
    return graph.dereference(graph.located(operation.definition(), declaration));
  }

  private static LocatedNode responseSchema(
      OperationDocumentGraph graph, LocatedNode document, LocatedNode response, JsonNode headers) {
    boolean swagger2 = "2.0".equals(document.node().path("swagger").asText());
    if (swagger2) {
      JsonNode schema = response.node().get("schema");
      return schema == null ? null : graph.dereference(graph.located(response, schema));
    }
    JsonNode content = response.node().path("content");
    if (!content.isObject() || content.isEmpty()) {
      return null;
    }
    String actual = header(headers, "content-type");
    String mediaType =
        actual == null ? "" : actual.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
    String selected = selectResponseMediaType(content, mediaType);
    JsonNode media = content.get(selected);
    JsonNode schema = media == null ? null : media.get("schema");
    return schema == null ? null : graph.dereference(graph.located(response, schema));
  }

  private static String selectResponseMediaType(JsonNode content, String actual) {
    if (!actual.isBlank() && content.has(actual)) {
      return actual;
    }
    if (!actual.isBlank()) {
      int slash = actual.indexOf('/');
      if (slash > 0) {
        String wildcard = actual.substring(0, slash) + "/*";
        if (content.has(wildcard)) return wildcard;
      }
    }
    if (content.has("*/*")) return "*/*";
    if (content.size() == 1 && actual.isBlank()) {
      return content.fieldNames().next();
    }
    throw new IllegalArgumentException(
        "OpenAPI response content does not declare media type "
            + (actual.isBlank() ? "<missing>" : actual));
  }

  private static void validateResponseSchema(
      PreparedOpenApi prepared, LocatedNode schema, JsonNode value, int status) {
    ResolvedDataSchema resolved =
        resolvedOpenApiSchema(
            prepared.document().node(),
            schema,
            "/paths/"
                + prepared.operation().path()
                + "/"
                + prepared.operation().method()
                + "/responses/"
                + status);
    prepared.schemas().validate(resolved, value);
  }

  /**
   * Converts the OpenAPI 3.0 {@code nullable} extension into the equivalent JSON Schema draft-4
   * type/enum representation before validation.
   *
   * <p>OpenAPI 3.0 Schema Objects are based on, but are not themselves, JSON Schema. In particular,
   * a generic JSON Schema validator is required to ignore the unknown {@code nullable} keyword.
   * Passing an OpenAPI 3.0 schema through unchanged therefore rejects a legitimate JSON null when
   * the declared non-null type is, for example, {@code string}.
   */
  private static void normalizeOpenApi30Nullable(JsonNode node) {
    if (node == null) return;
    if (node.isArray()) {
      node.forEach(OpenApiCallAdapter::normalizeOpenApi30Nullable);
      return;
    }
    if (!node.isObject()) return;

    ObjectNode object = (ObjectNode) node;
    List<JsonNode> children = new ArrayList<>();
    object.elements().forEachRemaining(children::add);
    children.forEach(OpenApiCallAdapter::normalizeOpenApi30Nullable);

    if (!object.path("nullable").asBoolean(false)) return;
    object.remove("nullable");

    JsonNode type = object.get("type");
    if (type != null && type.isTextual()) {
      ArrayNode types = JsonNodeFactory.instance.arrayNode();
      types.add(type.textValue());
      types.add("null");
      object.set("type", types);
    } else if (type != null && type.isArray()) {
      boolean alreadyNullable = false;
      for (JsonNode candidate : type) {
        if (candidate.isTextual() && "null".equals(candidate.textValue())) {
          alreadyNullable = true;
          break;
        }
      }
      if (!alreadyNullable) {
        ((ArrayNode) type).add("null");
      }
    } else {
      ObjectNode nonNull = object.deepCopy();
      JsonNode id = nonNull.remove("$id");
      object.removeAll();
      if (id != null) object.set("$id", id);
      ArrayNode alternatives = object.putArray("anyOf");
      alternatives.add(nonNull);
      alternatives.addObject().put("type", "null");
    }

    JsonNode allowed = object.get("enum");
    if (allowed != null && allowed.isArray()) {
      boolean containsNull = false;
      for (JsonNode candidate : allowed) {
        if (candidate.isNull()) {
          containsNull = true;
          break;
        }
      }
      if (!containsNull) ((ArrayNode) allowed).addNull();
    }
  }

  private static LocatedNode parameterSchema(
      OperationDocumentGraph graph, Parameter parameter, boolean swagger2) {
    JsonNode declaration = parameter.declaration().node();
    if (declaration.has("content")) {
      JsonNode content = declaration.required("content");
      if (!content.isObject() || content.size() != 1) {
        throw new IllegalArgumentException(
            "OpenAPI parameter content must define exactly " + "one media type");
      }
      JsonNode media = content.elements().next();
      JsonNode schema = media.get("schema");
      return schema == null
          ? null
          : graph.dereference(graph.located(parameter.declaration(), schema));
    }
    JsonNode schema = declaration.get("schema");
    if (schema != null) {
      return graph.dereference(graph.located(parameter.declaration(), schema));
    }
    /*
     * Swagger 2 non-body parameters place JSON Schema keywords directly
     * on the Parameter Object.
     */
    if (!swagger2) return null;
    ObjectNode projected = JsonNodeFactory.instance.objectNode();
    for (String keyword :
        List.of(
            "type",
            "format",
            "items",
            "enum",
            "default",
            "maximum",
            "exclusiveMaximum",
            "minimum",
            "exclusiveMinimum",
            "maxLength",
            "minLength",
            "pattern",
            "maxItems",
            "minItems",
            "uniqueItems",
            "multipleOf")) {
      if (declaration.has(keyword)) {
        projected.set(keyword, declaration.get(keyword));
      }
    }
    if ("file".equals(projected.path("type").asText())) {
      projected.put("type", "string");
    }
    return projected.isEmpty() ? null : graph.located(parameter.declaration(), projected);
  }

  private static void validateRequestSchema(
      DataSchemaValidator schemas,
      LocatedNode openApiDocument,
      LocatedNode schema,
      JsonNode value,
      String definitionPath) {
    ResolvedDataSchema resolved =
        resolvedOpenApiSchema(openApiDocument.node(), schema, definitionPath);
    schemas.validate(resolved, value);
  }

  private static ResolvedDataSchema resolvedOpenApiSchema(
      JsonNode openApiDocument, LocatedNode schema, String definitionPath) {
    JsonNode document = schema.node().deepCopy();
    boolean openApi30 = openApiDocument.path("openapi").asText().startsWith("3.0");
    boolean draft4 = "2.0".equals(openApiDocument.path("swagger").asText()) || openApi30;
    if (openApi30) {
      normalizeOpenApi30Nullable(document);
    }
    if (document.isObject()) {
      /*
       * Draft 4 uses "id", while 2020-12 uses "$id", to establish
       * the base URI for relative references. Keep the extracted schema
       * anchored to the immutable source document used by the normalized
       * resource registry.
       */
      ((ObjectNode) document).put(draft4 ? "id" : "$id", schema.documentUri().toString());
    }
    String format = schemaFormat(openApiDocument);
    String canonical = document.toString();
    return new ResolvedDataSchema(
        definitionPath, format, schema.locationUri(), hexSha256(canonical), document);
  }

  private static DataSchemaValidator openApiValidator(OperationRequest request) {
    List<ResolvedWorkflowResource> normalized = new ArrayList<>();
    for (ResolvedWorkflowResource resource : request.resources()) {
      if (!structuredDocument(resource.mediaType())) {
        normalized.add(resource);
        continue;
      }
      JsonNode document = new OperationDocumentGraph(resource, request.resources()).root().node();
      if (!document.path("openapi").asText().startsWith("3.0")) {
        normalized.add(resource);
        continue;
      }
      JsonNode copy = document.deepCopy();
      normalizeOpenApi30Nullable(copy);
      normalized.add(
          ResolvedWorkflowResource.of(resource.uri(), "application/json", copy.toString()));
    }
    return new DataSchemaValidator(normalized);
  }

  private static boolean structuredDocument(String mediaType) {
    String normalized = mediaType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim();
    return normalized.equals("application/json")
        || normalized.endsWith("+json")
        || normalized.equals("application/yaml")
        || normalized.equals("application/x-yaml")
        || normalized.equals("text/yaml")
        || normalized.equals("text/x-yaml");
  }

  private static String schemaFormat(JsonNode openApiDocument) {
    return "2.0".equals(openApiDocument.path("swagger").asText())
            || openApiDocument.path("openapi").asText().startsWith("3.0")
        ? "json:4"
        : "json:2020-12";
  }

  private static OperationObservation requestValidationFailure(
      OperationRequest request, DataSchemaValidationException invalid) {
    return new OperationObservation(
        OperationObservationStatus.FAILED,
        null,
        new WorkflowError(
            REQUEST_VALIDATION_ERROR,
            400,
            request.operationId(),
            "OpenAPI request contract validation failed",
            rootMessage(invalid)),
        null);
  }

  private static String header(JsonNode headers, String name) {
    if (!headers.isObject()) return null;
    Iterator<Map.Entry<String, JsonNode>> values = headers.properties().iterator();
    while (values.hasNext()) {
      Map.Entry<String, JsonNode> value = values.next();
      if (name.equalsIgnoreCase(value.getKey())) {
        return value.getValue().asText();
      }
    }
    return null;
  }

  private static String hexSha256(String value) {
    return java.util.HexFormat.of().formatHex(sha256(value));
  }

  private static String rootMessage(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current.getMessage() == null ? current.getClass().getName() : current.getMessage();
  }

  private static void requireOpenApiDocument(JsonNode document) {
    if (document == null || !document.isObject()) {
      throw new IllegalArgumentException("OpenAPI resource must contain an object");
    }
    if (!document.has("openapi") && !"2.0".equals(document.path("swagger").asText())) {
      throw new IllegalArgumentException("Document is neither OpenAPI 3.x nor Swagger 2.0");
    }
  }

  private static Operation findOperation(
      OperationDocumentGraph graph, LocatedNode document, String operationId) {
    JsonNode paths = document.node().required("paths");
    for (Iterator<Map.Entry<String, JsonNode>> entries = paths.properties().iterator();
        entries.hasNext(); ) {
      Map.Entry<String, JsonNode> path = entries.next();
      LocatedNode pathItem = graph.dereference(graph.located(document, path.getValue()));
      for (String method : METHODS) {
        JsonNode value = pathItem.node().get(method);
        if (value == null) {
          continue;
        }
        LocatedNode candidate = graph.dereference(graph.located(pathItem, value));
        if (operationId.equals(candidate.node().path("operationId").asText())) {
          return new Operation(method.toUpperCase(Locale.ROOT), path.getKey(), pathItem, candidate);
        }
      }
    }
    throw new IllegalArgumentException("OpenAPI operationId was not found: " + operationId);
  }

  private static List<Parameter> parameterDeclarations(
      OperationDocumentGraph graph, Operation operation) {
    Map<String, Parameter> result = new LinkedHashMap<>();
    addAll(graph, result, operation.pathItem(), operation.pathItem().node().get("parameters"));
    addAll(graph, result, operation.definition(), operation.definition().node().get("parameters"));
    return List.copyOf(result.values());
  }

  private static void addAll(
      OperationDocumentGraph graph,
      Map<String, Parameter> target,
      LocatedNode owner,
      JsonNode values) {
    if (values != null && values.isArray()) {
      values.forEach(
          value -> {
            LocatedNode resolved = graph.dereference(graph.located(owner, value));
            String name = requiredText(resolved.node(), "name");
            String location = requiredText(resolved.node(), "in");
            target.put(location + "\u0000" + name, new Parameter(name, location, resolved));
          });
    }
  }

  private static JsonNode takeParameterValue(
      ObjectNode values, Parameter parameter, List<Parameter> declarations) {
    String qualified = parameter.location() + "." + parameter.name();
    JsonNode value = values.remove(qualified);
    if (value != null) {
      return value;
    }
    if (!values.has(parameter.name())) {
      return null;
    }
    long sameName =
        declarations.stream()
            .filter(candidate -> candidate.name().equals(parameter.name()))
            .count();
    if (sameName > 1) {
      throw new IllegalArgumentException(
          "OpenAPI parameter "
              + parameter.name()
              + " is ambiguous; qualify it as "
              + parameter.location()
              + "."
              + parameter.name());
    }
    return values.remove(parameter.name());
  }

  private static JsonNode defaultValue(JsonNode parameter) {
    JsonNode schema = parameter.get("schema");
    if (schema != null && schema.has("default")) {
      return schema.get("default");
    }
    return parameter.get("default");
  }

  private static String server(URI documentUri, JsonNode document, Operation operation) {
    JsonNode servers = operation.definition().node().get("servers");
    if (servers == null || servers.isEmpty()) {
      servers = operation.pathItem().node().get("servers");
    }
    if (servers == null || servers.isEmpty()) {
      servers = document.get("servers");
    }
    if (servers != null && !servers.isEmpty()) {
      return resolveServer(documentUri, servers.get(0));
    }
    if ("2.0".equals(document.path("swagger").asText())) {
      String host = requiredText(document, "host");
      String scheme =
          document.path("schemes").isArray() && !document.path("schemes").isEmpty()
              ? document.path("schemes").get(0).textValue()
              : "https";
      return scheme + "://" + host + document.path("basePath").asText("");
    }
    throw new IllegalArgumentException("OpenAPI operation has no server");
  }

  private static String resolveServer(URI documentUri, JsonNode server) {
    String url = requiredText(server, "url");
    JsonNode variables = server.get("variables");
    if (variables != null) {
      for (Iterator<Map.Entry<String, JsonNode>> entries = variables.properties().iterator();
          entries.hasNext(); ) {
        Map.Entry<String, JsonNode> variable = entries.next();
        String value = requiredText(variable.getValue(), "default");
        url = url.replace("{" + variable.getKey() + "}", value);
      }
    }
    if (url.contains("{")) {
      throw new IllegalArgumentException("OpenAPI server contains an unresolved variable: " + url);
    }
    final URI serverUri;
    try {
      serverUri = URI.create(url);
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("OpenAPI server URL is invalid: " + url, invalid);
    }
    URI resolved = serverUri.isAbsolute() ? serverUri : documentUri.resolve(serverUri);
    if (!resolved.isAbsolute() || resolved.getScheme() == null || resolved.getHost() == null) {
      throw new IllegalArgumentException(
          "OpenAPI server URL does not resolve to an absolute " + "network endpoint: " + url);
    }
    return resolved.toString();
  }

  private static String join(String base, String path) {
    if (base.endsWith("/") && path.startsWith("/")) {
      return base.substring(0, base.length() - 1) + path;
    }
    if (!base.endsWith("/") && !path.startsWith("/")) {
      return base + "/" + path;
    }
    return base + path;
  }

  private static String replacePath(String path, String name, String value, boolean encoded) {
    String token = "{" + name + "}";
    if (!path.contains(token)) {
      throw new IllegalArgumentException("OpenAPI path parameter " + name + " has no template");
    }
    return path.replace(token, encoded ? value : percentEncode(value));
  }

  private static String cookieHeader(List<OpenApiParameterSerializer.QueryValue> cookies) {
    List<String> values = new ArrayList<>();
    cookies.forEach(entry -> values.add(entry.name() + "=" + entry.value()));
    return String.join("; ", values);
  }

  private static String appendQuery(
      String endpoint, List<OpenApiParameterSerializer.QueryValue> values) {
    if (values.isEmpty()) {
      return endpoint;
    }
    StringBuilder result = new StringBuilder(endpoint);
    result.append(endpoint.contains("?") ? '&' : '?');
    for (int index = 0; index < values.size(); index++) {
      if (index > 0) {
        result.append('&');
      }
      OpenApiParameterSerializer.QueryValue value = values.get(index);
      result.append(value.name()).append('=').append(value.value());
    }
    return result.toString();
  }

  private static boolean allowsBody(String method) {
    return !"GET".equals(method) && !"HEAD".equals(method) && !"TRACE".equals(method);
  }

  private static String percentEncode(String value) {
    StringBuilder result = new StringBuilder();
    for (byte octet : value.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
      int unsigned = octet & 0xff;
      if (unsigned >= 'a' && unsigned <= 'z'
          || unsigned >= 'A' && unsigned <= 'Z'
          || unsigned >= '0' && unsigned <= '9'
          || unsigned == '-'
          || unsigned == '.'
          || unsigned == '_'
          || unsigned == '~') {
        result.append((char) unsigned);
      } else {
        result
            .append('%')
            .append(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)))
            .append(Character.toUpperCase(Character.forDigit(unsigned & 0xf, 16)));
      }
    }
    return result.toString();
  }

  private static ObjectNode requireObject(JsonNode value, String description) {
    if (!value.isObject()) {
      throw new IllegalArgumentException(description + " must be an object");
    }
    return (ObjectNode) value;
  }

  private static String requiredText(JsonNode owner, String field) {
    JsonNode value = owner.get(field);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalArgumentException("OpenAPI " + field + " must be text");
    }
    return value.textValue();
  }

  private static BodyPayload requestBody(
      OperationDocumentGraph graph, Operation operation, ObjectNode parameters) {
    JsonNode declaration = operation.definition().node().get("requestBody");
    if (declaration == null) {
      return new BodyPayload(null, null, null);
    }
    LocatedNode requestBody = graph.dereference(graph.located(operation.definition(), declaration));
    JsonNode content = requestBody.node().path("content");
    if (!content.isObject() || content.isEmpty()) {
      throw new IllegalArgumentException("OpenAPI requestBody defines no media types");
    }
    String mediaType = selectMediaType(content);
    LocatedNode media = graph.located(requestBody, content.required(mediaType));
    JsonNode schemaDeclaration = media.node().get("schema");
    LocatedNode schema =
        schemaDeclaration == null
            ? null
            : graph.dereference(graph.located(media, schemaDeclaration));
    JsonNode value = parameters.remove("requestBody");
    if (value == null) {
      value = parameters.remove("body");
    }
    if (value == null && schema != null && schema.node().path("properties").isObject()) {
      ObjectNode object = JsonNodeFactory.instance.objectNode();
      schema
          .node()
          .path("properties")
          .fieldNames()
          .forEachRemaining(
              name -> {
                JsonNode property = parameters.remove(name);
                if (property != null) {
                  object.set(name, property);
                }
              });
      if (!object.isEmpty()) {
        value = object;
      }
    }
    if (value == null && requestBody.node().path("required").asBoolean(false)) {
      throw new IllegalArgumentException("Missing required OpenAPI request body");
    }
    return new BodyPayload(value, mediaType, schema);
  }

  private static String selectMediaType(JsonNode content) {
    if (content.has("application/json")) {
      return "application/json";
    }
    Iterator<String> names = content.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (name.toLowerCase(Locale.ROOT).endsWith("+json")) {
        return name;
      }
    }
    return content.fieldNames().next();
  }

  private static JsonNode encodeBody(JsonNode body, String mediaType) {
    if (body == null
        || mediaType == null
        || "application/json".equalsIgnoreCase(mediaType)
        || mediaType.toLowerCase(Locale.ROOT).endsWith("+json")) {
      return body;
    }
    if ("application/x-www-form-urlencoded".equalsIgnoreCase(mediaType)) {
      if (body.isTextual()) {
        // Swagger 2 formData has already been serialized as a
        // repeated-field form payload.
        return body;
      }
      if (!body.isObject()) {
        throw new IllegalArgumentException("Form request body must be an object");
      }
      List<String> fields = new ArrayList<>();
      body.properties()
          .iterator()
          .forEachRemaining(
              entry -> {
                if (!entry.getValue().isValueNode()) {
                  throw new IllegalArgumentException(
                      "Form field " + entry.getKey() + " must be scalar");
                }
                fields.add(
                    percentEncode(entry.getKey()) + "=" + percentEncode(entry.getValue().asText()));
              });
      return JsonNodeFactory.instance.textNode(String.join("&", fields));
    }
    if (body.isTextual()) {
      return body;
    }
    throw new IllegalArgumentException(
        "OpenAPI request body media type " + mediaType + " requires a textual value");
  }

  private static String consumes(JsonNode document, Operation operation) {
    JsonNode consumes = operation.definition().node().path("consumes");
    if (!consumes.isArray() || consumes.isEmpty()) {
      consumes = document.path("consumes");
    }
    return consumes.isArray() && !consumes.isEmpty()
        ? consumes.get(0).asText()
        : "application/json";
  }

  private static BodyPayload swaggerRequestBody(
      JsonNode body,
      List<OpenApiParameterSerializer.QueryValue> formFields,
      String mediaType,
      String operationId) {
    if (body != null && !formFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Swagger 2 operation cannot combine body and formData " + "parameters");
    }
    if (formFields.isEmpty()) {
      return new BodyPayload(body, mediaType, null);
    }
    if ("application/x-www-form-urlencoded".equalsIgnoreCase(mediaType)) {
      List<String> fields = new ArrayList<>();
      formFields.forEach(
          field -> fields.add(percentEncode(field.name()) + "=" + percentEncode(field.value())));
      return new BodyPayload(
          JsonNodeFactory.instance.textNode(String.join("&", fields)),
          "application/x-www-form-urlencoded",
          null);
    }
    if ("multipart/form-data".equalsIgnoreCase(mediaType)) {
      String boundary =
          "oks-" + java.util.HexFormat.of().formatHex(sha256(operationId)).substring(0, 32);
      StringBuilder encoded = new StringBuilder();
      formFields.forEach(
          field ->
              encoded
                  .append("--")
                  .append(boundary)
                  .append("\r\n")
                  .append("Content-Disposition: form-data; name=\"")
                  .append(quoted(field.name()))
                  .append("\"\r\n\r\n")
                  .append(field.value())
                  .append("\r\n"));
      encoded.append("--").append(boundary).append("--\r\n");
      return new BodyPayload(
          JsonNodeFactory.instance.textNode(encoded.toString()),
          "multipart/form-data; boundary=" + boundary,
          null);
    }
    throw new IllegalArgumentException(
        "Swagger 2 formData requires consumes "
            + "application/x-www-form-urlencoded or "
            + "multipart/form-data, not "
            + mediaType);
  }

  private static byte[] sha256(String value) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256")
          .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static String quoted(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static Iterable<String> iterable(Iterator<String> iterator) {
    return () -> iterator;
  }

  private record Operation(
      String method, String path, LocatedNode pathItem, LocatedNode definition) {}

  private record Parameter(String name, String location, LocatedNode declaration) {}

  private record BodyPayload(JsonNode value, String mediaType, LocatedNode schema) {}

  private record PreparedOpenApi(
      OperationRequest httpRequest,
      OperationRequest originalRequest,
      OperationDocumentGraph graph,
      LocatedNode document,
      Operation operation,
      DataSchemaValidator schemas,
      String requestedOutput) {}
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
