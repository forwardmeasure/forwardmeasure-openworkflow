/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedSecret;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Resolves mounted tenant credentials only after the host's AuthZEN decision has permitted I/O. */
public final class MountedOperationCredentialResolver implements OperationSecurityResolver {
  private static final String TENANT_PREFIX = "did:forwardmeasure:tenant:";

  private final TenantSecretProvider secrets;
  private final OperationDataReferenceFactory dataReferences;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();

  public MountedOperationCredentialResolver(
      TenantSecretProvider secrets, OperationDataReferenceFactory dataReferences) {
    this.secrets = Objects.requireNonNull(secrets, "secrets");
    this.dataReferences = Objects.requireNonNull(dataReferences, "dataReferences");
  }

  @Override
  public CompletionStage<SecuredOperationRequest> secure(OperationRequest request) {
    Objects.requireNonNull(request, "request");
    ResolvedAuthentication authentication = null;
    Map<String, ResolvedSecret> supplemental = new LinkedHashMap<>();
    try {
      TenantId tenant = tenant(request);
      JsonNode configured = request.descriptor().get("authentication");
      if (configured != null) {
        authentication = resolveAuthentication(request, tenant, configured);
      }
      for (String name : supplementalNames(request.descriptor())) {
        char[] value = secrets.resolve(tenant, name);
        try {
          supplemental.put(name, new ResolvedSecret(name, Map.of("value", value)));
        } finally {
          Arrays.fill(value, '\0');
        }
      }
      OperationRequest resolved = request;
      if (authentication != null) resolved = resolved.withAuthentication(authentication);
      if (!supplemental.isEmpty()) resolved = resolved.withSecrets(supplemental);
      return CompletableFuture.completedFuture(new SecuredOperationRequest(resolved));
    } catch (RuntimeException failure) {
      if (authentication != null) authentication.close();
      supplemental.values().forEach(ResolvedSecret::close);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private ResolvedAuthentication resolveAuthentication(
      OperationRequest request, TenantId tenant, JsonNode configured) {
    ResolvedAuthentication.Kind kind =
        ResolvedAuthentication.Kind.valueOf(configured.required("kind").textValue());
    Map<String, char[]> values = new LinkedHashMap<>();
    try {
      if (configured.has("secretName")) {
        char[] secret = secrets.resolve(tenant, configured.required("secretName").textValue());
        try {
          populateSecret(kind, secret, values);
        } finally {
          Arrays.fill(secret, '\0');
        }
      } else {
        populateExpression(request, tenant, kind, configured, values);
      }
      return new ResolvedAuthentication(kind, values);
    } finally {
      wipe(values);
    }
  }

  private void populateExpression(
      OperationRequest request,
      TenantId tenant,
      ResolvedAuthentication.Kind kind,
      JsonNode configured,
      Map<String, char[]> values) {
    var secretValues = JsonNodeFactory.instance.objectNode();
    var sensitive = new java.util.ArrayList<char[]>();
    try {
      for (String reference : names(configured.path("secretReferences"))) {
        char[] value = secrets.resolve(tenant, reference);
        sensitive.add(value);
        secretValues.putObject(reference).put("value", new String(value));
      }
      JsonNode input = dataReferences.resolveDescriptorValue(request, "taskInput");
      JsonNode context = dataReferences.resolveDescriptorValue(request, "workflowContext");
      JsonNode evaluated =
          expressions.evaluateTemplate(
              configured.required("expressions"),
              input,
              new RuntimeExpressionArguments(
                  context, input, null, secretValues, null, request.descriptor(), null, null),
              ExpressionMode.parse(request.descriptor().path("expressionMode").asText("strict")));
      switch (kind) {
        case BASIC, DIGEST -> {
          copyText(evaluated, values, "username");
          copyText(evaluated, values, "password");
        }
        case BEARER -> copyText(evaluated, values, "token");
        case OAUTH2, OIDC -> {
          String field = evaluated.has("access_token") ? "access_token" : "token";
          copyText(evaluated, values, field);
        }
      }
    } finally {
      sensitive.forEach(value -> Arrays.fill(value, '\0'));
      secretValues.removeAll();
    }
  }

  private static void populateSecret(
      ResolvedAuthentication.Kind kind, char[] secret, Map<String, char[]> values) {
    String clear = new String(secret);
    switch (kind) {
      case BASIC, DIGEST -> {
        int separator = clear.indexOf(':');
        if (separator < 1 || separator == clear.length() - 1) {
          throw new IllegalArgumentException(kind + " secret must contain username:password");
        }
        values.put("username", clear.substring(0, separator).toCharArray());
        values.put("password", clear.substring(separator + 1).toCharArray());
      }
      case BEARER -> values.put("token", clear.toCharArray());
      case OAUTH2, OIDC -> values.put("access_token", clear.toCharArray());
    }
  }

  private static TenantId tenant(OperationRequest request) {
    String did = request.requestedBy().tenantId().toString();
    if (!did.startsWith(TENANT_PREFIX)) {
      throw new SecurityException("Operation tenant is not a ForwardMeasure tenant DID");
    }
    return new TenantId(UUID.fromString(did.substring(TENANT_PREFIX.length())));
  }

  private static Set<String> supplementalNames(JsonNode descriptor) {
    return names(descriptor.path("secretReferences"));
  }

  private static Set<String> names(JsonNode references) {
    if (references.isMissingNode()) return Set.of();
    if (!references.isArray()) {
      throw new IllegalArgumentException("Operation secret references must be an array");
    }
    Set<String> result = new LinkedHashSet<>();
    references.forEach(
        reference -> {
          if (!reference.isTextual() || reference.textValue().isBlank()) {
            throw new IllegalArgumentException("Operation secret reference must be text");
          }
          result.add(reference.textValue());
        });
    return Set.copyOf(result);
  }

  private static void copyText(JsonNode source, Map<String, char[]> values, String field) {
    JsonNode value = source.get(field);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalArgumentException(
          "Authentication expression must produce non-blank '" + field + "'");
    }
    values.put(field, value.textValue().toCharArray());
  }

  private static void wipe(Map<String, char[]> values) {
    values.values().forEach(value -> Arrays.fill(value, '\0'));
    values.clear();
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
