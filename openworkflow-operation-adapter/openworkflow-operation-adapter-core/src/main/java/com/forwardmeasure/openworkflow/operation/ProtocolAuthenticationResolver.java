package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/** Resolves protocol credentials only at the tenant-authorised adapter edge. */
public final class ProtocolAuthenticationResolver {
  private final SecretProvider secrets;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();

  public ProtocolAuthenticationResolver(SecretProvider secrets) {
    this.secrets = Objects.requireNonNull(secrets, "secrets");
  }

  public Credential resolve(
      ExecutionId executionId,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext context) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(authentication, "authentication");
    if (!authentication.secretBacked()) {
      JsonNode configuration = materializeConfiguration(executionId, authentication, context);
      return switch (authentication.kind()) {
        case BASIC ->
            basic(required(configuration, "username"), required(configuration, "password"));
        case BEARER, OAUTH2, OIDC ->
            bearer(authentication.kind(), required(configuration, "token"));
        case DIGEST ->
            new Credential(
                authentication.kind(),
                null,
                required(configuration, "username"),
                required(configuration, "password"),
                configuration);
      };
    }
    char[] secret = secrets.resolve(executionId.tenantId(), authentication.secretName());
    try {
      String value = new String(secret);
      if (authentication.kind() == AuthenticationPlan.Kind.BASIC
          || authentication.kind() == AuthenticationPlan.Kind.DIGEST) {
        int separator = value.indexOf(':');
        if (separator < 1 || separator == value.length() - 1) {
          throw new IllegalArgumentException("Basic/Digest secret must contain username:password");
        }
        if (authentication.kind() == AuthenticationPlan.Kind.BASIC) {
          return basic(value.substring(0, separator), value.substring(separator + 1));
        }
        return new Credential(
            authentication.kind(),
            null,
            value.substring(0, separator),
            value.substring(separator + 1),
            null);
      }
      return bearer(authentication.kind(), value);
    } finally {
      Arrays.fill(secret, '\0');
    }
  }

  public JsonNode materializeConfiguration(
      ExecutionId executionId,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext context) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(authentication, "authentication");
    if (authentication.secretBacked())
      throw new IllegalArgumentException(
          "Secret-backed authentication has no expression configuration");
    if (context == null)
      throw new IllegalArgumentException("Expression-backed authentication context is absent");
    ObjectNode resolvedSecrets = JsonNodeFactory.instance.objectNode();
    var sensitive = new java.util.ArrayList<char[]>();
    try {
      for (String reference : authentication.secretReferences()) {
        char[] value = secrets.resolve(executionId.tenantId(), reference);
        sensitive.add(value);
        resolvedSecrets.put(reference, new String(value));
      }
      RuntimeExpressionArguments arguments =
          new RuntimeExpressionArguments(
              context.context(),
              context.input(),
              context.output(),
              resolvedSecrets,
              context.authorization(),
              context.task(),
              context.workflow(),
              context.runtime(),
              context.variables());
      return expressions.evaluateTemplate(
          authentication.expressionConfiguration(),
          context.input(),
          arguments,
          ExpressionMode.STRICT);
    } finally {
      sensitive.forEach(value -> Arrays.fill(value, '\0'));
      resolvedSecrets.removeAll();
    }
  }

  private static Credential basic(String username, String password) {
    String authorization =
        "Basic "
            + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    return new Credential(AuthenticationPlan.Kind.BASIC, authorization, username, password, null);
  }

  private static Credential bearer(AuthenticationPlan.Kind kind, String token) {
    return new Credential(kind, "Bearer " + token, null, null, null);
  }

  private static String required(JsonNode configuration, String name) {
    JsonNode value = configuration.get(name);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalArgumentException(
          "Authentication field '" + name + "' must evaluate to non-blank text");
    }
    return value.textValue();
  }

  public record Credential(
      AuthenticationPlan.Kind kind,
      String authorization,
      String username,
      String password,
      JsonNode configuration) {
    public Credential {
      Objects.requireNonNull(kind, "kind");
      configuration = configuration == null ? null : configuration.deepCopy();
    }

    @Override
    public JsonNode configuration() {
      return configuration == null ? null : configuration.deepCopy();
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
