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
package com.forwardmeasure.openworkflow.definition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Immutable publication-time location of one catalogued function. */
public record CatalogFunctionReference(
    String name, String version, String catalog, URI uri, JsonNode endpoint) {
  private static final Pattern QUALIFIED = Pattern.compile("^([^:@/]+):([^@/]+)@([^@/]+)$");

  public CatalogFunctionReference {
    requireText(name, "name");
    requireText(version, "version");
    if (catalog != null && catalog.isBlank()) {
      throw new IllegalArgumentException("catalog must be null or non-blank");
    }
    Objects.requireNonNull(uri, "uri");
    if (!uri.isAbsolute() || uri.getFragment() != null) {
      throw new IllegalArgumentException("Catalog function URI must be absolute and fragment-free");
    }
    uri = uri.normalize();
    Objects.requireNonNull(endpoint, "endpoint");
    endpoint = endpoint.deepCopy();
  }

  /**
   * Resolves a standard qualified name or a direct immutable function URI. Inline functions
   * intentionally return empty.
   */
  public static Optional<CatalogFunctionReference> resolve(String call, JsonNode catalogs) {
    Objects.requireNonNull(call, "call");
    Objects.requireNonNull(catalogs, "catalogs");
    Matcher matcher = QUALIFIED.matcher(call);
    if (matcher.matches()) {
      String name = matcher.group(1);
      String version = matcher.group(2);
      String catalog = matcher.group(3);
      JsonNode declaration = catalogs.get(catalog);
      if (declaration == null) {
        throw new WorkflowDefinitionException(
            java.util.List.of("Catalog '" + catalog + "' is not declared in use.catalogs"));
      }
      JsonNode configuredEndpoint = declaration.required("endpoint");
      URI root = WorkflowResourceResolver.literalEndpoint(configuredEndpoint);
      URI uri = functionUri(root, name, version);
      return Optional.of(
          new CatalogFunctionReference(
              name, version, catalog, uri, endpoint(uri, configuredEndpoint)));
    }
    try {
      URI direct = URI.create(call);
      if (direct.isAbsolute()
          && ("https".equalsIgnoreCase(direct.getScheme())
              || "http".equalsIgnoreCase(direct.getScheme()))) {
        URI normalized = withoutFragment(direct);
        return Optional.of(
            new CatalogFunctionReference(
                normalized.toString(), "uri", null, normalized, endpoint(normalized, null)));
      }
    } catch (IllegalArgumentException ignored) {
      // A qualified or inline function name is not itself a URI.
    }
    return Optional.empty();
  }

  @Override
  public JsonNode endpoint() {
    return endpoint.deepCopy();
  }

  static URI functionUri(URI root, String name, String version) {
    String host = root.getHost();
    String[] path = root.getPath().replaceFirst("^/", "").split("/");
    if ("github.com".equalsIgnoreCase(host) && path.length >= 2) {
      String reference = path.length >= 4 && "tree".equals(path[2]) ? path[3] : "main";
      return URI.create(
          "https://raw.githubusercontent.com/"
              + path[0]
              + "/"
              + path[1]
              + "/refs/heads/"
              + reference
              + "/functions/"
              + name
              + "/"
              + version
              + "/function.yaml");
    }
    String value = root.toString();
    if (!value.endsWith("/")) value += "/";
    return URI.create(value)
        .resolve("functions/" + name + "/" + version + "/function.yaml")
        .normalize();
  }

  private static JsonNode endpoint(URI uri, JsonNode configured) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.put("uri", uri.toString());
    if (configured != null && configured.isObject() && configured.has("authentication")) {
      result.set("authentication", configured.required("authentication").deepCopy());
    }
    return result;
  }

  private static URI withoutFragment(URI uri) {
    if (uri.getFragment() == null) return uri.normalize();
    return URI.create(uri.toString().substring(0, uri.toString().indexOf('#'))).normalize();
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
