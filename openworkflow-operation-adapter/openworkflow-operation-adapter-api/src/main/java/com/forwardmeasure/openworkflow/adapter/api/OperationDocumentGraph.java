package com.forwardmeasure.openworkflow.adapter.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * URI-aware view of digest-pinned JSON/YAML operation documents.
 *
 * <p>References are resolved only against the immutable resources carried by an {@link
 * OperationRequest}. No network access is possible here.
 */
public final class OperationDocumentGraph {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  private final URI primaryUri;
  private final Map<URI, ResolvedWorkflowResource> resources;
  private final Map<URI, JsonNode> parsed = new HashMap<>();

  public OperationDocumentGraph(
      ResolvedWorkflowResource primary, List<ResolvedWorkflowResource> resources) {
    Objects.requireNonNull(primary, "primary");
    Objects.requireNonNull(resources, "resources");
    this.primaryUri = primary.uri();
    Map<URI, ResolvedWorkflowResource> indexed = new HashMap<>();
    for (ResolvedWorkflowResource resource : resources) {
      ResolvedWorkflowResource prior = indexed.put(resource.uri(), resource);
      if (prior != null && !prior.sha256().equals(resource.sha256())) {
        throw new IllegalArgumentException(
            "Operation resource graph contains conflicting " + "content for " + resource.uri());
      }
    }
    ResolvedWorkflowResource pinned = indexed.get(primaryUri);
    if (pinned == null || !pinned.sha256().equals(primary.sha256())) {
      throw new IllegalArgumentException(
          "Operation resource graph does not contain its primary " + "resource");
    }
    this.resources = Map.copyOf(indexed);
  }

  public LocatedNode root() {
    return new LocatedNode(primaryUri, parse(primaryUri), primaryUri);
  }

  public LocatedNode child(LocatedNode owner, String field) {
    Objects.requireNonNull(owner, "owner");
    return located(owner, owner.node().get(field));
  }

  public LocatedNode located(LocatedNode owner, JsonNode node) {
    Objects.requireNonNull(owner, "owner");
    String pointer = pointerTo(parse(owner.documentUri()), node, "");
    return new LocatedNode(
        owner.documentUri(),
        node,
        pointer == null ? null : withFragment(owner.documentUri(), pointer));
  }

  public LocatedNode dereference(LocatedNode value) {
    Objects.requireNonNull(value, "value");
    LocatedNode current = value;
    Set<URI> visited = new HashSet<>();
    while (current.node() != null && current.node().isObject() && current.node().has("$ref")) {
      String reference = current.node().required("$ref").asText();
      URI absolute;
      try {
        absolute = current.documentUri().resolve(reference).normalize();
      } catch (IllegalArgumentException invalid) {
        throw new IllegalArgumentException(
            "Operation document has an invalid reference: " + reference, invalid);
      }
      if (!absolute.isAbsolute() || !visited.add(absolute)) {
        throw new IllegalArgumentException(
            "Operation document has a cyclic or non-absolute " + "reference: " + reference);
      }
      URI documentUri = withoutFragment(absolute);
      JsonNode target = parse(documentUri);
      String fragment = absolute.getFragment();
      if (fragment != null && !fragment.isEmpty()) {
        if (!fragment.startsWith("/")) {
          throw new IllegalArgumentException(
              "Operation document reference uses an "
                  + "unsupported non-pointer fragment: "
                  + reference);
        }
        target = target.at(fragment);
      }
      if (target.isMissingNode()) {
        throw new IllegalArgumentException(
            "Operation document reference does not exist: " + absolute);
      }
      current = new LocatedNode(documentUri, target, absolute);
    }
    return current;
  }

  private static String pointerTo(JsonNode current, JsonNode target, String pointer) {
    if (current == target) {
      return pointer;
    }
    if (current == null || target == null) {
      return null;
    }
    if (current.isObject()) {
      for (var field : current.properties()) {
        String located =
            pointerTo(field.getValue(), target, pointer + "/" + escapePointer(field.getKey()));
        if (located != null) {
          return located;
        }
      }
    } else if (current.isArray()) {
      for (int index = 0; index < current.size(); index++) {
        String located = pointerTo(current.get(index), target, pointer + "/" + index);
        if (located != null) {
          return located;
        }
      }
    }
    return null;
  }

  private static String escapePointer(String token) {
    return token.replace("~", "~0").replace("/", "~1");
  }

  private static URI withFragment(URI documentUri, String fragment) {
    try {
      if (documentUri.isOpaque()) {
        return new URI(documentUri.getScheme(), documentUri.getSchemeSpecificPart(), fragment);
      }
      return new URI(
          documentUri.getScheme(),
          documentUri.getAuthority(),
          documentUri.getPath(),
          documentUri.getQuery(),
          fragment);
    } catch (java.net.URISyntaxException invalid) {
      throw new IllegalArgumentException("Cannot address operation document node", invalid);
    }
  }

  private JsonNode parse(URI uri) {
    return parsed.computeIfAbsent(
        uri,
        ignored -> {
          ResolvedWorkflowResource resource = resources.get(uri);
          if (resource == null) {
            throw new IllegalArgumentException(
                "External operation reference was not admitted: " + uri);
          }
          try {
            String mediaType = resource.mediaType().toLowerCase(Locale.ROOT);
            JsonNode document =
                (mediaType.contains("yaml") || mediaType.contains("yml") ? YAML : JSON)
                    .readTree(resource.content());
            if (document == null) {
              throw new IllegalArgumentException("Operation document is empty: " + uri);
            }
            return document;
          } catch (IOException invalid) {
            throw new IllegalArgumentException(
                "Operation document is not valid JSON/YAML: " + uri, invalid);
          }
        });
  }

  private static URI withoutFragment(URI uri) {
    if (uri.getFragment() == null) {
      return uri;
    }
    String value = uri.toString();
    return URI.create(value.substring(0, value.indexOf('#')));
  }

  public record LocatedNode(URI documentUri, JsonNode node, URI locationUri) {
    public LocatedNode {
      Objects.requireNonNull(documentUri, "documentUri");
    }

    public LocatedNode(URI documentUri, JsonNode node) {
      this(documentUri, node, null);
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
