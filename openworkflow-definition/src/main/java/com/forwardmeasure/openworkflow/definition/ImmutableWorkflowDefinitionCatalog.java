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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable tenant-scoped catalogue used while compiling parent workflows. The caller must supply
 * definitions for exactly one authorized tenant.
 */
public final class ImmutableWorkflowDefinitionCatalog implements WorkflowDefinitionCatalog {
  private static final String OPEN_WORKFLOW_DSL = "1.0.3";
  private final Map<WorkflowCoordinates, ResolvedSubflow> definitions;

  public ImmutableWorkflowDefinitionCatalog(Collection<ResolvedSubflow> definitions) {
    Objects.requireNonNull(definitions, "definitions");
    Map<WorkflowCoordinates, ResolvedSubflow> indexed = new LinkedHashMap<>();
    for (ResolvedSubflow definition : definitions) {
      Objects.requireNonNull(definition, "definition");
      ResolvedSubflow duplicate = indexed.putIfAbsent(definition.coordinates(), definition);
      if (duplicate != null && !duplicate.equals(definition)) {
        throw new IllegalArgumentException(
            "Conflicting immutable workflow definition " + definition.coordinates());
      }
    }
    this.definitions = Map.copyOf(indexed);
  }

  @Override
  public Optional<ResolvedSubflow> resolve(String namespace, String name, String requestedVersion) {
    Objects.requireNonNull(namespace, "namespace");
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(requestedVersion, "requestedVersion");
    if (!"latest".equals(requestedVersion)) {
      return Optional.ofNullable(
          definitions.get(
              new WorkflowCoordinates(namespace, name, requestedVersion, OPEN_WORKFLOW_DSL)));
    }
    List<ResolvedSubflow> candidates = new ArrayList<>();
    for (ResolvedSubflow candidate : definitions.values()) {
      WorkflowCoordinates coordinates = candidate.coordinates();
      if (coordinates.namespace().equals(namespace)
          && coordinates.name().equals(name)
          && coordinates.dsl().equals(OPEN_WORKFLOW_DSL)) {
        candidates.add(candidate);
      }
    }
    return candidates.stream()
        .max(
            Comparator.comparing(
                candidate -> SemanticVersion.parse(candidate.coordinates().version())));
  }

  /** SemVer 2.0 precedence used only to pin publication-time latest. */
  private record SemanticVersion(int major, int minor, int patch, List<String> prerelease)
      implements Comparable<SemanticVersion> {
    private static final Pattern PATTERN =
        Pattern.compile(
            "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
                + "(?:-([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?"
                + "(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$");

    static SemanticVersion parse(String value) {
      Matcher matched = PATTERN.matcher(value);
      if (!matched.matches()) {
        throw new IllegalArgumentException(
            "Admitted workflow has invalid semantic version " + value);
      }
      return new SemanticVersion(
          Integer.parseInt(matched.group(1)),
          Integer.parseInt(matched.group(2)),
          Integer.parseInt(matched.group(3)),
          matched.group(4) == null ? List.of() : List.of(matched.group(4).split("\\.")));
    }

    @Override
    public int compareTo(SemanticVersion other) {
      int compared = Integer.compare(major, other.major);
      if (compared == 0) {
        compared = Integer.compare(minor, other.minor);
      }
      if (compared == 0) {
        compared = Integer.compare(patch, other.patch);
      }
      if (compared != 0) return compared;
      if (prerelease.isEmpty() || other.prerelease.isEmpty()) {
        return prerelease.isEmpty() ? (other.prerelease.isEmpty() ? 0 : 1) : -1;
      }
      int length = Math.min(prerelease.size(), other.prerelease.size());
      for (int index = 0; index < length; index++) {
        String left = prerelease.get(index);
        String right = other.prerelease.get(index);
        boolean leftNumeric = left.chars().allMatch(Character::isDigit);
        boolean rightNumeric = right.chars().allMatch(Character::isDigit);
        if (leftNumeric && rightNumeric) {
          compared = new BigInteger(left).compareTo(new BigInteger(right));
        } else if (leftNumeric != rightNumeric) {
          compared = leftNumeric ? -1 : 1;
        } else {
          compared = left.compareTo(right);
        }
        if (compared != 0) return compared;
      }
      return Integer.compare(prerelease.size(), other.prerelease.size());
    }
  }
}
