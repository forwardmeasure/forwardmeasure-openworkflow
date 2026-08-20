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
package com.forwardmeasure.openworkflow.migration;

import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Validated connection and schema policy for one Cassandra migration run. */
public record CassandraMigrationTarget(
    List<InetSocketAddress> contactPoints,
    String localDatacenter,
    Optional<String> username,
    Optional<String> password,
    String migrationKeyspace,
    String applicationKeyspace,
    int replicationFactor) {
  private static final Pattern IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]{0,47}");

  public CassandraMigrationTarget {
    contactPoints = List.copyOf(Objects.requireNonNull(contactPoints, "contactPoints"));
    if (contactPoints.isEmpty()) {
      throw new IllegalArgumentException("At least one Cassandra contact point is required");
    }
    contactPoints.forEach(
        endpoint -> {
          Objects.requireNonNull(endpoint, "contactPoint");
          if (endpoint.getPort() < 1 || endpoint.getPort() > 65535) {
            throw new IllegalArgumentException("Invalid Cassandra contact-point port");
          }
        });
    localDatacenter = identifier(localDatacenter, "localDatacenter");
    migrationKeyspace = identifier(migrationKeyspace, "migrationKeyspace");
    applicationKeyspace = identifier(applicationKeyspace, "applicationKeyspace");
    username = normalized(username, "username");
    password = normalized(password, "password");
    if (username.isPresent() != password.isPresent()) {
      throw new IllegalArgumentException(
          "Cassandra username and password must be supplied together");
    }
    if (replicationFactor < 1) {
      throw new IllegalArgumentException("Cassandra replicationFactor must be positive");
    }
  }

  public String jdbcUrl() {
    String endpoints =
        contactPoints.stream()
            .map(CassandraMigrationTarget::jdbcEndpoint)
            .collect(Collectors.joining("--"));
    return "jdbc:cassandra://"
        + endpoints
        + "/"
        + migrationKeyspace
        + "?compliancemode=Liquibase&localdatacenter="
        + URLEncoder.encode(localDatacenter, StandardCharsets.UTF_8);
  }

  public static CassandraMigrationTarget unauthenticated(
      InetSocketAddress contactPoint, String localDatacenter) {
    return new CassandraMigrationTarget(
        List.of(contactPoint),
        localDatacenter,
        Optional.empty(),
        Optional.empty(),
        OpenWorkflowCassandraMigrator.DEFAULT_MIGRATION_KEYSPACE,
        OpenWorkflowCassandraMigrator.DEFAULT_APPLICATION_KEYSPACE,
        1);
  }

  private static Optional<String> normalized(Optional<String> value, String name) {
    Objects.requireNonNull(value, name);
    return value.map(item -> required(item, name));
  }

  private static String identifier(String value, String name) {
    String candidate = required(value, name);
    if (!IDENTIFIER.matcher(candidate).matches()) {
      throw new IllegalArgumentException("Invalid Cassandra " + name + ": " + candidate);
    }
    return candidate;
  }

  private static String required(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String jdbcEndpoint(InetSocketAddress endpoint) {
    String host = endpoint.getHostString();
    return (host.contains(":") ? "[" + host + "]" : host) + ":" + endpoint.getPort();
  }
}
