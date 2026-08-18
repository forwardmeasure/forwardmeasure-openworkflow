package com.forwardmeasure.openworkflow.persistence;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.util.List;
import java.util.Objects;

/** Selects one namespaced backend configuration without classpath-order effects. */
public final class PersistenceConfigLoader {
  private static final String ROOT = "openworkflow.persistence.profiles.";

  private PersistenceConfigLoader() {}

  public static Config select(Config loaded, PersistenceProfile profile) {
    Objects.requireNonNull(loaded, "loaded");
    Objects.requireNonNull(profile, "profile");
    String path = ROOT + profile.value();
    if (!loaded.hasPath(path)) {
      throw new IllegalStateException(
          "Persistence profile artifact is not on the classpath: " + profile.value());
    }
    return loaded.getConfig(path).withFallback(loaded).resolve();
  }

  /**
   * Applies the host's canonical database settings to the selected Pekko persistence profile. This
   * keeps framework bindings independent of each plugin's private configuration paths.
   */
  public static Config withConnection(
      Config selected,
      PersistenceProfile profile,
      String endpoint,
      String username,
      String password,
      String localDatacenter) {
    Objects.requireNonNull(selected, "selected");
    Objects.requireNonNull(profile, "profile");
    requireText(endpoint, "endpoint");
    username = Objects.requireNonNullElse(username, "");
    password = Objects.requireNonNullElse(password, "");

    return switch (profile) {
      case POSTGRESQL ->
          selected
              .withValue(
                  "pekko-persistence-jdbc.shared-databases.openworkflow.db.url",
                  ConfigValueFactory.fromAnyRef(endpoint))
              .withValue(
                  "pekko-persistence-jdbc.shared-databases.openworkflow.db.user",
                  ConfigValueFactory.fromAnyRef(username))
              .withValue(
                  "pekko-persistence-jdbc.shared-databases.openworkflow.db.password",
                  ConfigValueFactory.fromAnyRef(password))
              .resolve();
      case CASSANDRA -> configureCassandra(selected, endpoint, username, password, localDatacenter);
    };
  }

  private static Config configureCassandra(
      Config selected, String endpoint, String username, String password, String localDatacenter) {
    requireText(localDatacenter, "localDatacenter");
    Config configured =
        selected
            .withValue(
                "datastax-java-driver.basic.contact-points",
                ConfigValueFactory.fromIterable(List.of(endpoint)))
            .withValue(
                "datastax-java-driver.basic.load-balancing-policy.local-datacenter",
                ConfigValueFactory.fromAnyRef(localDatacenter));
    if (!username.isBlank()) {
      requireText(password, "password");
      configured =
          configured
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.class",
                  ConfigValueFactory.fromAnyRef("PlainTextAuthProvider"))
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.username",
                  ConfigValueFactory.fromAnyRef(username))
              .withValue(
                  "datastax-java-driver.advanced.auth-provider.password",
                  ConfigValueFactory.fromAnyRef(password));
    }
    return configured.resolve();
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
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
