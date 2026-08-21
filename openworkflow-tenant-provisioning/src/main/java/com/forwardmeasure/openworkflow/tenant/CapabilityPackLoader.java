/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads capability packs bundled on the classpath (config/keycloak/*.json, indexed by
 * capability-packs-index.json), then overlays any *.json files found in an optional external
 * directory. Overlay entries add a new pack or replace a bundled one with the same id - this is how
 * a deployment can add or update packs (e.g. mounting a ConfigMap) without rebuilding this image.
 */
final class CapabilityPackLoader {
  private static final String BUNDLED_INDEX = "/capability-packs-index.json";

  private CapabilityPackLoader() {}

  static Map<String, CapabilityPack> load(String overrideDirectory) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, CapabilityPack> packs = new LinkedHashMap<>();
    for (String resource : bundledResourceNames(mapper)) {
      CapabilityPack pack = readResource(mapper, resource);
      packs.put(pack.id(), pack);
    }
    if (overrideDirectory != null && !overrideDirectory.isBlank()) {
      overlayDirectory(mapper, Path.of(overrideDirectory), packs);
    }
    return Map.copyOf(packs);
  }

  private static List<String> bundledResourceNames(ObjectMapper mapper) {
    try (InputStream input = CapabilityPackLoader.class.getResourceAsStream(BUNDLED_INDEX)) {
      if (input == null) {
        throw new IllegalStateException("Missing bundled resource " + BUNDLED_INDEX);
      }
      return mapper.readValue(
          input, mapper.getTypeFactory().constructCollectionType(List.class, String.class));
    } catch (IOException exception) {
      throw new UncheckedIOException("Unable to read " + BUNDLED_INDEX, exception);
    }
  }

  private static CapabilityPack readResource(ObjectMapper mapper, String name) {
    String resource = name.startsWith("/") ? name : "/" + name;
    try (InputStream input = CapabilityPackLoader.class.getResourceAsStream(resource)) {
      if (input == null) {
        throw new IllegalStateException("Missing bundled capability pack resource " + resource);
      }
      return mapper.readValue(input, CapabilityPackDocument.class).toCapabilityPack();
    } catch (IOException exception) {
      throw new UncheckedIOException("Unable to read " + resource, exception);
    }
  }

  private static void overlayDirectory(
      ObjectMapper mapper, Path directory, Map<String, CapabilityPack> packs) {
    if (!Files.isDirectory(directory)) {
      throw new IllegalStateException(
          "Capability pack config directory does not exist: " + directory);
    }
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, "*.json")) {
      for (Path entry : entries) {
        CapabilityPack pack =
            mapper.readValue(entry.toFile(), CapabilityPackDocument.class).toCapabilityPack();
        packs.put(pack.id(), pack);
      }
    } catch (IOException exception) {
      throw new UncheckedIOException(
          "Unable to read capability pack overrides from " + directory, exception);
    }
  }
}
