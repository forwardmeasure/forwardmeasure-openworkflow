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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CapabilityPackLoaderTest {
  @Test
  void loadsTheBundledOpenworkflowPackWithNoOverrideDirectory() {
    Map<String, CapabilityPack> packs = CapabilityPackLoader.load(null);

    assertEquals(Set.of("openworkflow"), packs.keySet());
    CapabilityPack openworkflow = packs.get("openworkflow");
    assertEquals("1", openworkflow.version());
    assertTrue(openworkflow.roles().contains("workflow-administrator"));
  }

  @Test
  void overrideDirectoryReplacesABundledPackById(@TempDir Path directory) throws IOException {
    Files.writeString(
        directory.resolve("openworkflow-override.json"),
        """
        {"packId": "openworkflow", "packVersion": "2", "roles": ["custom-role"]}
        """);

    Map<String, CapabilityPack> packs = CapabilityPackLoader.load(directory.toString());

    assertEquals(Set.of("openworkflow"), packs.keySet());
    CapabilityPack openworkflow = packs.get("openworkflow");
    assertEquals("2", openworkflow.version());
    assertEquals(Set.of("custom-role"), openworkflow.roles());
  }

  @Test
  void overrideDirectoryAddsANewPack(@TempDir Path directory) throws IOException {
    Files.writeString(
        directory.resolve("new-pack.json"),
        """
        {"packId": "new-product", "packVersion": "1", "roles": ["new-product-viewer"]}
        """);

    Map<String, CapabilityPack> packs = CapabilityPackLoader.load(directory.toString());

    assertEquals(Set.of("openworkflow", "new-product"), packs.keySet());
    assertEquals(Set.of("new-product-viewer"), packs.get("new-product").roles());
  }
}
