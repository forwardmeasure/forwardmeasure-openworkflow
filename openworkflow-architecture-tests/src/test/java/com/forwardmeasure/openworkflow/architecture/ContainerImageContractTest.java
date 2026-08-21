/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.architecture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ContainerImageContractTest {
  private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
  private static final List<String> FRAMEWORKS = List.of("quarkus", "spring", "micronaut");

  @Test
  void imageCatalogContainsEveryIndependentWorkload() throws IOException {
    String versions =
        Files.readString(ROOT.resolve("deploy/helmfile/environments/image-versions.yaml"));
    for (String framework : FRAMEWORKS) {
      assertImage(versions, "openworkflow-definition-management-" + framework);
      assertImage(versions, "openworkflow-execution-management-" + framework);
      assertImage(versions, "openworkflow-engine-kafka-streams-" + framework);
      assertImage(versions, "openworkflow-engine-pekko-" + framework);
      assertImage(versions, "openworkflow-operation-adapter-" + framework);
      assertImage(versions, "openworkflow-studio-" + framework);
    }
    assertFalse(versions.contains("openworkflow-quarkus-service"));
    assertFalse(versions.contains("openworkflow-spring-service"));
    assertFalse(versions.contains("openworkflow-micronaut-service"));
  }

  @Test
  void helmfileDeclaresEachCapabilityAndEngineFlavour() throws IOException {
    String root = Files.readString(ROOT.resolve("deploy/helmfile/helmfile.yaml.gotmpl"));
    for (String file :
        List.of(
            "definition-services.yaml.gotmpl",
            "execution-services.yaml.gotmpl",
            "execution-engines.yaml.gotmpl",
            "operation-adapters.yaml.gotmpl",
            "studios.yaml.gotmpl")) {
      assertTrue(root.contains(file), file);
    }
    String engines =
        Files.readString(ROOT.resolve("deploy/helmfile/helmfiles/execution-engines.yaml.gotmpl"));
    assertTrue(engines.contains("list \"kafka-streams\" \"pekko\""));
    assertTrue(engines.contains("list \"quarkus\" \"spring\" \"micronaut\""));
  }

  @Test
  void engineAssembliesCannotMixKafkaStreamsAndPekko() throws IOException {
    for (String framework : FRAMEWORKS) {
      Path kafka =
          ROOT.resolve("openworkflow-deployments/engine-kafka-streams/" + framework + "/pom.xml");
      Path pekko = ROOT.resolve("openworkflow-deployments/engine-pekko/" + framework + "/pom.xml");
      assertTrue(Files.exists(kafka), kafka.toString());
      assertTrue(Files.exists(pekko), pekko.toString());
      assertFalse(Files.readString(kafka).contains("openworkflow-pekko"), kafka.toString());
      assertFalse(Files.readString(pekko).contains("openworkflow-kafka-streams"), pekko.toString());
    }
  }

  @Test
  void operationAdaptersAreIndependentExecutablesForEveryFramework() throws IOException {
    for (String framework : FRAMEWORKS) {
      Path module = ROOT.resolve("openworkflow-deployments/operation-adapter/" + framework);
      String pom = Files.readString(module.resolve("pom.xml"));
      assertTrue(pom.contains("openworkflow-operation-adapter-kafka"), framework);
      assertTrue(pom.contains("openworkflow-operation-adapter-pekko"), framework);
      assertFalse(pom.contains("openworkflow-engine-kafka-streams-"), framework);
      assertFalse(pom.contains("openworkflow-engine-pekko-"), framework);
      assertTrue(Files.exists(module.resolve("src/main/docker/Dockerfile.jvm")), framework);
    }
  }

  @Test
  void obsoleteMixedDeploymentChartsAreRemoved() {
    for (String chart :
        List.of(
            "openworkflow-definition-service",
            "openworkflow-kafka-runtime",
            "openworkflow-pekko-runtime")) {
      assertFalse(
          Files.exists(ROOT.resolve("deploy/helmfile/releases/" + chart + "/Chart.yaml")), chart);
    }
  }

  @Test
  void sharedFrameworkBindingsContainNoProductCapability() throws IOException {
    for (String framework : FRAMEWORKS) {
      String pom =
          Files.readString(
              ROOT.resolve(
                  "openworkflow-framework-bindings/"
                      + framework
                      + "/openworkflow-"
                      + framework
                      + "-binding/pom.xml"));
      assertFalse(pom.contains("openworkflow-definition-management"), framework);
      assertFalse(pom.contains("openworkflow-execution-management"), framework);
      assertFalse(pom.contains("openworkflow-engine-" + framework), framework);
      assertFalse(pom.contains("openworkflow-kafka-streams"), framework);
      assertFalse(pom.contains("openworkflow-pekko"), framework);
    }
  }

  private static void assertImage(String catalog, String image) {
    assertTrue(catalog.contains("forwardmeasure/" + image), image);
  }
}
