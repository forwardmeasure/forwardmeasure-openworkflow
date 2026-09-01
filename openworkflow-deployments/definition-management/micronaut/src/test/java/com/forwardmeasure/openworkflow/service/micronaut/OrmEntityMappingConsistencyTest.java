/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.micronaut;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import jakarta.persistence.Entity;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Guards against the documented Micronaut/Hibernate JPA trap (see {@code
 * docs/source-provenance.md}): {@code META-INF/openworkflow-orm.xml} explicitly enumerates every
 * {@code @Entity} class by fully-qualified name because Micronaut's Hibernate JPA integration does
 * not reliably discover entities via classpath package-scanning (unlike Spring/Quarkus). If a new
 * {@code @Entity} class is added to one of the scanned packages but not also added to this XML
 * file, Micronaut silently fails to see it at runtime, with no compile-time signal.
 *
 * <p>This test reflectively scans the same packages {@code application.yaml} configures under
 * {@code jpa.default.packages-to-scan} / {@code entity-scan.packages} for classes carrying {@link
 * Entity}, parses the actual {@code openworkflow-orm.xml} shipped on the classpath, and asserts the
 * two sets of fully-qualified class names are identical. That catches both directions of drift: a
 * forgotten new entity (found by the scan, missing from the XML) and a stale leftover XML entry
 * (listed in the XML, no longer a real {@code @Entity} on the classpath).
 *
 * <p>The scan spans both a directory of compiled classes (the reactor sibling module {@code
 * openworkflow-definition-management-application}, which owns {@code
 * com.forwardmeasure.openworkflow.definition.domain.entity}) and a packaged dependency jar ({@code
 * forwardmeasure-jpa-identity}, which owns {@code com.forwardmeasure.jpa.identity.entity}).
 * ArchUnit's {@link ClassFileImporter} - already used elsewhere in this repository for
 * classpath-wide architecture rules (see {@code openworkflow-architecture-tests}) - reads bytecode
 * off whatever classpath entry the JVM resolves for a package, directory or jar alike, so no
 * additional scanning library is needed.
 */
class OrmEntityMappingConsistencyTest {

  // Mirrors jpa.default.packages-to-scan / entity-scan.packages in
  // src/main/resources/application.yaml.
  private static final String[] SCANNED_PACKAGES = {
    "com.forwardmeasure.jpa.identity.entity",
    "com.forwardmeasure.openworkflow.definition.domain.entity"
  };

  private static final String ORM_XML_RESOURCE = "/META-INF/openworkflow-orm.xml";

  @Test
  void ormXmlEntityListExactlyMatchesEntityAnnotatedClassesOnClasspath() throws Exception {
    Set<String> scannedEntities = scanForEntityAnnotatedClasses();
    Set<String> xmlEntities = parseOrmXmlEntityClasses();

    Set<String> missingFromXml = new TreeSet<>(scannedEntities);
    missingFromXml.removeAll(xmlEntities);

    Set<String> staleInXml = new TreeSet<>(xmlEntities);
    staleInXml.removeAll(scannedEntities);

    assertTrue(
        missingFromXml.isEmpty(),
        () ->
            "@Entity class(es) found on the classpath but NOT registered in "
                + ORM_XML_RESOURCE
                + " - Micronaut will silently fail to see them at runtime. Add <entity"
                + " class=\"...\"/> for: "
                + missingFromXml);

    assertTrue(
        staleInXml.isEmpty(),
        () ->
            "<entity class=\"...\"/> entries found in "
                + ORM_XML_RESOURCE
                + " that no longer correspond to a real @Entity class on the classpath (stale"
                + " leftover mapping - remove or investigate): "
                + staleInXml);
  }

  private static Set<String> scanForEntityAnnotatedClasses() {
    JavaClasses classes = new ClassFileImporter().importPackages(SCANNED_PACKAGES);
    Set<String> result = new HashSet<>();
    classes.stream()
        .filter(javaClass -> javaClass.isAnnotatedWith(Entity.class))
        .forEach(javaClass -> result.add(javaClass.getName()));
    return result;
  }

  private static Set<String> parseOrmXmlEntityClasses()
      throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    Set<String> result = new HashSet<>();
    try (InputStream xmlStream =
        OrmEntityMappingConsistencyTest.class.getResourceAsStream(ORM_XML_RESOURCE)) {
      if (xmlStream == null) {
        throw new IllegalStateException(
            ORM_XML_RESOURCE
                + " not found on the test classpath - expected it under"
                + " src/main/resources/META-INF/.");
      }
      // Namespace-wildcarded lookup so this keeps working across orm.xml schema-version bumps.
      NodeList entityNodes = builder.parse(xmlStream).getElementsByTagNameNS("*", "entity");
      for (int i = 0; i < entityNodes.getLength(); i++) {
        Element entityElement = (Element) entityNodes.item(i);
        result.add(entityElement.getAttribute("class"));
      }
    }
    return result;
  }
}
