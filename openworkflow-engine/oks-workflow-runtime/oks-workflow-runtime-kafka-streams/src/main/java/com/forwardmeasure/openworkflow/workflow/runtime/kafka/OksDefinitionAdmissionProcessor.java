package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowDefinitionException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdmitWorkflowDefinitionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.KafkaRecordLimitException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.KafkaRecordLimits;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Validates source and admits one immutable tenant-scoped definition version. */
final class OksDefinitionAdmissionProcessor
    implements Processor<String, AdmitWorkflowDefinitionCommand, String, OksDefinitionOutput> {
  private final OpenWorkflowCompiler compiler = new OpenWorkflowCompiler();
  private ProcessorContext<String, OksDefinitionOutput> context;
  private KeyValueStore<String, WorkflowDefinitionBundle> definitions;
  private KeyValueStore<String, WorkflowDefinitionBundle> catalog;
  private KeyValueStore<String, String> commandReceipts;

  @Override
  public void init(ProcessorContext<String, OksDefinitionOutput> context) {
    this.context = Objects.requireNonNull(context, "context");
    this.definitions = context.getStateStore(OksStores.DEFINITION_ADMISSIONS);
    this.catalog = context.getStateStore(OksStores.DEFINITIONS);
    this.commandReceipts = context.getStateStore(OksStores.DEFINITION_COMMANDS);
  }

  @Override
  public void process(Record<String, AdmitWorkflowDefinitionCommand> record) {
    if (record.value() == null) return;
    AdmitWorkflowDefinitionCommand command = record.value();
    if (!command.key().canonical().equals(record.key())) {
      throw new IllegalArgumentException("Kafka key does not match definition command key");
    }

    String fingerprint = CommandFingerprints.sha256(command);
    String receiptKey = receiptKey(command);
    String priorFingerprint = commandReceipts.get(receiptKey);
    if (priorFingerprint != null) {
      if (priorFingerprint.equals(fingerprint)) {
        return;
      }
      forwardHistory(
          record,
          decision(
              command,
              WorkflowDefinitionAdmissionStatus.REJECTED,
              sourceSha256(command.source()),
              null,
              null,
              List.of("Admission command ID was reused with different content")));
      return;
    }

    final com.forwardmeasure.openworkflow.definition.WorkflowPlan plan;
    try {
      plan =
          compiler.compile(
              command.source().getBytes(StandardCharsets.UTF_8),
              command.resources(),
              (namespace, name, version) -> resolveSubflow(command, namespace, name, version));
    } catch (WorkflowDefinitionException rejected) {
      decide(
          record,
          fingerprint,
          decision(
              command,
              WorkflowDefinitionAdmissionStatus.REJECTED,
              sourceSha256(command.source()),
              null,
              null,
              rejected.violations()));
      return;
    }
    if (!command.key().coordinates().equals(plan.coordinates())) {
      decide(
          record,
          fingerprint,
          decision(
              command,
              WorkflowDefinitionAdmissionStatus.REJECTED,
              plan.sourceSha256(),
              plan.definitionSha256(),
              OpenWorkflowCompiler.COMPILER_SHA256,
              List.of("Command definition key does not match source document coordinates")));
      return;
    }
    WorkflowDefinitionBundle existing = definitions.get(record.key());
    if (existing != null) {
      observeExisting(record, command, fingerprint, plan, existing);
      return;
    }

    var bundle =
        new WorkflowDefinitionBundle(
            command.key(),
            command.source(),
            plan,
            OpenWorkflowCompiler.COMPILER_SHA256,
            command.commandId(),
            command.actor(),
            command.requestedAt());
    try {
      byte[] encoded =
          new JsonSerde<>(WorkflowDefinitionBundle.class)
              .serializer()
              .serialize("definition-size-validation", bundle);
      KafkaRecordLimits.requireDefinitionPayload(encoded.length);
    } catch (KafkaRecordLimitException oversized) {
      decide(
          record,
          fingerprint,
          decision(
              command,
              WorkflowDefinitionAdmissionStatus.REJECTED,
              plan.sourceSha256(),
              plan.definitionSha256(),
              OpenWorkflowCompiler.COMPILER_SHA256,
              List.of(oversized.getMessage())));
      return;
    }
    definitions.put(record.key(), bundle);
    context.forward(
        record
            .withKey(bundle.reference().canonical())
            .withValue(new OksDefinitionOutput.Bundle(bundle)),
        OksTopology.DEFINITION_BUNDLE_OUTPUT);
    decide(
        record,
        fingerprint,
        decision(
            command,
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            plan.sourceSha256(),
            plan.definitionSha256(),
            OpenWorkflowCompiler.COMPILER_SHA256,
            List.of()),
        bundle);
  }

  private void observeExisting(
      Record<String, AdmitWorkflowDefinitionCommand> record,
      AdmitWorkflowDefinitionCommand command,
      String fingerprint,
      com.forwardmeasure.openworkflow.definition.WorkflowPlan candidate,
      WorkflowDefinitionBundle existing) {
    if (candidate.definitionSha256().equals(existing.plan().definitionSha256())) {
      decide(
          record,
          fingerprint,
          decision(
              command,
              WorkflowDefinitionAdmissionStatus.UNCHANGED,
              existing.plan().sourceSha256(),
              existing.plan().definitionSha256(),
              existing.compilerSha256(),
              List.of()),
          existing);
      return;
    }
    decide(
        record,
        fingerprint,
        decision(
            command,
            WorkflowDefinitionAdmissionStatus.REJECTED,
            candidate.sourceSha256(),
            candidate.definitionSha256(),
            null,
            List.of(
                "Workflow version is immutable and already has different source or resolved schema"
                    + " content")));
  }

  private void decide(
      Record<String, AdmitWorkflowDefinitionCommand> record,
      String fingerprint,
      WorkflowDefinitionAdmissionEvent event) {
    decide(record, fingerprint, event, null);
  }

  private void decide(
      Record<String, AdmitWorkflowDefinitionCommand> record,
      String fingerprint,
      WorkflowDefinitionAdmissionEvent event,
      WorkflowDefinitionBundle bundle) {
    commandReceipts.put(receiptKey(record.value()), fingerprint);
    forwardHistory(record, event);
    context.forward(
        record
            .withKey(event.commandId())
            .withValue(
                new OksDefinitionOutput.Catalogue(
                    new com.forwardmeasure.openworkflow.workflow.runtime.api
                        .WorkflowDefinitionCatalogueEvent(event, bundle))),
        OksTopology.DEFINITION_CATALOGUE_OUTPUT);
  }

  private void forwardHistory(
      Record<String, AdmitWorkflowDefinitionCommand> record,
      WorkflowDefinitionAdmissionEvent event) {
    context.forward(
        record.withValue(new OksDefinitionOutput.Decision(event)),
        OksTopology.DEFINITION_HISTORY_OUTPUT);
  }

  private static WorkflowDefinitionAdmissionEvent decision(
      AdmitWorkflowDefinitionCommand command,
      WorkflowDefinitionAdmissionStatus status,
      String sourceSha256,
      String definitionSha256,
      String compilerSha256,
      List<String> issues) {
    return new WorkflowDefinitionAdmissionEvent(
        command.key().canonical() + ":" + command.commandId(),
        command.commandId(),
        command.key(),
        status,
        sourceSha256,
        definitionSha256,
        compilerSha256,
        issues,
        command.actor(),
        command.requestedAt());
  }

  private static String sourceSha256(String source) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static String receiptKey(AdmitWorkflowDefinitionCommand command) {
    return command.key().canonical() + command.commandId().length() + ":" + command.commandId();
  }

  private Optional<ResolvedSubflow> resolveSubflow(
      AdmitWorkflowDefinitionCommand command,
      String namespace,
      String name,
      String requestedVersion) {
    WorkflowDefinitionBundle selected = null;
    try (var entries = catalog.all()) {
      while (entries.hasNext()) {
        WorkflowDefinitionBundle candidate = entries.next().value;
        if (!candidate.key().tenantId().equals(command.key().tenantId())
            || !candidate.key().coordinates().namespace().equals(namespace)
            || !candidate.key().coordinates().name().equals(name)
            || !"1.0.3".equals(candidate.key().coordinates().dsl())) {
          continue;
        }
        String candidateVersion = candidate.key().coordinates().version();
        if (!"latest".equals(requestedVersion)) {
          if (candidateVersion.equals(requestedVersion)) {
            selected = candidate;
            break;
          }
          continue;
        }
        if (selected == null
            || SemanticVersion.compare(candidateVersion, selected.key().coordinates().version())
                > 0) {
          selected = candidate;
        }
      }
    }
    if (selected == null) return Optional.empty();
    return Optional.of(
        new ResolvedSubflow(
            selected.key().coordinates(),
            selected.plan().sourceSha256(),
            selected.plan().definitionSha256()));
  }

  /** SemVer 2.0 precedence used only to pin a publication-time "latest". */
  private record SemanticVersion(int major, int minor, int patch, List<String> prerelease)
      implements Comparable<SemanticVersion> {
    private static final Pattern PATTERN =
        Pattern.compile(
            "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
                + "(?:-([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?"
                + "(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$");

    static int compare(String left, String right) {
      return parse(left).compareTo(parse(right));
    }

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
      int core = Integer.compare(major, other.major);
      if (core == 0) core = Integer.compare(minor, other.minor);
      if (core == 0) core = Integer.compare(patch, other.patch);
      if (core != 0) return core;
      if (prerelease.isEmpty() || other.prerelease.isEmpty()) {
        return prerelease.isEmpty() ? (other.prerelease.isEmpty() ? 0 : 1) : -1;
      }
      int length = Math.min(prerelease.size(), other.prerelease.size());
      for (int index = 0; index < length; index++) {
        String left = prerelease.get(index);
        String right = other.prerelease.get(index);
        boolean leftNumeric = left.chars().allMatch(Character::isDigit);
        boolean rightNumeric = right.chars().allMatch(Character::isDigit);
        int compared;
        if (leftNumeric && rightNumeric) {
          compared = new java.math.BigInteger(left).compareTo(new java.math.BigInteger(right));
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
