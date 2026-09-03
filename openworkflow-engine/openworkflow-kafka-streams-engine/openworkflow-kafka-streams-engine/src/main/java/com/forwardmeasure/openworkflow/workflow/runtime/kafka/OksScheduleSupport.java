package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static com.cronutils.model.CronType.UNIX;

import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.definition.Iso8601Duration;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HexFormat;
import java.util.Objects;
import org.apache.kafka.streams.KeyValue;

/** Stable schedule identities, descriptors and calendar calculations. */
final class OksScheduleSupport {
  static final String PURPOSE = "workflow-schedule";
  static final String KIND_EVERY = "every";
  static final String KIND_CRON = "cron";
  static final String KIND_AFTER = "after";
  static final String KIND_EVENT = "on";
  private static final CronParser CRON =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(UNIX));
  private static final JqRuntimeExpressionEvaluator EXPRESSIONS =
      new JqRuntimeExpressionEvaluator();

  private OksScheduleSupport() {}

  static WorkflowEffect timer(
      WorkflowDefinitionBundle bundle,
      String kind,
      Instant dueAt,
      String recurrence,
      JsonNode input,
      ActorContext actor) {
    Objects.requireNonNull(bundle, "bundle");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(dueAt, "dueAt");
    Objects.requireNonNull(input, "input");
    return timer(
        bundle.key(),
        bundle.plan().sourceSha256(),
        bundle.plan().definitionSha256(),
        kind,
        dueAt,
        recurrence,
        input,
        actor);
  }

  private static WorkflowEffect timer(
      WorkflowDefinitionKey definitionKey,
      String sourceSha256,
      String definitionSha256,
      String kind,
      Instant dueAt,
      String recurrence,
      JsonNode input,
      ActorContext actor) {
    String slot = kind + ":" + dueAt;
    String identity =
        digest(definitionKey.canonical() + sourceSha256 + definitionSha256 + "\n" + slot);
    ExecutionKey key =
        new ExecutionKey(
            definitionKey.tenantId(),
            new WorkflowExecutionId("scheduled-" + identity.substring(0, 40)));
    String timerId = "workflow-schedule:" + identity;
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", timerId);
    descriptor.put("purpose", PURPOSE);
    descriptor.put("scheduleKind", kind);
    descriptor.put("dueAt", dueAt.toString());
    descriptor.put("slot", slot);
    descriptor.put("definitionNamespace", definitionKey.coordinates().namespace());
    descriptor.put("definitionName", definitionKey.coordinates().name());
    descriptor.put("definitionVersion", definitionKey.coordinates().version());
    descriptor.put("definitionDsl", definitionKey.coordinates().dsl());
    descriptor.put("definitionSourceSha256", sourceSha256);
    descriptor.put("definitionSha256", definitionSha256);
    if (recurrence != null) {
      descriptor.put("recurrence", recurrence);
    }
    descriptor.set("input", input.deepCopy());
    return new WorkflowEffect(
        timerId,
        key,
        WorkflowEffectType.SCHEDULE_TIMER,
        "/",
        DataReferences.inline(descriptor),
        actor,
        actor.authenticatedAt());
  }

  static StartExecutionCommand start(WorkflowEffect timer, ActorContext actor) {
    JsonNode descriptor = timer.payload().inlineValue();
    WorkflowDefinitionKey definitionKey =
        new WorkflowDefinitionKey(
            timer.key().tenantId(),
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                descriptor.required("definitionNamespace").textValue(),
                descriptor.required("definitionName").textValue(),
                descriptor.required("definitionVersion").textValue(),
                descriptor.required("definitionDsl").textValue()));
    WorkflowDefinitionReference definition =
        new WorkflowDefinitionReference(
            definitionKey,
            descriptor.required("definitionSourceSha256").textValue(),
            descriptor.required("definitionSha256").textValue());
    String timerId = descriptor.required("timerId").textValue();
    return new StartExecutionCommand(
        "schedule-start:" + digest(timerId),
        timer.key(),
        definition,
        DataReferences.inline(descriptor.required("input")),
        actor,
        Instant.parse(descriptor.required("dueAt").textValue()));
  }

  static StartExecutionCommand eventStart(
      WorkflowDefinitionBundle bundle,
      String triggerIdentity,
      JsonNode input,
      ActorContext actor,
      Instant receivedAt) {
    String identity = digest(bundle.reference().canonical() + "\non:" + triggerIdentity);
    ExecutionKey key =
        new ExecutionKey(
            bundle.key().tenantId(),
            new WorkflowExecutionId("scheduled-" + identity.substring(0, 40)));
    return new StartExecutionCommand(
        "schedule-start:" + identity,
        key,
        bundle.reference(),
        DataReferences.inline(input),
        actor,
        receivedAt);
  }

  static KeyValue<String, WorkflowEffect> nextRecurringTimer(
      WorkflowEffect current, Instant now, ActorContext actor) {
    JsonNode descriptor = current.payload().inlineValue();
    String kind = descriptor.required("scheduleKind").textValue();
    if (!KIND_EVERY.equals(kind) && !KIND_CRON.equals(kind)) {
      return null;
    }
    Instant dueAt = Instant.parse(descriptor.required("dueAt").textValue());
    String recurrence = descriptor.required("recurrence").textValue();
    Instant next =
        KIND_EVERY.equals(kind) ? nextInterval(dueAt, now, recurrence) : nextCron(recurrence, now);
    WorkflowDefinitionKey definitionKey =
        new WorkflowDefinitionKey(
            current.key().tenantId(),
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                descriptor.required("definitionNamespace").textValue(),
                descriptor.required("definitionName").textValue(),
                descriptor.required("definitionVersion").textValue(),
                descriptor.required("definitionDsl").textValue()));
    WorkflowEffect effect =
        timer(
            definitionKey,
            descriptor.required("definitionSourceSha256").textValue(),
            descriptor.required("definitionSha256").textValue(),
            kind,
            next,
            recurrence,
            descriptor.required("input"),
            actor);
    return KeyValue.pair(effect.payload().inlineValue().required("timerId").textValue(), effect);
  }

  static Duration duration(
      DurationPlan plan,
      JsonNode input,
      com.forwardmeasure.openworkflow.expression.ExpressionMode mode,
      Instant anchor) {
    return duration(durationLiteral(plan, input, mode), anchor);
  }

  static Duration duration(String literal, Instant anchor) {
    Duration duration = Iso8601Duration.between(anchor, literal);
    if (duration.isNegative() || duration.isZero()) {
      throw new IllegalArgumentException("Schedule duration must be positive");
    }
    return duration;
  }

  static String durationLiteral(
      DurationPlan plan,
      JsonNode input,
      com.forwardmeasure.openworkflow.expression.ExpressionMode mode) {
    JsonNode value = plan.value();
    if (plan.kind() == DurationPlan.Kind.INLINE) {
      Duration duration =
          Duration.ofDays(value.path("days").asLong())
              .plusHours(value.path("hours").asLong())
              .plusMinutes(value.path("minutes").asLong())
              .plusSeconds(value.path("seconds").asLong())
              .plusMillis(value.path("milliseconds").asLong());
      if (duration.isNegative() || duration.isZero()) {
        throw new IllegalArgumentException("Schedule duration must be positive");
      }
      return duration.toString();
    }
    String text = value.textValue();
    if (plan.kind() == DurationPlan.Kind.EXPRESSION) {
      JsonNode evaluated =
          EXPRESSIONS.evaluateExpression(text, input, RuntimeExpressionArguments.empty(), mode);
      if (!evaluated.isTextual()) {
        throw new IllegalArgumentException(
            "Schedule duration expression must produce " + "an ISO 8601 duration");
      }
      text = evaluated.textValue();
    }
    Iso8601Duration.validate(text);
    return text;
  }

  static Instant nextCron(String expression, Instant after) {
    Cron parsed = CRON.parse(expression);
    parsed.validate();
    return ExecutionTime.forCron(parsed)
        .nextExecution(ZonedDateTime.ofInstant(after, ZoneOffset.UTC))
        .orElseThrow(() -> new IllegalArgumentException("Cron expression has no next execution"))
        .toInstant();
  }

  private static Instant nextInterval(Instant previousDue, Instant now, String recurrence) {
    String datePart =
        recurrence.substring(
            0, recurrence.indexOf('T') < 0 ? recurrence.length() : recurrence.indexOf('T'));
    if (datePart.indexOf('Y') >= 0 || datePart.indexOf('M') >= 0) {
      Instant next = Iso8601Duration.addTo(previousDue, recurrence);
      while (!next.isAfter(now)) {
        Instant advanced = Iso8601Duration.addTo(next, recurrence);
        if (!advanced.isAfter(next)) {
          throw new IllegalArgumentException("Schedule interval must be positive");
        }
        next = advanced;
      }
      return next;
    }
    Duration interval = Iso8601Duration.between(previousDue, recurrence);
    if (interval.isNegative() || interval.isZero()) {
      throw new IllegalArgumentException("Schedule interval must be positive");
    }
    if (previousDue.isAfter(now)) return previousDue;
    BigInteger elapsedNanos =
        BigInteger.valueOf(Duration.between(previousDue, now).getSeconds())
            .multiply(BigInteger.valueOf(1_000_000_000L))
            .add(BigInteger.valueOf(Duration.between(previousDue, now).getNano()));
    BigInteger intervalNanos =
        BigInteger.valueOf(interval.getSeconds())
            .multiply(BigInteger.valueOf(1_000_000_000L))
            .add(BigInteger.valueOf(interval.getNano()));
    BigInteger slots = elapsedNanos.divide(intervalNanos).add(BigInteger.ONE);
    BigInteger nextNanos = intervalNanos.multiply(slots);
    BigInteger[] parts = nextNanos.divideAndRemainder(BigInteger.valueOf(1_000_000_000L));
    return previousDue.plusSeconds(parts[0].longValueExact()).plusNanos(parts[1].longValueExact());
  }

  private static String digest(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
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
