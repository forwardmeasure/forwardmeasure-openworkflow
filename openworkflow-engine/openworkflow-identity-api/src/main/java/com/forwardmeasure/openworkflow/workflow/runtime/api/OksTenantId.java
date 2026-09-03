package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/** Immutable tenant DID supplied by an authenticated ingress adapter. */
public record OksTenantId(Did value) implements Comparable<OksTenantId> {
  public OksTenantId {
    Objects.requireNonNull(value, "value");
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static OksTenantId parse(String value) {
    return new OksTenantId(Did.parse(value));
  }

  /** MicroProfile Config implicit-converter entry point. */
  public static OksTenantId of(String value) {
    return parse(value);
  }

  @Override
  public int compareTo(OksTenantId other) {
    return value.compareTo(other.value);
  }

  @JsonValue
  @Override
  public String toString() {
    return value.toString();
  }
}
