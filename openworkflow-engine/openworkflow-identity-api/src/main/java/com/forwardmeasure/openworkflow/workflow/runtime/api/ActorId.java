package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/** Stable actor DID supplied by the integrating identity and persistence layer. */
public record ActorId(Did value) implements Comparable<ActorId> {
  public ActorId {
    Objects.requireNonNull(value, "value");
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static ActorId parse(String value) {
    return new ActorId(Did.parse(value));
  }

  @Override
  public int compareTo(ActorId other) {
    return value.compareTo(other.value);
  }

  @JsonValue
  @Override
  public String toString() {
    return value.toString();
  }
}
