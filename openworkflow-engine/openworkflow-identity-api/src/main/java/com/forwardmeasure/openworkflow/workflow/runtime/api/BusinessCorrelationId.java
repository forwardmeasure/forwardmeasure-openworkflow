package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/** Transport-neutral business correlation identity. */
public record BusinessCorrelationId(@JsonValue String value) {
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public BusinessCorrelationId {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()
        || value.length() > 256
        || value.chars().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException(
          "Business correlation identity must be printable, non-blank text no longer than 256"
              + " characters");
    }
  }

  public static BusinessCorrelationId parse(String value) {
    return new BusinessCorrelationId(value);
  }

  public static BusinessCorrelationId parseNullable(String value) {
    return value == null ? null : parse(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
