package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A W3C DID Core 1.1 decentralized identifier. This validates generic DID syntax; method-specific
 * validation and resolution belong to adapters.
 */
public record Did(String value) implements Comparable<Did> {
  private static final int MAX_LENGTH = 2_048;
  private static final Pattern SYNTAX =
      Pattern.compile(
          "^did:([a-z0-9]+):"
              + "(?:(?:[A-Za-z0-9._-]|%[0-9A-Fa-f]{2})*:)*"
              + "(?:[A-Za-z0-9._-]|%[0-9A-Fa-f]{2})+$");

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public Did {
    Objects.requireNonNull(value, "value");
    if (value.length() > MAX_LENGTH || !SYNTAX.matcher(value).matches()) {
      throw new IllegalArgumentException("Identifier must conform to W3C DID Core 1.1 syntax");
    }
  }

  public static Did parse(String value) {
    return new Did(value);
  }

  public String method() {
    int start = "did:".length();
    return value.substring(start, value.indexOf(':', start));
  }

  public String methodSpecificId() {
    int start = value.indexOf(':', "did:".length()) + 1;
    return value.substring(start);
  }

  @JsonValue
  @Override
  public String toString() {
    return value;
  }

  @Override
  public int compareTo(Did other) {
    return value.compareTo(other.value);
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
