package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable stack identifying the next durable workflow cutpoint. */
public record ExecutionCursor(List<ExecutionFrame> frames) {

  public ExecutionCursor {
    frames = List.copyOf(Objects.requireNonNull(frames, "frames"));
    if (!frames.isEmpty() && frames.getFirst().taskPath() != null) {
      throw new IllegalArgumentException("The first execution frame must be the workflow root");
    }
    for (int index = 1; index < frames.size(); index++) {
      if (frames.get(index).taskPath() == null) {
        throw new IllegalArgumentException(
            "Only the first execution frame may be the workflow root");
      }
    }
  }

  public static ExecutionCursor start(DataReference input) {
    return new ExecutionCursor(List.of(ExecutionFrame.root(input)));
  }

  public boolean complete() {
    return frames.isEmpty();
  }

  public ExecutionFrame current() {
    if (frames.isEmpty()) {
      throw new IllegalStateException("A completed cursor has no current frame");
    }
    return frames.getLast();
  }

  public ExecutionCursor replaceCurrent(ExecutionFrame frame) {
    Objects.requireNonNull(frame, "frame");
    if (frames.isEmpty()) {
      throw new IllegalStateException("Cannot replace a completed cursor");
    }
    var changed = new ArrayList<>(frames);
    changed.set(changed.size() - 1, frame);
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor enter(String taskPath, DataReference rawInput, DataReference input) {
    var changed = new ArrayList<>(frames);
    changed.add(new ExecutionFrame(taskPath, 0, rawInput, input));
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor enterFor(
      String taskPath, DataReference rawInput, DataReference input, DataReference collection) {
    var changed = new ArrayList<>(frames);
    changed.add(
        new ExecutionFrame(
            taskPath, 0, rawInput, input, new ForIterationState(collection, 0, input), null, null));
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor enterTry(
      String taskPath, DataReference rawInput, DataReference input, java.time.Instant startedAt) {
    var changed = new ArrayList<>(frames);
    changed.add(ExecutionFrame.enteredTry(taskPath, rawInput, input, startedAt));
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor enterExtension(
      String taskPath,
      DataReference rawInput,
      DataReference input,
      ExtensionRuntimeState extensionState) {
    var changed = new ArrayList<>(frames);
    changed.add(ExecutionFrame.enteredExtension(taskPath, rawInput, input, extensionState));
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor truncateAndReplace(int frameIndex, ExecutionFrame frame) {
    if (frameIndex < 0 || frameIndex >= frames.size()) {
      throw new IllegalArgumentException("frameIndex is outside the cursor");
    }
    var changed = new ArrayList<>(frames.subList(0, frameIndex + 1));
    changed.set(frameIndex, Objects.requireNonNull(frame, "frame"));
    return new ExecutionCursor(changed);
  }

  public ExecutionCursor exit() {
    if (frames.isEmpty()) {
      throw new IllegalStateException("Cannot exit a completed cursor");
    }
    return new ExecutionCursor(frames.subList(0, frames.size() - 1));
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
