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
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.PlanStepKind;
import com.forwardmeasure.openworkflow.definition.SwitchCasePlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Deterministic instruction layout for the accepted sequential task slice. */
final class MilestoneOneProgram {
  sealed interface Instruction
      permits EnterExtension,
          ExtensionGate,
          ExitExtension,
          ExecuteSet,
          ExecuteSwitch,
          EnterDo,
          ExitDo,
          EnterFor,
          ExitFor,
          ExecuteWait,
          ExecuteRaise,
          EnterTry,
          ExitTry,
          ExecuteEmit,
          ExecuteListen,
          ExitListen,
          ExecuteSubworkflow,
          ExecuteHttpCall,
          ExecuteProtocolCall,
          ExitProtocolCall,
          EnterFunction,
          ExitFunction,
          EnterFork,
          ExitFork {
    PlanStep step();

    int next();
  }

  record EnterExtension(PlanStep step, int next, int exit) implements Instruction {}

  record ExtensionGate(PlanStep step, int application, int next, int skippedNext)
      implements Instruction {}

  record ExitExtension(PlanStep step, int next) implements Instruction {}

  record ExecuteSet(PlanStep step, int next) implements Instruction {}

  record SwitchTarget(String condition, int next) {}

  record ExecuteSwitch(PlanStep step, int next, List<SwitchTarget> cases, Integer defaultNext)
      implements Instruction {}

  record EnterDo(PlanStep step, int next, int exit) implements Instruction {}

  record ExitDo(PlanStep step, int next) implements Instruction {}

  record EnterFor(PlanStep step, int next, int exit, int after) implements Instruction {}

  record ExitFor(PlanStep step, int next, int body) implements Instruction {}

  record ExecuteWait(PlanStep step, int next) implements Instruction {}

  record ExecuteRaise(PlanStep step, int next) implements Instruction {}

  record ExecuteEmit(PlanStep step, int next) implements Instruction {}

  record ExecuteListen(PlanStep step, int next, int after) implements Instruction {}

  record ExecuteSubworkflow(PlanStep step, int next) implements Instruction {}

  record ExecuteHttpCall(PlanStep step, int next) implements Instruction {}

  record ExecuteProtocolCall(PlanStep step, int next, int after) implements Instruction {}

  record EnterFunction(PlanStep step, int next, int exit) implements Instruction {}

  record ExitFunction(PlanStep step, int next) implements Instruction {}

  record ExitListen(PlanStep step, int next, int body) implements Instruction {}

  record ExitProtocolCall(PlanStep step, int next, int body) implements Instruction {}

  record EnterTry(PlanStep step, int next, int catchEntry, int successfulExit, int caughtExit)
      implements Instruction {}

  record ExitTry(PlanStep step, int next, boolean caught) implements Instruction {}

  record BranchRange(String name, int start, int end) {}

  record EnterFork(PlanStep step, int next, int exit, List<BranchRange> branches)
      implements Instruction {}

  record ExitFork(PlanStep step, int next) implements Instruction {}

  private final List<Instruction> instructions;

  private MilestoneOneProgram(List<Instruction> instructions) {
    this.instructions = List.copyOf(instructions);
  }

  static MilestoneOneProgram compile(WorkflowPlan plan) {
    Objects.requireNonNull(plan, "plan");
    var mutable = new ArrayList<MutableInstruction>();
    appendScope(plan.steps(), mutable, -1);
    int end = mutable.size();
    var resolved = mutable.stream().map(value -> value.resolve(end)).toList();
    validateForkSlice(resolved);
    return new MilestoneOneProgram(resolved);
  }

  private static void validateForkSlice(List<Instruction> instructions) {
    for (Instruction candidate : instructions) {
      if (!(candidate instanceof EnterFork fork)) continue;
      for (BranchRange branch : fork.branches()) {
        for (int cursor = branch.start(); cursor < branch.end(); cursor++) {
          Instruction lane = instructions.get(cursor);
          if (!(lane instanceof ExecuteSet)
              && !(lane instanceof EnterExtension)
              && !(lane instanceof ExtensionGate)
              && !(lane instanceof ExitExtension)
              && !(lane instanceof ExecuteSwitch)
              && !(lane instanceof EnterDo)
              && !(lane instanceof ExitDo)
              && !(lane instanceof EnterFor)
              && !(lane instanceof ExitFor)
              && !(lane instanceof ExecuteWait)
              && !(lane instanceof ExecuteRaise)
              && !(lane instanceof ExecuteEmit)
              && !(lane instanceof ExecuteListen)
              && !(lane instanceof ExecuteSubworkflow)
              && !(lane instanceof ExecuteHttpCall)
              && !(lane instanceof ExecuteProtocolCall)
              && !(lane instanceof ExitProtocolCall)
              && !(lane instanceof EnterFunction)
              && !(lane instanceof ExitFunction)
              && !(lane instanceof ExitListen)
              && !(lane instanceof EnterTry)
              && !(lane instanceof ExitTry)
              && !(lane instanceof EnterFork)
              && !(lane instanceof ExitFork)) {
            throw new IllegalArgumentException(
                "The current durable fork increment supports"
                    + " set/switch/do/for/wait/raise/try/fork/emit/listen branch tasks; unsupported"
                    + " "
                    + lane.step().kind()
                    + " at "
                    + lane.step().path());
          }
        }
      }
    }
  }

  int size() {
    return instructions.size();
  }

  Instruction instruction(int index) {
    if (index < 0 || index >= instructions.size()) {
      throw new IllegalArgumentException("Instruction is outside the M1 program: " + index);
    }
    return instructions.get(index);
  }

  EnterTry tryScope(String taskPath) {
    return instructions.stream()
        .filter(EnterTry.class::isInstance)
        .map(EnterTry.class::cast)
        .filter(value -> value.step().path().equals(taskPath))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No compiled try scope for " + taskPath));
  }

  static boolean supports(PlanStepKind kind) {
    return kind == PlanStepKind.EXTENSION
        || kind == PlanStepKind.SET
        || kind == PlanStepKind.DO
        || kind == PlanStepKind.SWITCH
        || kind == PlanStepKind.FOR
        || kind == PlanStepKind.WAIT
        || kind == PlanStepKind.RAISE
        || kind == PlanStepKind.TRY
        || kind == PlanStepKind.FORK
        || kind == PlanStepKind.EMIT
        || kind == PlanStepKind.LISTEN
        || kind == PlanStepKind.RUN
        || kind == PlanStepKind.CALL;
  }

  private static void appendScope(
      List<PlanStep> steps, List<MutableInstruction> output, int scopeExit) {
    Map<String, Integer> entries = new HashMap<>();
    var members = new ArrayList<Member>();
    for (PlanStep step : steps) {
      if (!supports(step.kind())) {
        throw new IllegalArgumentException(
            "Milestone 1 cannot execute task kind " + step.kind() + " at " + step.path());
      }
      int entry = output.size();
      if (entries.put(step.name(), entry) != null) {
        throw new IllegalArgumentException("Duplicate task name in scope: " + step.name());
      }
      if (step.kind() == PlanStepKind.EXTENSION) {
        int enter = output.size();
        var enterInstruction = new MutableEnterExtension(step);
        output.add(enterInstruction);
        var beforeGates = new ArrayList<MutableExtensionGate>();
        for (int index = 0; index < step.extensionPlan().applications().size(); index++) {
          var gate = new MutableExtensionGate(step, index);
          output.add(gate);
          beforeGates.add(gate);
          gate.next = output.size();
          appendScope(
              step.extensionPlan().applications().get(index).before(),
              output,
              Integer.MIN_VALUE + enter);
          gate.skippedNext = output.size();
        }
        appendScope(List.of(step.extensionPlan().target()), output, Integer.MIN_VALUE + enter);
        var afterGates = new ArrayList<MutableExtensionGate>();
        for (int index = 0; index < step.extensionPlan().applications().size(); index++) {
          var gate = new MutableExtensionGate(step, index);
          output.add(gate);
          afterGates.add(gate);
          gate.next = output.size();
          appendScope(
              step.extensionPlan().applications().get(index).after(),
              output,
              Integer.MIN_VALUE + enter);
          gate.skippedNext = output.size();
        }
        int exit = output.size();
        output.add(new MutableExitExtension(step));
        enterInstruction.next = enter + 1;
        enterInstruction.exit = exit;
        replaceTarget(output, Integer.MIN_VALUE + enter, exit);
        members.add(new Member(step, entry, exit));
      } else if (step.kind() == PlanStepKind.SET) {
        output.add(new MutableSet(step));
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.WAIT) {
        output.add(new MutableWait(step));
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.RAISE) {
        output.add(new MutableRaise(step));
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.EMIT) {
        output.add(new MutableEmit(step));
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.LISTEN) {
        var listen = new MutableListen(step);
        output.add(listen);
        if (step.listenPlan().foreach()) {
          listen.next = entry + 1;
          appendScope(step.children(), output, exitTarget(entry));
          int exit = output.size();
          var exitInstruction = new MutableExitListen(step);
          exitInstruction.body = entry + 1;
          output.add(exitInstruction);
          replaceTarget(output, exitTarget(entry), exit);
          members.add(new Member(step, entry, exit));
        } else {
          members.add(new Member(step, entry, entry));
        }
      } else if (step.kind() == PlanStepKind.RUN) {
        if (step.runPlan().kind()
            == com.forwardmeasure.openworkflow.definition.RunPlan.Kind.WORKFLOW) {
          output.add(new MutableSubworkflow(step));
        } else {
          output.add(new MutableProtocolCall(step));
        }
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.CALL) {
        if (step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.FUNCTION
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.HTTP
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.OPEN_API
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.ASYNC_API
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.GRPC
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.A2A
            && step.callPlan().kind()
                != com.forwardmeasure.openworkflow.definition.CallPlan.Kind.MCP) {
          throw new IllegalArgumentException(
              "The current function increment supports only reusable function calls at "
                  + step.path());
        }
        if (step.callPlan().kind()
            == com.forwardmeasure.openworkflow.definition.CallPlan.Kind.FUNCTION) {
          int enter = output.size();
          var enterInstruction = new MutableEnterFunction(step);
          enterInstruction.next = enter + 1;
          output.add(enterInstruction);
          appendScope(step.children(), output, exitTarget(entry));
          int exit = output.size();
          output.add(new MutableExitFunction(step));
          enterInstruction.exit = exit;
          replaceTarget(output, exitTarget(entry), exit);
          members.add(new Member(step, entry, exit));
        } else if (step.callPlan().kind()
                == com.forwardmeasure.openworkflow.definition.CallPlan.Kind.HTTP
            || step.callPlan().kind()
                == com.forwardmeasure.openworkflow.definition.CallPlan.Kind.OPEN_API) {
          output.add(new MutableHttpCall(step));
          members.add(new Member(step, entry, entry));
        } else {
          var protocol = new MutableProtocolCall(step);
          output.add(protocol);
          if (step.callPlan() != null
              && step.callPlan().asyncApiSubscription() != null
              && step.callPlan().asyncApiSubscription().foreach()) {
            protocol.next = entry + 1;
            appendScope(step.children(), output, exitTarget(entry));
            int exit = output.size();
            var exitInstruction = new MutableExitProtocolCall(step);
            exitInstruction.body = entry + 1;
            output.add(exitInstruction);
            replaceTarget(output, exitTarget(entry), exit);
            members.add(new Member(step, entry, exit));
          } else {
            members.add(new Member(step, entry, entry));
          }
        }
      } else if (step.kind() == PlanStepKind.SWITCH) {
        output.add(new MutableSwitch(step));
        members.add(new Member(step, entry, entry));
      } else if (step.kind() == PlanStepKind.DO) {
        int enter = output.size();
        var enterInstruction = new MutableEnterDo(step);
        enterInstruction.next = enter + 1;
        output.add(enterInstruction);
        appendScope(step.children(), output, exitTarget(entry));
        int exit = output.size();
        output.add(new MutableExitDo(step));
        ((MutableEnterDo) output.get(enter)).exit = exit;
        replaceTarget(output, exitTarget(entry), exit);
        members.add(new Member(step, entry, exit));
      } else if (step.kind() == PlanStepKind.FOR) {
        int enter = output.size();
        var enterInstruction = new MutableEnterFor(step);
        enterInstruction.next = enter + 1;
        output.add(enterInstruction);
        appendScope(step.children(), output, exitTarget(entry));
        int exit = output.size();
        var exitInstruction = new MutableExitFor(step);
        exitInstruction.body = enter + 1;
        output.add(exitInstruction);
        enterInstruction.exit = exit;
        replaceTarget(output, exitTarget(entry), exit);
        members.add(new Member(step, entry, exit));
      } else if (step.kind() == PlanStepKind.TRY) {
        int enter = output.size();
        var enterInstruction = new MutableEnterTry(step);
        enterInstruction.next = enter + 1;
        output.add(enterInstruction);
        appendScope(step.tryPlan().steps(), output, exitTarget(entry));
        int successfulExit = output.size();
        output.add(new MutableExitTry(step, false));
        int catchEntry = output.size();
        appendScope(step.tryPlan().catchPlan().steps(), output, catchExitTarget(entry));
        int caughtExit = output.size();
        output.add(new MutableExitTry(step, true));
        enterInstruction.catchEntry = catchEntry;
        enterInstruction.successfulExit = successfulExit;
        enterInstruction.caughtExit = caughtExit;
        replaceTarget(output, exitTarget(entry), successfulExit);
        replaceTarget(output, catchExitTarget(entry), caughtExit);
        members.add(new Member(step, entry, successfulExit, caughtExit));
      } else {
        int enter = output.size();
        var enterInstruction = new MutableEnterFork(step);
        output.add(enterInstruction);
        var branches = new ArrayList<MutableBranchRange>();
        for (PlanStep branch : step.children()) {
          int start = output.size();
          appendScope(List.of(branch), output, exitTarget(entry));
          replaceTarget(output, exitTarget(entry), output.size());
          branches.add(new MutableBranchRange(branch.name(), start, output.size()));
        }
        int exit = output.size();
        output.add(new MutableExitFork(step));
        enterInstruction.next = branches.getFirst().start();
        enterInstruction.exit = exit;
        enterInstruction.branches = List.copyOf(branches);
        members.add(new Member(step, entry, exit));
      }
    }
    for (int index = 0; index < members.size(); index++) {
      Member member = members.get(index);
      int continuation =
          index + 1 < members.size() ? members.get(index + 1).entry() : output.size();
      int next = resolveThen(member.step(), member.completion(), continuation, entries, scopeExit);
      output.get(member.completion()).next = next;
      if (member.caughtCompletion() != null) {
        String catchThen = member.step().tryPlan().catchPlan().thenDirective();
        output.get(member.caughtCompletion()).next =
            catchThen == null
                ? next
                : resolveDirective(
                    catchThen, member.caughtCompletion(), continuation, entries, scopeExit);
      }
      if (output.get(member.entry()) instanceof MutableEnterFor loop) {
        loop.after = next;
      }
      if (output.get(member.entry()) instanceof MutableListen listen) {
        listen.after = next;
      }
      if (output.get(member.entry()) instanceof MutableProtocolCall protocol) {
        protocol.after = next;
      }
      if (output.get(member.completion()) instanceof MutableSwitch selected) {
        selected.resolveCases(member.completion(), continuation, entries, scopeExit);
      }
    }
  }

  private static int exitTarget(int entry) {
    return Integer.MIN_VALUE + entry;
  }

  private static int catchExitTarget(int entry) {
    return Integer.MAX_VALUE - entry;
  }

  private static void replaceTarget(List<MutableInstruction> output, int target, int replacement) {
    for (MutableInstruction instruction : output) {
      if (instruction.next == target) instruction.next = replacement;
      instruction.replaceTarget(target, replacement);
    }
  }

  private static int resolveThen(
      PlanStep step,
      int completion,
      int continuation,
      Map<String, Integer> entries,
      int scopeExit) {
    return switch (step.dataFlow().thenDirective()) {
      case "continue" -> continuation;
      case "end" -> -1;
      case "exit" -> scopeExit;
      default -> {
        Integer target = entries.get(step.dataFlow().thenDirective());
        if (target == null) {
          throw new IllegalArgumentException(
              "Task flow target is outside its scope at " + step.path());
        }
        yield target;
      }
    };
  }

  private static int resolveDirective(
      String directive,
      int completion,
      int continuation,
      Map<String, Integer> entries,
      int scopeExit) {
    return switch (directive) {
      case "continue" -> continuation;
      case "end" -> -1;
      case "exit" -> scopeExit;
      default -> {
        Integer target = entries.get(directive);
        if (target == null) {
          throw new IllegalArgumentException("Task flow target is outside its scope: " + directive);
        }
        yield target;
      }
    };
  }

  private record Member(PlanStep step, int entry, int completion, Integer caughtCompletion) {
    Member(PlanStep step, int entry, int completion) {
      this(step, entry, completion, null);
    }
  }

  private abstract static class MutableInstruction {
    final PlanStep step;
    int next;

    MutableInstruction(PlanStep step) {
      this.step = step;
    }

    abstract Instruction resolve(int end);

    int resolvedNext(int end) {
      return next < 0 ? end : next;
    }

    void replaceTarget(int target, int replacement) {}
  }

  private static final class MutableEnterExtension extends MutableInstruction {
    int exit;

    MutableEnterExtension(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterExtension(step, next, exit);
    }
  }

  private static final class MutableExtensionGate extends MutableInstruction {
    final int application;
    int skippedNext;

    MutableExtensionGate(PlanStep step, int application) {
      super(step);
      this.application = application;
    }

    @Override
    Instruction resolve(int end) {
      return new ExtensionGate(
          step, application, resolvedNext(end), skippedNext < 0 ? end : skippedNext);
    }

    @Override
    void replaceTarget(int target, int replacement) {
      if (skippedNext == target) skippedNext = replacement;
    }
  }

  private static final class MutableExitExtension extends MutableInstruction {
    MutableExitExtension(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitExtension(step, resolvedNext(end));
    }
  }

  private static final class MutableSet extends MutableInstruction {
    MutableSet(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteSet(step, resolvedNext(end));
    }
  }

  private static final class MutableEnterFunction extends MutableInstruction {
    int exit;

    MutableEnterFunction(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterFunction(step, resolvedNext(end), exit);
    }
  }

  private static final class MutableExitFunction extends MutableInstruction {
    MutableExitFunction(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitFunction(step, resolvedNext(end));
    }
  }

  private static final class MutableSwitch extends MutableInstruction {
    List<SwitchTarget> cases = List.of();
    Integer defaultNext;

    MutableSwitch(PlanStep step) {
      super(step);
    }

    void resolveCases(
        int completion, int continuation, Map<String, Integer> entries, int scopeExit) {
      var resolved = new ArrayList<SwitchTarget>();
      for (SwitchCasePlan switchCase : step.switchCases()) {
        int target =
            resolveDirective(
                switchCase.thenDirective(), completion, continuation, entries, scopeExit);
        if (switchCase.defaultCase()) defaultNext = target;
        else resolved.add(new SwitchTarget(switchCase.condition(), target));
      }
      cases = List.copyOf(resolved);
    }

    @Override
    Instruction resolve(int end) {
      var resolvedCases =
          cases.stream()
              .map(
                  value ->
                      new SwitchTarget(value.condition(), value.next() < 0 ? end : value.next()))
              .toList();
      Integer resolvedDefault = defaultNext == null ? null : defaultNext < 0 ? end : defaultNext;
      return new ExecuteSwitch(step, resolvedNext(end), resolvedCases, resolvedDefault);
    }
  }

  private static final class MutableWait extends MutableInstruction {
    MutableWait(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteWait(step, resolvedNext(end));
    }
  }

  private static final class MutableRaise extends MutableInstruction {
    MutableRaise(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteRaise(step, resolvedNext(end));
    }
  }

  private static final class MutableEmit extends MutableInstruction {
    MutableEmit(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteEmit(step, resolvedNext(end));
    }
  }

  private static final class MutableListen extends MutableInstruction {
    int after;

    MutableListen(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteListen(step, resolvedNext(end), after < 0 ? end : after);
    }
  }

  private static final class MutableSubworkflow extends MutableInstruction {
    MutableSubworkflow(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteSubworkflow(step, resolvedNext(end));
    }
  }

  private static final class MutableHttpCall extends MutableInstruction {
    MutableHttpCall(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteHttpCall(step, resolvedNext(end));
    }
  }

  private static final class MutableProtocolCall extends MutableInstruction {
    int after;

    MutableProtocolCall(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExecuteProtocolCall(step, resolvedNext(end), after < 0 ? end : after);
    }
  }

  private static final class MutableExitProtocolCall extends MutableInstruction {
    int body;

    MutableExitProtocolCall(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitProtocolCall(step, resolvedNext(end), body);
    }
  }

  private static final class MutableExitListen extends MutableInstruction {
    int body;

    MutableExitListen(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitListen(step, resolvedNext(end), body);
    }
  }

  private static final class MutableEnterDo extends MutableInstruction {
    int exit;

    MutableEnterDo(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterDo(step, next, exit);
    }
  }

  private static final class MutableExitDo extends MutableInstruction {
    MutableExitDo(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitDo(step, resolvedNext(end));
    }
  }

  private static final class MutableEnterFor extends MutableInstruction {
    int exit;
    int after;

    MutableEnterFor(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterFor(step, next, exit, after < 0 ? end : after);
    }
  }

  private static final class MutableExitFor extends MutableInstruction {
    int body;

    MutableExitFor(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitFor(step, resolvedNext(end), body);
    }
  }

  private static final class MutableEnterTry extends MutableInstruction {
    int catchEntry;
    int successfulExit;
    int caughtExit;

    MutableEnterTry(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterTry(step, next, catchEntry, successfulExit, caughtExit);
    }
  }

  private static final class MutableExitTry extends MutableInstruction {
    private final boolean caught;

    MutableExitTry(PlanStep step, boolean caught) {
      super(step);
      this.caught = caught;
    }

    @Override
    Instruction resolve(int end) {
      return new ExitTry(step, resolvedNext(end), caught);
    }
  }

  private record MutableBranchRange(String name, int start, int end) {
    BranchRange resolve() {
      return new BranchRange(name, start, end);
    }
  }

  private static final class MutableEnterFork extends MutableInstruction {
    int exit;
    List<MutableBranchRange> branches = List.of();

    MutableEnterFork(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new EnterFork(
          step, next, exit, branches.stream().map(MutableBranchRange::resolve).toList());
    }
  }

  private static final class MutableExitFork extends MutableInstruction {
    MutableExitFork(PlanStep step) {
      super(step);
    }

    @Override
    Instruction resolve(int end) {
      return new ExitFork(step, resolvedNext(end));
    }
  }
}
