import { describe, expect, it } from "vitest";
import {
  applyWorkflowSettings,
  fromYaml,
  parseWorkflowSettings,
  setChildrenAtPath,
  tasksAtPath,
  toYaml,
  UnsupportedTaskError,
  type Task,
} from "./dsl";
import { SAMPLE } from "../workflow";

describe("canvas <-> Serverless Workflow DSL conversion", () => {
  it("parses the studio sample into an editable set task", () => {
    const graph = fromYaml(SAMPLE);
    expect(graph.tasks).toEqual([
      {
        kind: "set",
        name: "greet",
        set: { message: "Hello from OpenWorkflow Studio" },
      },
    ]);
  });

  it("parses call tasks with their target and parameters", () => {
    const source = [
      "do:",
      "  - fetchPet:",
      "      call: http",
      "      with:",
      "        method: get",
      "        endpoint: https://example.com/pet",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "call",
        name: "fetchPet",
        call: "http",
        with: { method: "get", endpoint: "https://example.com/pet" },
      },
    ]);
  });

  it("round-trips a graph back into valid Serverless Workflow DSL, preserving document metadata", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        { kind: "set", name: "greet", set: { message: "Updated" } },
        {
          kind: "call",
          name: "notify",
          call: "http",
          with: { endpoint: "https://example.com" },
        },
      ],
    });
    const reparsed = fromYaml(rewritten);
    expect(reparsed.tasks).toEqual([
      { kind: "set", name: "greet", set: { message: "Updated" } },
      {
        kind: "call",
        name: "notify",
        call: "http",
        with: { endpoint: "https://example.com" },
      },
    ]);
    // document metadata (dsl/namespace/name/version) must survive a round trip untouched -
    // toYaml() only replaces the "do:" key, it doesn't regenerate the whole document.
    expect(rewritten).toContain("namespace: forwardmeasure");
    expect(rewritten).toContain("name: hello-studio");
  });

  it("parses switch tasks with named cases, conditions, and targets", () => {
    const source = [
      "do:",
      "  - checkAge:",
      "      switch:",
      "        - adult:",
      "            when: ${ .age >= 18 }",
      "            then: notifyAdult",
      "        - default:",
      "            then: exit",
      "  - notifyAdult:",
      "      set:",
      "        message: welcome",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "switch",
        name: "checkAge",
        cases: [
          { name: "adult", when: "${ .age >= 18 }", then: "notifyAdult" },
          { name: "default", when: undefined, then: "exit" },
        ],
      },
      { kind: "set", name: "notifyAdult", set: { message: "welcome" } },
    ]);
  });

  it("round-trips a switch task's cases", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "switch",
          name: "route",
          cases: [
            { name: "vip", when: '${ .tier == "vip" }', then: "exit" },
            { name: "default", then: "exit" },
          ],
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "switch",
        name: "route",
        cases: [
          { name: "vip", when: '${ .tier == "vip" }', then: "exit" },
          { name: "default", when: undefined, then: "exit" },
        ],
      },
    ]);
  });

  it('rejects a switch case with no "then" (positional fallthrough isn\'t modeled here)', () => {
    const source = "do:\n  - branch:\n      switch:\n        - default: {}\n";
    expect(() => fromYaml(source)).toThrow(UnsupportedTaskError);
  });

  it("parses a task's common cross-cutting properties alongside its kind-specific fields", () => {
    const source = [
      "do:",
      "  - greet:",
      "      if: ${ .enabled }",
      "      input:",
      "        schema: {}",
      "      timeout: PT30S",
      "      then: notify",
      "      metadata:",
      "        owner: team-a",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "set",
        name: "greet",
        set: { message: "hi" },
        if: "${ .enabled }",
        input: { schema: {} },
        timeout: "PT30S",
        then: "notify",
        metadata: { owner: "team-a" },
      },
    ]);
  });

  it("preserves a non-switch task's own \"then\" through a canvas round-trip (previously silently dropped)", () => {
    // Ground truth: OpenWorkflowCompiler's taskDataFlow() reads task.path("then")
    // for every task kind, not just "switch" cases - a set/call/emit/etc. task
    // can name an explicit next step, overriding positional fall-through. Before
    // CommonTaskProps carried "then", toYaml() rewriting "do:" from the parsed
    // graph would silently drop this on any edit made through the canvas.
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        { kind: "set", name: "greet", set: { message: "hi" }, then: "notify" },
        { kind: "set", name: "notify", set: { message: "bye" } },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      { kind: "set", name: "greet", set: { message: "hi" }, then: "notify" },
      { kind: "set", name: "notify", set: { message: "bye" } },
    ]);
  });

  it("keeps a switch task's own \"then\" separate from its cases' per-case \"then\"", () => {
    const source = [
      "do:",
      "  - route:",
      "      then: fallback",
      "      switch:",
      "        - toA:",
      "            when: ${ .a }",
      "            then: taskA",
      "",
    ].join("\n");
    const tasks = fromYaml(source).tasks;
    expect(tasks).toEqual([
      {
        kind: "switch",
        name: "route",
        then: "fallback",
        cases: [{ name: "toA", when: "${ .a }", then: "taskA" }],
      },
    ]);
  });

  it("round-trips a task's common cross-cutting properties, omitting unset ones", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "call",
          name: "fetchPet",
          call: "http",
          with: { endpoint: "https://example.com" },
          if: "${ .fetch }",
          timeout: "PT10S",
          export: { as: "$context" },
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "call",
        name: "fetchPet",
        call: "http",
        with: { endpoint: "https://example.com" },
        if: "${ .fetch }",
        timeout: "PT10S",
        export: { as: "$context" },
      },
    ]);
    expect(rewritten).not.toContain("input:");
    expect(rewritten).not.toContain("output:");
    expect(rewritten).not.toContain("metadata:");
  });

  it("parses raise tasks with their Problem Details error fields", () => {
    const source = [
      "do:",
      "  - fail:",
      "      raise:",
      "        error:",
      "          type: https://example.com/errors/not-found",
      "          status: 404",
      "          title: Not Found",
      "          instance: /pets/1",
      "          detail: No pet with that id",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "raise",
        name: "fail",
        error: {
          type: "https://example.com/errors/not-found",
          status: 404,
          title: "Not Found",
          instance: "/pets/1",
          detail: "No pet with that id",
        },
      },
    ]);
  });

  it("parses a raise task whose error is a plain string naming a use.errors entry", () => {
    const source = "do:\n  - fail:\n      raise:\n        error: notFound\n";
    expect(fromYaml(source).tasks).toEqual([
      { kind: "raise", name: "fail", error: "notFound" },
    ]);
  });

  it("round-trips a raise error reference", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [{ kind: "raise", name: "fail", error: "notFound" }],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      { kind: "raise", name: "fail", error: "notFound" },
    ]);
  });

  it("parses an inline raise error with no title (only type/status are required)", () => {
    const source = [
      "do:",
      "  - fail:",
      "      raise:",
      "        error:",
      "          type: https://example.com/errors/bad",
      "          status: 400",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "raise",
        name: "fail",
        error: { type: "https://example.com/errors/bad", status: 400 },
      },
    ]);
  });

  it("rejects an inline raise error with no type/status", () => {
    const source = "do:\n  - fail:\n      raise:\n        error: {}\n";
    expect(() => fromYaml(source)).toThrow(UnsupportedTaskError);
  });

  it("round-trips a raise task, omitting unset optional error fields", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "raise",
          name: "fail",
          error: { type: "https://example.com/errors/bad", status: 400, title: "Bad" },
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "raise",
        name: "fail",
        error: { type: "https://example.com/errors/bad", status: 400, title: "Bad" },
      },
    ]);
    expect(rewritten).not.toContain("instance:");
    expect(rewritten).not.toContain("detail:");
  });

  it("parses wait tasks with a plain duration string", () => {
    const source = "do:\n  - pause:\n      wait: PT30S\n";
    expect(fromYaml(source).tasks).toEqual([
      { kind: "wait", name: "pause", wait: "PT30S" },
    ]);
  });

  it("round-trips a wait task's duration", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [{ kind: "wait", name: "pause", wait: "PT1M" }],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      { kind: "wait", name: "pause", wait: "PT1M" },
    ]);
  });

  it("parses emit tasks, reading the event.with properties", () => {
    const source = [
      "do:",
      "  - notify:",
      "      emit:",
      "        event:",
      "          with:",
      "            source: /orders",
      "            type: com.example.order.created",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "emit",
        name: "notify",
        with: { source: "/orders", type: "com.example.order.created" },
      },
    ]);
  });

  it("round-trips an emit task's event properties", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "emit",
          name: "notify",
          with: { source: "/orders", type: "com.example.order.created" },
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "emit",
        name: "notify",
        with: { source: "/orders", type: "com.example.order.created" },
      },
    ]);
  });

  it("parses a do task's nested children recursively, including doubly-nested groups", () => {
    const source = [
      "do:",
      "  - outer:",
      "      do:",
      "        - inner:",
      "            do:",
      "              - leaf:",
      "                  set:",
      "                    message: hi",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "do",
        name: "outer",
        children: [
          {
            kind: "do",
            name: "inner",
            children: [
              { kind: "set", name: "leaf", set: { message: "hi" } },
            ],
          },
        ],
      },
    ]);
  });

  it("round-trips a do task's children, preserving nesting", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "do",
          name: "group",
          children: [
            { kind: "set", name: "step1", set: { a: 1 } },
            { kind: "call", name: "step2", call: "http", with: {} },
          ],
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "do",
        name: "group",
        children: [
          { kind: "set", name: "step1", set: { a: 1 } },
          { kind: "call", name: "step2", call: "http", with: {} },
        ],
      },
    ]);
  });

  it("parses a for task's loop fields and its nested loop body", () => {
    const source = [
      "do:",
      "  - retry:",
      "      for:",
      "        each: pet",
      "        in: ${ .pets }",
      "        at: index",
      "      while: ${ .continue }",
      "      do:",
      "        - notify:",
      "            set:",
      "              message: hi",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "for",
        name: "retry",
        itemVariable: "pet",
        collection: "${ .pets }",
        indexVariable: "index",
        whileCondition: "${ .continue }",
        children: [{ kind: "set", name: "notify", set: { message: "hi" } }],
      },
    ]);
  });

  it("parses a for task with no loop body as an empty children list", () => {
    const source =
      "do:\n  - retry:\n      for:\n        each: item\n        in: ${ .items }\n";
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "for",
        name: "retry",
        itemVariable: "item",
        collection: "${ .items }",
        indexVariable: undefined,
        whileCondition: undefined,
        children: [],
      },
    ]);
  });

  it("round-trips a for task, omitting unset optional fields", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "for",
          name: "retry",
          itemVariable: "pet",
          collection: "${ .pets }",
          children: [{ kind: "set", name: "notify", set: { message: "hi" } }],
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "for",
        name: "retry",
        itemVariable: "pet",
        collection: "${ .pets }",
        indexVariable: undefined,
        whileCondition: undefined,
        children: [{ kind: "set", name: "notify", set: { message: "hi" } }],
      },
    ]);
    expect(rewritten).not.toContain("at:");
    expect(rewritten).not.toContain("while:");
  });

  it("parses a fork task's branches (structurally a task list, same as do)", () => {
    const source = [
      "do:",
      "  - branch:",
      "      fork:",
      "        compete: true",
      "        branches:",
      "          - left:",
      "              set:",
      "                a: 1",
      "          - right:",
      "              call: http",
      "              with: {}",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "fork",
        name: "branch",
        compete: true,
        children: [
          { kind: "set", name: "left", set: { a: 1 } },
          { kind: "call", name: "right", call: "http", with: {} },
        ],
      },
    ]);
  });

  it("parses a fork task with compete omitted as false", () => {
    const source =
      "do:\n  - branch:\n      fork:\n        branches:\n          - only:\n              set: {}\n";
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "fork",
        name: "branch",
        compete: false,
        children: [{ kind: "set", name: "only", set: {} }],
      },
    ]);
  });

  it("round-trips a fork task's branches and compete flag", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "fork",
          name: "branch",
          compete: true,
          children: [
            { kind: "set", name: "left", set: { a: 1 } },
            { kind: "set", name: "right", set: { b: 2 } },
          ],
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "fork",
        name: "branch",
        compete: true,
        children: [
          { kind: "set", name: "left", set: { a: 1 } },
          { kind: "set", name: "right", set: { b: 2 } },
        ],
      },
    ]);
  });

  it('omits "compete" from the serialized YAML when false (the default)', () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "fork",
          name: "branch",
          compete: false,
          children: [{ kind: "set", name: "only", set: {} }],
        },
      ],
    });
    expect(rewritten).not.toContain("compete:");
  });

  it("parses a try task's block and a fully inline catch clause", () => {
    const source = [
      "do:",
      "  - tryGetPet:",
      "      try:",
      "        - getPet:",
      "            call: http",
      "            with:",
      "              method: get",
      "              endpoint: https://example.com/pet",
      "      catch:",
      "        errors:",
      "          with:",
      "            status: 503",
      "        as: err",
      "        when: ${ .retryable }",
      "        exceptWhen: ${ .fatal }",
      "        retry:",
      "          delay:",
      "            seconds: 3",
      "          backoff:",
      "            exponential: {}",
      "          limit:",
      "            attempt:",
      "              count: 5",
      "              duration: PT30S",
      "            duration: PT2M",
      "          jitter:",
      "            from: PT1S",
      "            to: PT3S",
      "        then: exit",
      "        do:",
      "          - notifySupport:",
      "              emit:",
      "                event:",
      "                  with:",
      "                    type: com.example.failure",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "try",
        name: "tryGetPet",
        children: [
          {
            kind: "call",
            name: "getPet",
            call: "http",
            with: { method: "get", endpoint: "https://example.com/pet" },
          },
        ],
        catchClause: {
          errors: {
            type: undefined,
            status: 503,
            instance: undefined,
            title: undefined,
            detail: undefined,
          },
          as: "err",
          when: "${ .retryable }",
          exceptWhen: "${ .fatal }",
          retry: {
            delay: { seconds: 3 },
            backoff: "exponential",
            attemptCount: 5,
            attemptDuration: "PT30S",
            totalDuration: "PT2M",
            jitterFrom: "PT1S",
            jitterTo: "PT3S",
            when: undefined,
            exceptWhen: undefined,
          },
          then: "exit",
          children: [
            {
              kind: "emit",
              name: "notifySupport",
              with: { type: "com.example.failure" },
            },
          ],
        },
      },
    ]);
  });

  it("parses a try task with a named (reusable) retry policy reference", () => {
    const source = [
      "do:",
      "  - tryGetPet:",
      "      try:",
      "        - getPet:",
      "            call: http",
      "            with: {}",
      "      catch:",
      "        retry: default",
      "",
    ].join("\n");
    const tasks = fromYaml(source).tasks;
    expect(tasks[0].kind).toBe("try");
    expect((tasks[0] as { catchClause: { retry: unknown } }).catchClause.retry).toBe(
      "default",
    );
  });

  it("parses a try task with a minimal catch clause (no errors/retry)", () => {
    const source =
      "do:\n  - step:\n      try:\n        - inner:\n            set: {}\n      catch: {}\n";
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "try",
        name: "step",
        children: [{ kind: "set", name: "inner", set: {} }],
        catchClause: {
          errors: undefined,
          as: undefined,
          when: undefined,
          exceptWhen: undefined,
          retry: undefined,
          then: undefined,
          children: [],
        },
      },
    ]);
  });

  it("round-trips a try task's block and inline retry policy, omitting a default (constant) backoff", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "try",
          name: "step",
          children: [{ kind: "set", name: "inner", set: {} }],
          catchClause: {
            errors: { status: 503 },
            as: "err",
            retry: { backoff: "constant", attemptCount: 3 },
            children: [],
          },
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "try",
        name: "step",
        children: [{ kind: "set", name: "inner", set: {} }],
        catchClause: {
          errors: {
            type: undefined,
            status: 503,
            instance: undefined,
            title: undefined,
            detail: undefined,
          },
          as: "err",
          when: undefined,
          exceptWhen: undefined,
          retry: {
            delay: undefined,
            backoff: "constant",
            attemptCount: 3,
            attemptDuration: undefined,
            totalDuration: undefined,
            jitterFrom: undefined,
            jitterTo: undefined,
            when: undefined,
            exceptWhen: undefined,
          },
          then: undefined,
          children: [],
        },
      },
    ]);
    expect(rewritten).not.toContain("backoff:");
  });

  it("round-trips a named retry policy reference as a plain string", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "try",
          name: "step",
          children: [],
          catchClause: { retry: "default", children: [] },
        },
      ],
    });
    expect(rewritten).toContain("retry: default");
  });

  it("parses a plain listen task (no foreach) with an empty children list", () => {
    const source =
      "do:\n  - step:\n      listen:\n        to:\n          one:\n            with:\n              type: com.example.event\n        read: envelope\n";
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "listen",
        name: "step",
        consumption: { one: { with: { type: "com.example.event" } } },
        readAs: "envelope",
        itemVariable: undefined,
        indexVariable: undefined,
        children: [],
      },
    ]);
  });

  it("parses a listen task with a sibling foreach (not nested inside listen)", () => {
    const source = [
      "do:",
      "  - step:",
      "      listen:",
      "        to:",
      "          any: []",
      "      foreach:",
      "        item: event",
      "        at: i",
      "        do:",
      "          - handle:",
      "              set:",
      "                seen: true",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "listen",
        name: "step",
        consumption: { any: [] },
        readAs: undefined,
        itemVariable: "event",
        indexVariable: "i",
        children: [{ kind: "set", name: "handle", set: { seen: true } }],
      },
    ]);
  });

  it("round-trips a listen task's foreach loop, omitting an absent foreach entirely", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "listen",
          name: "step",
          consumption: { one: { with: { type: "x" } } },
          children: [],
        },
      ],
    });
    expect(rewritten).not.toContain("foreach:");
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "listen",
        name: "step",
        consumption: { one: { with: { type: "x" } } },
        readAs: undefined,
        itemVariable: undefined,
        indexVariable: undefined,
        children: [],
      },
    ]);
  });

  it("parses each run variant (container/script/shell/workflow) with await/return", () => {
    const source = [
      "do:",
      "  - runIt:",
      "      run:",
      "        container:",
      "          image: alpine",
      "          arguments: [Foo, Bar]",
      "        await: false",
      "        return: all",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "run",
        name: "runIt",
        variant: "container",
        configuration: { image: "alpine", arguments: ["Foo", "Bar"] },
        await: false,
        returnMode: "all",
      },
    ]);
  });

  it("parses a run task's workflow variant into structured fields", () => {
    const source = [
      "do:",
      "  - runIt:",
      "      run:",
      "        workflow:",
      "          namespace: test",
      "          name: register-customer",
      "          version: 0.1.0",
      "          input:",
      "            customer: .user",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "run",
        name: "runIt",
        variant: "workflow",
        workflowNamespace: "test",
        workflowName: "register-customer",
        workflowVersion: "0.1.0",
        workflowInput: { customer: ".user" },
      },
    ]);
  });

  it("round-trips a run task, omitting default await/return", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "run",
          name: "runIt",
          variant: "shell",
          configuration: { command: "echo hi" },
        },
      ],
    });
    expect(rewritten).not.toContain("await:");
    expect(rewritten).not.toContain("return:");
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "run",
        name: "runIt",
        variant: "shell",
        configuration: { command: "echo hi" },
        await: undefined,
        returnMode: undefined,
      },
    ]);
  });

  it("rejects task constructs the canvas doesn't support yet, rather than silently dropping them", () => {
    const source = "do:\n  - step:\n      notAKnownTaskKeyword: {}\n";
    expect(() => fromYaml(source)).toThrow(UnsupportedTaskError);
  });
});

describe("drill-down navigation helpers (tasksAtPath / setChildrenAtPath)", () => {
  const tree: Task[] = [
    { kind: "set", name: "before", set: {} },
    {
      kind: "do",
      name: "group",
      children: [
        { kind: "set", name: "inner1", set: {} },
        {
          kind: "do",
          name: "nested",
          children: [{ kind: "set", name: "deepest", set: {} }],
        },
      ],
    },
  ];

  it("resolves the top-level list for an empty path", () => {
    expect(tasksAtPath(tree, [])).toBe(tree);
  });

  it("resolves a nested list one level down", () => {
    expect(tasksAtPath(tree, ["group"])).toEqual([
      { kind: "set", name: "inner1", set: {} },
      {
        kind: "do",
        name: "nested",
        children: [{ kind: "set", name: "deepest", set: {} }],
      },
    ]);
  });

  it("resolves a doubly-nested list", () => {
    expect(tasksAtPath(tree, ["group", "nested"])).toEqual([
      { kind: "set", name: "deepest", set: {} },
    ]);
  });

  it("returns an empty list for a path that no longer resolves, rather than throwing", () => {
    expect(tasksAtPath(tree, ["doesNotExist"])).toEqual([]);
    expect(tasksAtPath(tree, ["before"])).toEqual([]); // "before" isn't a "do" task
  });

  it("writes a new child list back at a nested path, leaving everything else untouched", () => {
    const updated = setChildrenAtPath(tree, ["group", "nested"], [
      { kind: "set", name: "replaced", set: {} },
    ]);
    expect(tasksAtPath(updated, ["group", "nested"])).toEqual([
      { kind: "set", name: "replaced", set: {} },
    ]);
    // Untouched siblings survive.
    expect(tasksAtPath(updated, ["group"])[0]).toEqual({
      kind: "set",
      name: "inner1",
      set: {},
    });
    expect(updated[0]).toEqual({ kind: "set", name: "before", set: {} });
  });
});

describe("workflow-level settings (parseWorkflowSettings / applyWorkflowSettings)", () => {
  it("parses workflow-level timeout/schedule and every use.* catalog", () => {
    const source = [
      "document:",
      "  dsl: '1.0.0'",
      "  namespace: examples",
      "  name: with-settings",
      "  version: '0.1.0'",
      "timeout: PT1H",
      "schedule:",
      "  cron: 0 0 * * *",
      "use:",
      "  authentications:",
      "    petStoreAuth:",
      "      bearer:",
      "        token: ${ .token }",
      "  errors:",
      "    notFound:",
      "      type: https://example.com/errors/not-found",
      "      status: 404",
      "  extensions:",
      "    - mock:",
      "        extend: call",
      "  retries:",
      "    default:",
      "      backoff:",
      "        exponential: {}",
      "  functions:",
      "    getPetById:",
      "      call: http",
      "      with: {}",
      "  timeouts:",
      "    short: PT5S",
      "  catalogs:",
      "    shared:",
      "      endpoint: https://example.com/catalog",
      "  secrets:",
      "    - petStoreSecret",
      "do:",
      "  - greet:",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    expect(parseWorkflowSettings(source)).toEqual({
      timeout: "PT1H",
      schedule: { cron: "0 0 * * *" },
      evaluateMode: undefined,
      input: undefined,
      output: undefined,
      documentTitle: undefined,
      documentSummary: undefined,
      documentTags: undefined,
      documentMetadata: undefined,
      authentications: {
        petStoreAuth: { bearer: { token: "${ .token }" } },
      },
      errors: {
        notFound: { type: "https://example.com/errors/not-found", status: 404 },
      },
      extensions: [{ mock: { extend: "call" } }],
      retries: { default: { backoff: { exponential: {} } } },
      functions: { getPetById: { call: "http", with: {} } },
      timeouts: { short: "PT5S" },
      catalogs: { shared: { endpoint: "https://example.com/catalog" } },
      secrets: ["petStoreSecret"],
    });
  });

  it("parses a document with no settings as all-undefined, not throwing", () => {
    expect(parseWorkflowSettings(SAMPLE)).toEqual({
      timeout: undefined,
      schedule: undefined,
      evaluateMode: undefined,
      input: undefined,
      output: undefined,
      documentTitle: undefined,
      documentSummary: undefined,
      documentTags: undefined,
      documentMetadata: undefined,
      authentications: undefined,
      errors: undefined,
      extensions: undefined,
      retries: undefined,
      functions: undefined,
      timeouts: undefined,
      catalogs: undefined,
      secrets: undefined,
    });
  });

  it("parses evaluate.mode, normalizing case the same way ExpressionMode.parse() does server-side", () => {
    const withLoose = [
      "document:",
      "  dsl: '1.0.0'",
      "  namespace: examples",
      "  name: with-evaluate",
      "  version: '0.1.0'",
      "evaluate:",
      "  language: jq",
      "  mode: LOOSE",
      "do:",
      "  - greet:",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    expect(parseWorkflowSettings(withLoose).evaluateMode).toBe("loose");
  });

  it("treats a document with no evaluate: block as undefined (the compiler's own default is strict)", () => {
    expect(parseWorkflowSettings(SAMPLE).evaluateMode).toBeUndefined();
  });

  it("round-trips evaluate.mode: loose, and omits evaluate: entirely for strict (the default)", () => {
    const withLoose = applyWorkflowSettings(SAMPLE, { evaluateMode: "loose" });
    expect(parseWorkflowSettings(withLoose).evaluateMode).toBe("loose");

    const backToStrict = applyWorkflowSettings(withLoose, { evaluateMode: "strict" });
    expect(parseWorkflowSettings(backToStrict).evaluateMode).toBeUndefined();
    expect(backToStrict).not.toContain("evaluate:");
  });

  it("parses workflow-level input/output, the same {schema, from/as} shape a task's own input/output carries", () => {
    const source = [
      "document:",
      "  dsl: '1.0.0'",
      "  namespace: examples",
      "  name: with-io",
      "  version: '0.1.0'",
      "input:",
      "  schema:",
      "    document:",
      "      type: object",
      "  from: '${ .payload }'",
      "output:",
      "  as: '${ {result: .} }'",
      "do:",
      "  - greet:",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    const settings = parseWorkflowSettings(source);
    expect(settings.input).toEqual({
      schema: { document: { type: "object" } },
      from: "${ .payload }",
    });
    expect(settings.output).toEqual({ as: "${ {result: .} }" });
  });

  it("round-trips workflow-level input/output via applyWorkflowSettings", () => {
    const rewritten = applyWorkflowSettings(SAMPLE, {
      input: { schema: { document: { type: "object" } } },
      output: { as: "${ . }" },
    });
    const settings = parseWorkflowSettings(rewritten);
    expect(settings.input).toEqual({ schema: { document: { type: "object" } } });
    expect(settings.output).toEqual({ as: "${ . }" });
  });

  it("parses document.title/summary/tags/metadata, distinct from the governance layer's own title", () => {
    const source = [
      "document:",
      "  dsl: '1.0.0'",
      "  namespace: examples",
      "  name: with-doc-metadata",
      "  version: '0.1.0'",
      "  title: Pet Store Onboarding",
      "  summary: Registers a new pet store tenant.",
      "  tags:",
      "    team: platform",
      "  metadata:",
      "    owner: platform-team",
      "do:",
      "  - greet:",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    const settings = parseWorkflowSettings(source);
    expect(settings.documentTitle).toBe("Pet Store Onboarding");
    expect(settings.documentSummary).toBe("Registers a new pet store tenant.");
    expect(settings.documentTags).toEqual({ team: "platform" });
    expect(settings.documentMetadata).toEqual({ owner: "platform-team" });
  });

  it("round-trips document.title/summary/tags/metadata, preserving dsl/namespace/name/version", () => {
    const rewritten = applyWorkflowSettings(SAMPLE, {
      documentTitle: "Hello Studio",
      documentSummary: "A tiny sample workflow.",
      documentTags: { team: "platform" },
      documentMetadata: { owner: "platform-team" },
    });
    const settings = parseWorkflowSettings(rewritten);
    expect(settings.documentTitle).toBe("Hello Studio");
    expect(settings.documentSummary).toBe("A tiny sample workflow.");
    expect(settings.documentTags).toEqual({ team: "platform" });
    expect(settings.documentMetadata).toEqual({ owner: "platform-team" });
    expect(rewritten).toContain("namespace: forwardmeasure");
    expect(rewritten).toContain("name: hello-studio");
    expect(fromYaml(rewritten).tasks).toEqual(fromYaml(SAMPLE).tasks);
  });

  it("clears document.title/summary/tags/metadata without dropping the rest of document:", () => {
    const withMetadata = applyWorkflowSettings(SAMPLE, {
      documentTitle: "Hello Studio",
      documentTags: { team: "platform" },
    });
    const cleared = applyWorkflowSettings(withMetadata, {});
    expect(cleared).not.toContain("title:");
    expect(cleared).not.toContain("tags:");
    expect(cleared).toContain("namespace: forwardmeasure");
    expect(cleared).toContain("name: hello-studio");
  });

  it("round-trips settings while leaving do: and document metadata untouched", () => {
    const rewritten = applyWorkflowSettings(SAMPLE, {
      timeout: "PT1H",
      functions: { getPetById: { call: "http", with: {} } },
    });
    expect(parseWorkflowSettings(rewritten)).toEqual({
      timeout: "PT1H",
      schedule: undefined,
      evaluateMode: undefined,
      input: undefined,
      output: undefined,
      documentTitle: undefined,
      documentSummary: undefined,
      documentTags: undefined,
      documentMetadata: undefined,
      authentications: undefined,
      errors: undefined,
      extensions: undefined,
      retries: undefined,
      functions: { getPetById: { call: "http", with: {} } },
      timeouts: undefined,
      catalogs: undefined,
      secrets: undefined,
    });
    // Document metadata and the task list survive untouched.
    expect(rewritten).toContain("namespace: forwardmeasure");
    expect(rewritten).toContain("name: hello-studio");
    expect(fromYaml(rewritten).tasks).toEqual(fromYaml(SAMPLE).tasks);
  });

  it("omits use: entirely once every catalog is cleared, rather than leaving an empty map", () => {
    const withSettings = applyWorkflowSettings(SAMPLE, {
      functions: { getPetById: { call: "http", with: {} } },
    });
    const cleared = applyWorkflowSettings(withSettings, {});
    expect(cleared).not.toContain("use:");
  });
});
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
