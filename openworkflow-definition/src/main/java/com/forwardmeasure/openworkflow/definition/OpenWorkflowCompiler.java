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
package com.forwardmeasure.openworkflow.definition;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionException;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Validates against the exact pinned OpenWorkflow 1.0.3 schema and compiles the executable
 * semantics into a transport-free immutable plan.
 */
public final class OpenWorkflowCompiler {
  public static final int MAX_SOURCE_BYTES = 5 * 1024 * 1024;

  /** Exact schema bytes shipped by the pinned official SDK 7.29.0.Final. */
  public static final String SCHEMA_SHA256 =
      "e0cfa77227a9b537be7519041683508151e21dc80aa991dfbf6362dfc36b2331";

  // The one DSL version this build's bundled schema (SCHEMA_RESOURCE, hash-pinned via
  // SCHEMA_SHA256 above) and compilation logic actually target - a real spec version bump
  // (a new SDK release, a new /schema/workflow.yaml, and whatever compileTasks changes that
  // new version's constructs require) is real engineering work this constant doesn't do for
  // you; it exists so that work has exactly one place to start from instead of a string to grep
  // for. NOT the same thing as COMPILER_SHA256 below - that's a deliberately hand-authored,
  // separately-reviewed fingerprint of this compiler's admitted semantics as a whole (dsl
  // version included, but not driven by this constant), not a place this should be substituted
  // into.
  public static final String SUPPORTED_DSL_VERSION = "1.0.3";

  /**
   * Digest of the explicitly versioned compilation profile. Any change to admitted semantics must
   * change the profile text.
   */
  public static final String COMPILER_SHA256 =
      sha256(
          ("forwardmeasure-openworkflow-compiler:v1|dsl:1.0.3|tasks:do,set,switch,for,fork,emit,listen,wait,raise,try,call,run"
               + "|expressions:jq-1.7|data-flow:input,output,export,if,then"
               + "|resources:immutable-external,digest-pinned,transitive-operation-graphs"
               + "|schemas:json,inline,transitive-external|faults:structured-errors,catch,retry"
               + "|duration:iso8601-calendar-and-clock|functions:inline-and-immutable-catalogue"
               + "|extensions:ordered,conditional,once-durable,before,after,scope-exit"
               + "|functions:inline,cycle-safe,durable-frames"
               + "|subflows:tenant-catalog,publication-pinned"
               + "|authentication:declared-secret-references,expression-references-pinned,edge-resolved,mcp-stdio-environment-secret"
               + "|extensions:human-task-custom-function,durable-governed-work"
               + "|contracts:declared-edge-schema-inclusion-fail-closed")
              .getBytes(StandardCharsets.UTF_8));

  private static final String SCHEMA_RESOURCE = "/schema/workflow.yaml";
  private static final Set<String> COMMON_TASK_PROPERTIES =
      Set.of("if", "input", "output", "export", "timeout", "then", "metadata");
  private static final ObjectMapper YAML =
      new ObjectMapper(yamlFactory()).enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final Schema SCHEMA = loadSchema();
  private static final JqRuntimeExpressionEvaluator EXPRESSIONS =
      new JqRuntimeExpressionEvaluator();
  private static final Pattern PROTO_IMPORT =
      Pattern.compile("(?m)^\\s*import\\s+(?:(?:public|weak)\\s+)?" + "\"([^\"]+)\"\\s*;");
  private static final Pattern SECRET_BRACKET_REFERENCE =
      Pattern.compile("\\$secrets\\s*\\[\\s*([\"'])([^\"']+)\\1\\s*]");
  private static final Pattern SECRET_MEMBER_REFERENCE =
      Pattern.compile("\\$secrets\\.([A-Za-z_][A-Za-z0-9_-]*)");

  private static YAMLFactory yamlFactory() {
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setCodePointLimit(MAX_SOURCE_BYTES);
    return YAMLFactory.builder()
        .loaderOptions(loaderOptions)
        .streamReadConstraints(
            StreamReadConstraints.builder()
                .maxDocumentLength(MAX_SOURCE_BYTES)
                .maxStringLength(MAX_SOURCE_BYTES)
                .build())
        .build();
  }

  public WorkflowPlan compile(byte[] source) {
    return compile(source, List.of(), WorkflowDefinitionCatalog.unavailable());
  }

  /** Compiles source using external resource bytes already resolved outside engine execution. */
  public WorkflowPlan compile(
      byte[] source, Collection<ResolvedWorkflowResource> suppliedResources) {
    return compile(source, suppliedResources, WorkflowDefinitionCatalog.unavailable());
  }

  public WorkflowPlan compile(
      byte[] source,
      Collection<ResolvedWorkflowResource> suppliedResources,
      WorkflowDefinitionCatalog workflowCatalog) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(suppliedResources, "suppliedResources");
    Objects.requireNonNull(workflowCatalog, "workflowCatalog");
    if (source.length == 0 || source.length > MAX_SOURCE_BYTES) {
      throw new WorkflowDefinitionException(
          List.of(
              source.length == 0
                  ? "Workflow source is empty"
                  : "Workflow source exceeds " + MAX_SOURCE_BYTES + " bytes"));
    }
    final JsonNode root;
    try {
      root = YAML.readTree(source);
    } catch (IOException failure) {
      throw new WorkflowDefinitionException(
          List.of("Source is not valid YAML: " + failure.getMessage()));
    }
    var errors =
        SCHEMA.validate(
            root,
            context -> context.executionConfig(config -> config.formatAssertionsEnabled(true)));
    if (!errors.isEmpty()) {
      throw new WorkflowDefinitionException(
          errors.stream()
              .map(
                  error ->
                      error.getInstanceLocation()
                          + " ["
                          + error.getKeyword()
                          + "] "
                          + error.getMessage())
              .toList());
    }
    JsonNode document = root.required("document");
    var coordinates =
        new WorkflowCoordinates(
            document.required("namespace").textValue(),
            document.required("name").textValue(),
            document.required("version").textValue(),
            document.required("dsl").textValue());
    if (!SUPPORTED_DSL_VERSION.equals(coordinates.dsl())) {
      throw new WorkflowDefinitionException(
          List.of("/document/dsl must be exactly " + SUPPORTED_DSL_VERSION));
    }
    WorkflowExpressionConfiguration expressionConfiguration = expressionConfiguration(root);
    List<ResolvedWorkflowResource> resources = validatedResources(suppliedResources);
    Map<URI, ResolvedWorkflowResource> resourcesByUri = new HashMap<>();
    resources.forEach(resource -> resourcesByUri.put(resource.uri(), resource));
    WorkflowDataFlow dataFlow =
        workflowDataFlow(root, expressionConfiguration.mode(), resourcesByUri);
    ReusableComponents reusable = reusableComponents(root);
    TimeoutPlan workflowTimeout;
    SchedulePlan schedule;
    List<PlanStep> steps;
    try {
      workflowTimeout =
          root.has("timeout")
              ? compileTimeout(
                  root.required("timeout"), "/timeout", expressionConfiguration.mode(), reusable)
              : null;
      schedule =
          root.has("schedule")
              ? compileSchedule(
                  root.required("schedule"), expressionConfiguration.mode(), resourcesByUri)
              : null;
      steps =
          compileTasks(
              root.required("do"),
              "/do",
              expressionConfiguration.mode(),
              resourcesByUri,
              reusable,
              Set.of(),
              workflowCatalog,
              true);
    } catch (IllegalArgumentException incomplete) {
      // Plan records (CallPlan, TimeoutPlan, ...) validate their own semantic
      // invariants in their compact constructors via plain
      // IllegalArgumentException - appropriate for a record's own internal
      // consistency, but when the compiler itself is the one constructing
      // them from author-provided (possibly still-incomplete) YAML, that's
      // exactly the same "not yet compilable" signal WorkflowDefinitionException
      // already carries everywhere else in this method (the schema/DSL-version
      // checks above, for instance) - this specific failure mode just wasn't
      // translated before now. Confirmed live: saving a fresh, blank "call"
      // task (call: "") crashed the whole request with a raw
      // IllegalArgumentException instead of falling back to the source-only
      // draft save every OTHER incomplete-document shape already gets.
      throw new WorkflowDefinitionException(List.of(incomplete.getMessage()));
    }
    validateSchemas(dataFlow, schedule, steps, resources);
    String sourceSha256 = sha256(source);
    WorkflowPlan plan =
        new WorkflowPlan(
            coordinates,
            sourceSha256,
            definitionSha256(sourceSha256, resources, steps),
            COMPILER_SHA256,
            root,
            expressionConfiguration,
            dataFlow,
            resources,
            steps,
            documentMetadata(document),
            workflowTimeout,
            schedule);
    WorkflowContractAnalysis contracts = new WorkflowContractAnalyzer(resources).analyze(plan);
    if (!contracts.proven()) {
      throw new WorkflowDefinitionException(
          contracts.rejectedFindings().stream()
              .map(finding -> "Schema compatibility " + finding.diagnostic())
              .toList());
    }
    return plan;
  }

  private static String definitionSha256(
      String sourceSha256, List<ResolvedWorkflowResource> resources, List<PlanStep> steps) {
    List<ResolvedSubflow> subflows = new ArrayList<>();
    collectSubflows(steps, subflows);
    StringBuilder material =
        new StringBuilder("forwardmeasure-openworkflow-definition:v1\nsource:")
            .append(sourceSha256)
            .append("\ncompiler:")
            .append(COMPILER_SHA256)
            .append('\n');
    resources.stream()
        .sorted(java.util.Comparator.comparing(resource -> resource.uri().toString()))
        .forEach(
            resource ->
                material
                    .append("resource:")
                    .append(resource.uri())
                    .append(':')
                    .append(resource.sha256())
                    .append('\n'));
    subflows.stream()
        .distinct()
        .sorted(java.util.Comparator.comparing(ResolvedSubflow::canonical))
        .forEach(subflow -> material.append("subflow:").append(subflow.canonical()).append('\n'));
    return sha256(material.toString().getBytes(StandardCharsets.UTF_8));
  }

  private static void collectSubflows(List<PlanStep> steps, List<ResolvedSubflow> output) {
    for (PlanStep step : steps) {
      if (step.runPlan() != null && step.runPlan().subflow() != null) {
        output.add(step.runPlan().subflow());
      }
      collectSubflows(step.children(), output);
    }
  }

  private static List<PlanStep> compileTasks(
      JsonNode tasks,
      String listPath,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      ReusableComponents reusable,
      Set<String> functionStack,
      WorkflowDefinitionCatalog workflowCatalog,
      boolean applyExtensions) {
    List<PlanStep> result = new ArrayList<>();
    for (int index = 0; index < tasks.size(); index++) {
      JsonNode item = tasks.get(index);
      Iterator<Map.Entry<String, JsonNode>> fields = item.properties().iterator();
      Map.Entry<String, JsonNode> named = fields.next();
      String name = named.getKey();
      JsonNode task = named.getValue();
      String path = listPath + "/" + index + "/" + escape(name);
      TimeoutPlan timeout =
          task.has("timeout")
              ? compileTimeout(
                  task.required("timeout"), path + "/timeout", expressionMode, reusable)
              : null;
      TaskDataFlow dataFlow = taskDataFlow(task, path, expressionMode, resources);
      String keyword = taskKeyword(task);
      PlanStepKind stepKind = KEYWORD_TO_KIND.get(keyword);
      if (stepKind == null) {
        throw unsupported(path, "task kind '" + keyword + "' is not implemented");
      }
      switch (stepKind) {
        case SET -> {
          JsonNode configuration = task.required(keyword);
          validateTransform(configuration, expressionMode, path + "/set");
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.SET,
                  task,
                  configuration,
                  List.of(),
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case DO -> {
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.DO,
                  task,
                  null,
                  compileTasks(
                      task.required(keyword),
                      path + "/do",
                      expressionMode,
                      resources,
                      reusable,
                      functionStack,
                      workflowCatalog,
                      applyExtensions),
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case SWITCH -> {
          JsonNode configuration = task.required(keyword);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.SWITCH,
                  task,
                  configuration,
                  List.of(),
                  compileSwitchCases(configuration, path + "/switch", expressionMode),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case FOR -> {
          JsonNode configuration = task.required(keyword);
          ForPlan forPlan = compileFor(task, configuration, path, expressionMode);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.FOR,
                  task,
                  configuration,
                  compileTasks(
                      // Deliberately still the literal "do", not keyword ("for" here) - a for
                      // loop's own body sits under a sibling "do" key, a DIFFERENT field from
                      // its own "for" configuration (see KNOWN_TASK_KEYWORDS' comment on why
                      // that ordering matters too).
                      task.required("do"),
                      path + "/do",
                      expressionMode,
                      resources,
                      reusable,
                      functionStack,
                      workflowCatalog,
                      applyExtensions),
                  List.of(),
                  forPlan,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case FORK -> {
          JsonNode configuration = task.required(keyword);
          List<PlanStep> branches =
              compileForkBranches(
                  configuration.required("branches"),
                  path + "/fork/branches",
                  expressionMode,
                  resources,
                  reusable,
                  functionStack,
                  workflowCatalog,
                  applyExtensions);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.FORK,
                  task,
                  configuration,
                  branches,
                  List.of(),
                  null,
                  new ForkPlan(configuration.path("compete").asBoolean(false)),
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case EMIT -> {
          JsonNode properties = task.required(keyword).required("event").required("with");
          validateTransform(properties, expressionMode, path + "/emit/event/with");
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.EMIT,
                  task,
                  properties,
                  List.of(),
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case CALL -> {
          JsonNode arguments =
              task.has("with") ? task.required("with") : JsonNodeFactory.instance.objectNode();
          validateTransform(arguments, expressionMode, path + "/with");
          String call = task.required(keyword).textValue();
          CallPlan callPlan =
              compileCall(call, arguments, expressionMode, resources, reusable, path);
          List<PlanStep> functionBody;
          if (callPlan.kind() == CallPlan.Kind.FUNCTION) {
            functionBody =
                compileFunction(
                    call,
                    path,
                    expressionMode,
                    resources,
                    reusable,
                    functionStack,
                    workflowCatalog,
                    applyExtensions);
          } else if (callPlan.asyncApiSubscription() != null
              && callPlan.asyncApiSubscription().foreach()) {
            JsonNode iterator = arguments.required("subscription").required("foreach");
            functionBody =
                iterator.has("do")
                    ? compileTasks(
                        iterator.required("do"),
                        path + "/with/subscription/foreach/do",
                        expressionMode,
                        resources,
                        reusable,
                        functionStack,
                        workflowCatalog,
                        applyExtensions)
                    : List.of();
          } else {
            functionBody = List.of();
          }
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.CALL,
                  task,
                  arguments,
                  functionBody,
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  callPlan,
                  dataFlow));
        }
        case RUN -> {
          JsonNode configuration = task.required(keyword);
          validateTransform(configuration, expressionMode, path + "/run");
          RunPlan runPlan = compileRun(configuration, resources, path + "/run", workflowCatalog);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.RUN,
                  task,
                  configuration,
                  List.of(),
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  dataFlow,
                  runPlan));
        }
        case LISTEN -> {
          JsonNode configuration = task.required(keyword);
          ListenPlan listenPlan =
              compileListen(task, configuration, path, expressionMode, resources);
          JsonNode foreach = task.get("foreach");
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.LISTEN,
                  task,
                  configuration,
                  foreach != null && foreach.has("do")
                      ? compileTasks(
                          foreach.required("do"),
                          path + "/foreach/do",
                          expressionMode,
                          resources,
                          reusable,
                          functionStack,
                          workflowCatalog,
                          applyExtensions)
                      : List.of(),
                  List.of(),
                  null,
                  null,
                  listenPlan,
                  null,
                  null,
                  null,
                  dataFlow));
        }
        case WAIT -> {
          JsonNode configuration = task.required(keyword);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.WAIT,
                  task,
                  configuration,
                  List.of(),
                  List.of(),
                  null,
                  null,
                  null,
                  new WaitPlan(compileDuration(configuration, path + "/wait", expressionMode)),
                  null,
                  null,
                  dataFlow));
        }
        case RAISE -> {
          JsonNode configuration = task.required(keyword);
          RaisePlan raisePlan =
              compileRaise(
                  configuration.required("error"), path + "/raise/error", expressionMode, reusable);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.RAISE,
                  task,
                  configuration,
                  List.of(),
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  raisePlan,
                  null,
                  dataFlow));
        }
        case TRY -> {
          JsonNode configuration = task.required(keyword);
          JsonNode catchDefinition = task.required("catch");
          List<PlanStep> trySteps =
              compileTasks(
                  configuration,
                  path + "/try",
                  expressionMode,
                  resources,
                  reusable,
                  functionStack,
                  workflowCatalog,
                  applyExtensions);
          List<PlanStep> catchSteps =
              catchDefinition.has("do")
                  ? compileTasks(
                      catchDefinition.required("do"),
                      path + "/catch/do",
                      expressionMode,
                      resources,
                      reusable,
                      functionStack,
                      workflowCatalog,
                      applyExtensions)
                  : List.of();
          CatchPlan catchPlan =
              compileCatch(catchDefinition, catchSteps, path + "/catch", expressionMode, reusable);
          List<PlanStep> allChildren = new ArrayList<>(trySteps);
          allChildren.addAll(catchSteps);
          result.add(
              new PlanStep(
                  name,
                  path,
                  PlanStepKind.TRY,
                  task,
                  configuration,
                  allChildren,
                  List.of(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  new TryPlan(trySteps, catchPlan),
                  dataFlow));
        }
        default -> throw unsupported(path, "task kind '" + keyword + "' is not implemented");
      }
      if (timeout != null) {
        result.set(result.size() - 1, withTimeout(result.getLast(), timeout));
      }
      if (applyExtensions) {
        result.set(
            result.size() - 1,
            applyExtensions(
                result.getLast(),
                keyword,
                expressionMode,
                resources,
                reusable,
                functionStack,
                workflowCatalog));
      }
    }
    validateFlowTargets(result, listPath);
    return List.copyOf(result);
  }

  private static PlanStep applyExtensions(
      PlanStep target,
      String keyword,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      ReusableComponents reusable,
      Set<String> functionStack,
      WorkflowDefinitionCatalog workflowCatalog) {
    if (reusable.extensions().isEmpty()) return target;
    // Reuses KEYWORD_TO_KIND (already built, see above) rather than adding a stepKind parameter
    // just for this - targetKind's non-composite branch still needs the plain keyword string
    // regardless, since it's matched against an author-typed extend: "<kind>" value elsewhere.
    String targetKind =
        switch (KEYWORD_TO_KIND.get(keyword)) {
          case DO, FORK -> "composite";
          default -> keyword;
        };
    List<ExtensionApplicationPlan> applications = new ArrayList<>();
    for (int index = 0; index < reusable.extensions().size(); index++) {
      JsonNode item = reusable.extensions().get(index);
      Map.Entry<String, JsonNode> named = item.properties().iterator().next();
      String name = named.getKey();
      JsonNode extension = named.getValue();
      String extend = extension.required("extend").textValue();
      if (!"all".equals(extend) && !targetKind.equals(extend)) {
        continue;
      }
      String condition =
          extension.has("when") ? requiredExpression(extension.required("when").textValue()) : null;
      if (condition != null) {
        validateExpression(
            condition, expressionMode, "/use/extensions/" + index + "/" + escape(name) + "/when");
      }
      String base = target.path() + "/$extensions/" + index + "/" + escape(name);
      List<PlanStep> before =
          extension.has("before")
              ? compileTasks(
                  extension.required("before"),
                  base + "/before",
                  expressionMode,
                  resources,
                  reusable,
                  functionStack,
                  workflowCatalog,
                  false)
              : List.of();
      List<PlanStep> after =
          extension.has("after")
              ? compileTasks(
                  extension.required("after"),
                  base + "/after",
                  expressionMode,
                  resources,
                  reusable,
                  functionStack,
                  workflowCatalog,
                  false)
              : List.of();
      applications.add(new ExtensionApplicationPlan(name, extend, condition, before, after));
    }
    if (applications.isEmpty()) return target;

    PlanStep durableTarget = withoutCommonSemantics(target, target.path() + "/$task");
    ExtensionPlan extensionPlan = new ExtensionPlan(durableTarget, applications);
    return new PlanStep(
        target.name(),
        target.path(),
        PlanStepKind.EXTENSION,
        target.definition(),
        null,
        extensionPlan.allChildren(),
        List.of(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        target.dataFlow(),
        null,
        target.timeout(),
        extensionPlan);
  }

  private static PlanStep withoutCommonSemantics(PlanStep target, String path) {
    return new PlanStep(
        target.name(),
        path,
        target.kind(),
        target.definition(),
        target.configuration(),
        target.children(),
        target.switchCases(),
        target.forPlan(),
        target.forkPlan(),
        target.listenPlan(),
        target.waitPlan(),
        target.raisePlan(),
        target.tryPlan(),
        target.callPlan(),
        TaskDataFlow.defaults(),
        target.runPlan(),
        null,
        null);
  }

  private static List<PlanStep> compileFunction(
      String functionName,
      String invocationPath,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      ReusableComponents reusable,
      Set<String> functionStack,
      WorkflowDefinitionCatalog workflowCatalog,
      boolean applyExtensions) {
    JsonNode definition = reusable.functions().get(functionName);
    if (definition == null) {
      CatalogFunctionReference reference =
          CatalogFunctionReference.resolve(functionName, reusable.catalogs())
              .orElseThrow(
                  () ->
                      unsupported(
                          invocationPath + "/call",
                          "custom function '"
                              + functionName
                              + "' is not defined in "
                              + "use.functions and is not a "
                              + "catalogued function reference"));
      ResolvedWorkflowResource resource = resources.get(reference.uri());
      if (resource == null) {
        throw unsupported(
            invocationPath + "/call",
            "catalogued function " + functionName + " was not resolved before publication");
      }
      definition = WorkflowResourceResolver.parseDocument(resource, "catalog function");
      if (!definition.isObject()) {
        throw unsupported(
            invocationPath + "/call",
            "catalogued function " + functionName + " must contain one task object");
      }
    }
    if (functionStack.contains(functionName)) {
      throw unsupported(
          invocationPath + "/call",
          "reusable function cycle detected: "
              + String.join(" -> ", functionStack)
              + " -> "
              + functionName);
    }
    Set<String> nested = new java.util.LinkedHashSet<>(functionStack);
    nested.add(functionName);
    var tasks = JsonNodeFactory.instance.arrayNode();
    tasks.add(JsonNodeFactory.instance.objectNode().set(functionName, definition.deepCopy()));
    return compileTasks(
        tasks,
        invocationPath + "/function",
        expressionMode,
        resources,
        reusable,
        Set.copyOf(nested),
        workflowCatalog,
        applyExtensions);
  }

  // Every string here is a PlanStepKind's own .name() (compiler-checked - a typo or a renamed
  // enum constant fails to compile instead of silently never matching), but the ORDER is
  // explicit and load-bearing, not PlanStepKind's declaration order: a "for" task's own body
  // sits under a sibling "do" key on the SAME task object (the loop's config and its body are
  // two separate top-level fields, not one nested under the other), so "for" must be checked
  // before "do" or a for-loop task gets silently misidentified as a plain "do" container -
  // confirmed by two real test failures when this was first derived via
  // Arrays.stream(PlanStepKind.values()), which happens to declare DO before FOR. CALL is
  // intentionally checked separately below, unchanged from before this refactor.
  private static final List<String> KNOWN_TASK_KEYWORDS =
      Stream.of(
              PlanStepKind.SET,
              PlanStepKind.SWITCH,
              PlanStepKind.FOR,
              PlanStepKind.FORK,
              PlanStepKind.EMIT,
              PlanStepKind.LISTEN,
              PlanStepKind.WAIT,
              PlanStepKind.RAISE,
              PlanStepKind.TRY,
              PlanStepKind.DO,
              PlanStepKind.RUN)
          .map(kind -> kind.name().toLowerCase(Locale.ROOT))
          .toList();

  // The dispatch table compileTasks' switch below uses, built from the exact same keyword list
  // above (plus "call", checked the same separate way taskKeyword() checks it) - one place maps
  // a keyword string to its PlanStepKind, so the switch itself can be written against the enum
  // (case SET, case DO, ...) instead of repeating each keyword a second time as a case-label
  // string literal.
  private static final Map<String, PlanStepKind> KEYWORD_TO_KIND =
      Stream.concat(
              KNOWN_TASK_KEYWORDS.stream()
                  .map(kw -> Map.entry(kw, PlanStepKind.valueOf(kw.toUpperCase(Locale.ROOT)))),
              Stream.of(Map.entry("call", PlanStepKind.CALL)))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  private static String taskKeyword(JsonNode task) {
    for (String supported : KNOWN_TASK_KEYWORDS) {
      if (task.has(supported)) return supported;
    }
    if (task.has("call")) return "call";
    for (Iterator<String> names = task.fieldNames(); names.hasNext(); ) {
      String candidate = names.next();
      if (!COMMON_TASK_PROPERTIES.contains(candidate) && !"while".equals(candidate)) {
        return candidate;
      }
    }
    throw unsupported("/", "task kind could not be determined");
  }

  // The six reserved call-variant keywords plus the two reserved custom-function names, mapped
  // to their CallPlan.Kind - same reasoning as KEYWORD_TO_KIND above: compileCall's own switch
  // dispatches on the enum instead of repeating each string a second time as a case label. Any
  // "call" value NOT in this map is an ordinary catalogued/inline function name, not an error -
  // it resolves to Kind.FUNCTION, the switch's own default.
  private static final Map<String, CallPlan.Kind> CALL_VARIANT_TO_KIND =
      Map.of(
          "asyncapi",
          CallPlan.Kind.ASYNC_API,
          "grpc",
          CallPlan.Kind.GRPC,
          "http",
          CallPlan.Kind.HTTP,
          "openapi",
          CallPlan.Kind.OPEN_API,
          "a2a",
          CallPlan.Kind.A2A,
          "mcp",
          CallPlan.Kind.MCP,
          CallPlan.HUMAN_TASK_FUNCTION,
          CallPlan.Kind.HUMAN_TASK,
          CallPlan.CORRELATED_WORKER_FUNCTION,
          CallPlan.Kind.CORRELATED_WORKER);

  private static CallPlan compileCall(
      String call,
      JsonNode arguments,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      ReusableComponents reusable,
      String path) {
    AuthenticationPlan authentication =
        compileCallAuthentication(call, arguments, reusable, path, expressionMode);
    JsonNode safeArguments = withoutAuthentication(call, arguments);
    return switch (CALL_VARIANT_TO_KIND.getOrDefault(call, CallPlan.Kind.FUNCTION)) {
      case ASYNC_API -> {
        WorkflowResourceReference document =
            callResource(
                arguments.required("document"),
                WorkflowResourceKind.ASYNC_API_DOCUMENT,
                resources,
                path + "/with/document");
        validateAsyncApiOperation(document, arguments, resources, path);
        yield new CallPlan(
            CallPlan.Kind.ASYNC_API,
            null,
            document,
            safeArguments,
            arguments.path("subscription").isObject()
                ? compileAsyncApiSubscription(
                    arguments.required("subscription"),
                    expressionMode,
                    resources,
                    path + "/with/subscription")
                : null,
            authentication);
      }
      case GRPC -> {
        WorkflowResourceReference proto =
            callResource(
                arguments.required("proto"),
                WorkflowResourceKind.GRPC_PROTO,
                resources,
                path + "/with/proto");
        validateGrpcOperation(proto, arguments, resources, path);
        yield new CallPlan(CallPlan.Kind.GRPC, null, proto, safeArguments, null, authentication);
      }
      case HTTP ->
          new CallPlan(CallPlan.Kind.HTTP, null, null, safeArguments, null, authentication);
      case OPEN_API -> {
        WorkflowResourceReference document =
            callResource(
                arguments.required("document"),
                WorkflowResourceKind.OPEN_API_DOCUMENT,
                resources,
                path + "/with/document");
        validateOpenApiOperation(
            document, arguments.required("operationId").asText(), resources, path);
        yield new CallPlan(
            CallPlan.Kind.OPEN_API, null, document, safeArguments, null, authentication);
      }
      case A2A ->
          new CallPlan(
              CallPlan.Kind.A2A,
              null,
              arguments.has("agentCard")
                  ? callResource(
                      arguments.required("agentCard"),
                      WorkflowResourceKind.A2A_AGENT_CARD,
                      resources,
                      path + "/with/agentCard")
                  : null,
              safeArguments,
              null,
              authentication);
      case MCP -> {
        validateMcpStdioEnvironmentSecret(safeArguments, reusable, path);
        yield new CallPlan(CallPlan.Kind.MCP, null, null, safeArguments, null, authentication);
      }
      case HUMAN_TASK -> {
        validateHumanTaskCall(safeArguments, path + "/with");
        yield new CallPlan(CallPlan.Kind.HUMAN_TASK, null, null, safeArguments);
      }
      case CORRELATED_WORKER -> {
        validateCorrelatedWorkerCall(safeArguments, path + "/with");
        yield new CallPlan(
            CallPlan.Kind.CORRELATED_WORKER,
            null,
            callResource(
                arguments.required("document"),
                WorkflowResourceKind.ASYNC_API_DOCUMENT,
                resources,
                path + "/with/document"),
            safeArguments,
            compileAsyncApiSubscription(
                arguments.required("events").required("subscription"),
                expressionMode,
                resources,
                path + "/with/events/subscription"),
            authentication);
      }
      case FUNCTION -> {
        WorkflowResourceReference functionResource =
            CatalogFunctionReference.resolve(call, reusable.catalogs())
                .map(
                    reference -> {
                      ResolvedWorkflowResource resolved = resources.get(reference.uri());
                      if (resolved == null) {
                        throw unsupported(
                            path + "/call",
                            "catalogued function "
                                + call
                                + " was not resolved "
                                + "before publication");
                      }
                      return resolved.reference(WorkflowResourceKind.FUNCTION_DEFINITION);
                    })
                .orElse(null);
        yield new CallPlan(CallPlan.Kind.FUNCTION, call, functionResource, arguments);
      }
    };
  }

  private static void validateOpenApiOperation(
      WorkflowResourceReference reference,
      String operationId,
      Map<URI, ResolvedWorkflowResource> resources,
      String path) {
    ResolvedWorkflowResource resource = resources.get(reference.uri());
    if (resource == null || !resource.sha256().equals(reference.sha256())) {
      throw unsupported(path + "/with/document", "the pinned OpenAPI document is unavailable");
    }
    JsonNode document = WorkflowResourceResolver.parseDocument(resource, "OpenAPI document");
    boolean openApi3 = document.path("openapi").asText().startsWith("3.");
    boolean swagger2 = "2.0".equals(document.path("swagger").asText());
    if (!openApi3 && !swagger2) {
      throw unsupported(
          path + "/with/document", "must contain an OpenAPI 3.x or Swagger 2.0 document");
    }
    int matches = 0;
    for (Iterator<JsonNode> pathItems = document.path("paths").elements(); pathItems.hasNext(); ) {
      JsonNode pathItem = pathItems.next();
      for (String method :
          List.of("get", "put", "post", "delete", "options", "head", "patch", "trace")) {
        if (operationId.equals(pathItem.path(method).path("operationId").asText())) matches++;
      }
    }
    if (matches != 1) {
      throw unsupported(
          path + "/with/operationId",
          matches == 0
              ? "does not identify an operation in the pinned OpenAPI document"
              : "must uniquely identify one operation in the pinned OpenAPI document");
    }
  }

  private static void validateAsyncApiOperation(
      WorkflowResourceReference reference,
      JsonNode arguments,
      Map<URI, ResolvedWorkflowResource> resources,
      String path) {
    ResolvedWorkflowResource resource = resources.get(reference.uri());
    if (resource == null || !resource.sha256().equals(reference.sha256())) {
      throw unsupported(path + "/with/document", "the pinned AsyncAPI document is unavailable");
    }
    JsonNode document = WorkflowResourceResolver.parseDocument(resource, "AsyncAPI document");
    String version = document.path("asyncapi").asText();
    if (!(version.startsWith("2.6") || version.startsWith("3."))) {
      throw unsupported(path + "/with/document", "must contain an AsyncAPI 2.6 or 3.x document");
    }
    if (arguments.has("operation")) {
      String wanted = arguments.required("operation").asText();
      int matches = 0;
      for (Iterator<Map.Entry<String, JsonNode>> operations =
              document.path("operations").properties().iterator();
          operations.hasNext(); ) {
        Map.Entry<String, JsonNode> operation = operations.next();
        if (wanted.equals(operation.getKey())
            || wanted.equals(operation.getValue().path("operationId").asText())) matches++;
      }
      if (matches != 1)
        throw unsupported(
            path + "/with/operation",
            "must uniquely identify an operation in the pinned AsyncAPI document");
    } else {
      String channel = arguments.required("channel").asText();
      if (!document.path("channels").has(channel))
        throw unsupported(
            path + "/with/channel", "does not identify a channel in the pinned AsyncAPI document");
    }
  }

  private static void validateGrpcOperation(
      WorkflowResourceReference reference,
      JsonNode arguments,
      Map<URI, ResolvedWorkflowResource> resources,
      String path) {
    ResolvedWorkflowResource resource = resources.get(reference.uri());
    if (resource == null || !resource.sha256().equals(reference.sha256())) {
      throw unsupported(path + "/with/proto", "the pinned proto resource is unavailable");
    }
    String service = arguments.required("service").required("name").asText();
    String simpleService = service.substring(service.lastIndexOf('.') + 1);
    String packageName =
        service.contains(".") ? service.substring(0, service.lastIndexOf('.')) : null;
    String method = arguments.required("method").asText();
    java.util.regex.Pattern declaration =
        java.util.regex.Pattern.compile(
            "(?s)\\bservice\\s+"
                + java.util.regex.Pattern.quote(simpleService)
                + "\\s*\\{(.*?)\\}");
    java.util.regex.Matcher serviceMatch = declaration.matcher(resource.content());
    boolean packageMatches =
        packageName == null
            || java.util.regex.Pattern.compile(
                    "(?m)^\\s*package\\s+" + java.util.regex.Pattern.quote(packageName) + "\\s*;")
                .matcher(resource.content())
                .find();
    if (!packageMatches
        || !serviceMatch.find()
        || !java.util.regex.Pattern.compile(
                "\\brpc\\s+" + java.util.regex.Pattern.quote(method) + "\\s*\\(")
            .matcher(serviceMatch.group(1))
            .find()) {
      throw unsupported(
          path + "/with/method",
          "does not identify an RPC on service " + service + " in the pinned proto resource");
    }
  }

  /**
   * Validates the static shape of the governed human-task extension. Runtime expressions are
   * deliberately left unevaluated until the task starts.
   */
  private static void validateHumanTaskCall(JsonNode arguments, String path) {
    if (!arguments.isObject()) {
      throw unsupported(path, "human-task arguments must be an object");
    }
    if (!arguments.has("title")) {
      throw unsupported(path + "/title", "human-task title is required");
    }
    if (!arguments.has("approvals")) {
      throw unsupported(path + "/approvals", "human-task approval policy is required");
    }
    JsonNode approvals = arguments.required("approvals");
    if (approvals.isObject()) {
      JsonNode stages = approvals.get("stages");
      if (stages == null || !stages.isArray() || stages.isEmpty()) {
        throw unsupported(
            path + "/approvals/stages", "human-task approvals require at least one stage");
      }
    }
    if (arguments.has("dueAt") && arguments.has("dueAfter")) {
      throw unsupported(path, "human-task may declare dueAt or dueAfter, not both");
    }
    JsonNode presentation = arguments.get("presentation");
    if (presentation != null && presentation.isObject() && !presentation.has("kind")) {
      throw unsupported(path + "/presentation/kind", "human-task presentation kind is required");
    }
  }

  /** Validates the static shape of the correlated AsyncAPI worker call. */
  private static void validateCorrelatedWorkerCall(JsonNode arguments, String path) {
    if (!arguments.isObject()) {
      throw unsupported(path, "correlated-worker arguments must be an object");
    }
    if (!arguments.path("document").isObject()) {
      throw unsupported(path + "/document", "correlated-worker document is required");
    }
    JsonNode command = arguments.path("command");
    if (!command.isObject() || !command.path("message").isObject()) {
      throw unsupported(path + "/command", "correlated-worker command with a message is required");
    }
    JsonNode events = arguments.path("events");
    if (!events.isObject() || !events.path("subscription").isObject()) {
      throw unsupported(
          path + "/events/subscription", "correlated-worker event subscription is required");
    }
    JsonNode cancellation = arguments.get("cancellation");
    if (cancellation != null
        && (!cancellation.isObject() || !cancellation.path("message").isObject())) {
      throw unsupported(
          path + "/cancellation", "correlated-worker cancellation must contain a message");
    }
    if (!events.path("subscription").path("consume").has("for")) {
      throw unsupported(
          path + "/events/subscription/consume/for", "correlated-worker timeout is required");
    }
  }

  /**
   * Implementation extension carried in the specification-defined MCP transport options map. Only
   * the reference is admitted; values are resolved later at the authorised adapter edge.
   */
  private static void validateMcpStdioEnvironmentSecret(
      JsonNode arguments, ReusableComponents reusable, String path) {
    JsonNode configured = arguments.path("transport").path("options").get("environmentSecret");
    if (configured == null || configured.isNull()) return;
    if (!arguments.path("transport").has("stdio")) {
      throw unsupported(
          path + "/with/transport/options/environmentSecret",
          "is valid only for the MCP stdio transport");
    }
    if (!configured.isTextual() || configured.textValue().isBlank()) {
      throw unsupported(
          path + "/with/transport/options/environmentSecret", "must be a non-blank secret name");
    }
    if (!reusable.secrets().contains(configured.textValue())) {
      throw unsupported(
          path + "/with/transport/options/environmentSecret",
          "references undeclared secret '"
              + configured.textValue()
              + "'; declare it in use.secrets");
    }
  }

  private static AuthenticationPlan compileCallAuthentication(
      String call,
      JsonNode arguments,
      ReusableComponents reusable,
      String path,
      ExpressionMode expressionMode) {
    JsonNode configured =
        switch (CALL_VARIANT_TO_KIND.getOrDefault(call, CallPlan.Kind.FUNCTION)) {
          case ASYNC_API, OPEN_API -> arguments.get("authentication");
          case GRPC -> arguments.path("service").get("authentication");
          case HTTP -> arguments.path("endpoint").get("authentication");
          case A2A -> arguments.path("server").get("authentication");
          case MCP ->
              arguments.path("transport").path("http").path("endpoint").get("authentication");
          case HUMAN_TASK, CORRELATED_WORKER, FUNCTION -> null;
        };
    if (configured == null || configured.isNull()) {
      return null;
    }

    String reusableName = null;
    JsonNode policy = configured;
    if (configured.has("use")) {
      reusableName = configured.required("use").textValue();
      policy = reusable.authentications().get(reusableName);
      if (policy == null) {
        throw unsupported(
            path,
            "reusable authentication '" + reusableName + "' is not defined in use.authentications");
      }
    }

    String policyMember = null;
    for (String candidate : List.of("basic", "bearer", "digest", "oauth2", "oidc")) {
      if (policy.has(candidate)) {
        policyMember = candidate;
        break;
      }
    }
    if (policyMember == null) {
      throw unsupported(path, "authentication policy kind could not be determined");
    }
    JsonNode policyConfiguration = policy.required(policyMember);
    if (!policyConfiguration.has("use")) {
      AuthenticationPlan.Kind kind =
          AuthenticationPlan.Kind.valueOf(policyMember.toUpperCase(java.util.Locale.ROOT));
      List<String> secretReferences =
          validateDynamicAuthentication(
              kind, policyConfiguration, expressionMode, reusable.secrets(), path);
      return AuthenticationPlan.expressions(
          kind, reusableName, policyConfiguration, secretReferences);
    }
    String secretName = policyConfiguration.required("use").textValue();
    if (!reusable.secrets().contains(secretName)) {
      throw unsupported(
          path,
          "authentication references undeclared secret '"
              + secretName
              + "'; declare it in use.secrets");
    }
    return new AuthenticationPlan(
        AuthenticationPlan.Kind.valueOf(policyMember.toUpperCase(java.util.Locale.ROOT)),
        reusableName,
        secretName);
  }

  private static List<String> validateDynamicAuthentication(
      AuthenticationPlan.Kind kind,
      JsonNode configuration,
      ExpressionMode expressionMode,
      Set<String> declaredSecrets,
      String path) {
    List<String> sensitivePointers =
        switch (kind) {
          case BASIC, DIGEST -> List.of("/username", "/password");
          case BEARER -> List.of("/token");
          case OAUTH2, OIDC ->
              List.of(
                  "/client/secret",
                  "/client/assertion",
                  "/username",
                  "/password",
                  "/subject/token",
                  "/actor/token");
        };
    List<String> configuredSensitive =
        sensitivePointers.stream()
            .filter(pointer -> !configuration.at(pointer).isMissingNode())
            .toList();
    if (configuredSensitive.isEmpty()
        || configuredSensitive.stream()
            .anyMatch(pointer -> !isStrictRuntimeExpression(configuration.at(pointer)))) {
      throw unsupported(
          path,
          "inline authentication credentials must either be "
              + "runtime expressions or be externalized to a "
              + "secret declared in use.secrets");
    }
    try {
      EXPRESSIONS.validateTemplate(configuration, expressionMode);
    } catch (RuntimeException invalid) {
      throw unsupported(
          path, "authentication runtime expression is invalid: " + invalid.getMessage());
    }
    LinkedHashSet<String> references = new LinkedHashSet<>();
    collectSecretReferences(configuration, references);
    for (String reference : references) {
      if (!declaredSecrets.contains(reference)) {
        throw unsupported(
            path,
            "authentication expression references undeclared "
                + "secret '"
                + reference
                + "'; declare it in use.secrets");
      }
    }
    if (configuration.toString().contains("$secrets") && references.isEmpty()) {
      throw unsupported(
          path,
          "authentication expressions must use literal "
              + "$secrets.name or $secrets[\"name\"] "
              + "references so material can be resolved at "
              + "the authorised edge");
    }
    return references.stream().sorted().toList();
  }

  private static void collectSecretReferences(JsonNode node, Set<String> references) {
    if (node.isTextual()) {
      String expression = node.textValue();
      Matcher bracket = SECRET_BRACKET_REFERENCE.matcher(expression);
      while (bracket.find()) {
        references.add(bracket.group(2));
      }
      Matcher member = SECRET_MEMBER_REFERENCE.matcher(expression);
      while (member.find()) {
        references.add(member.group(1));
      }
      return;
    }
    if (node.isContainerNode()) {
      node.forEach(child -> collectSecretReferences(child, references));
    }
  }

  private static boolean isStrictRuntimeExpression(JsonNode value) {
    if (value == null || !value.isTextual()) {
      return false;
    }
    String text = value.textValue().trim();
    return text.startsWith("${") && text.endsWith("}");
  }

  private static JsonNode withoutAuthentication(String call, JsonNode arguments) {
    JsonNode copy = arguments.deepCopy();
    switch (CALL_VARIANT_TO_KIND.getOrDefault(call, CallPlan.Kind.FUNCTION)) {
      case ASYNC_API, OPEN_API ->
          ((com.fasterxml.jackson.databind.node.ObjectNode) copy).remove("authentication");
      case GRPC -> removeNestedAuthentication(copy, "service");
      case HTTP -> removeNestedAuthentication(copy, "endpoint");
      case A2A -> removeNestedAuthentication(copy, "server");
      case MCP -> {
        JsonNode endpoint = copy.path("transport").path("http").path("endpoint");
        if (endpoint.isObject()) {
          ((com.fasterxml.jackson.databind.node.ObjectNode) endpoint).remove("authentication");
        }
      }
      case HUMAN_TASK, CORRELATED_WORKER, FUNCTION -> {
        // User-defined reusable functions do not define adapter auth.
      }
    }
    return copy;
  }

  private static void removeNestedAuthentication(JsonNode parent, String member) {
    JsonNode nested = parent.path(member);
    if (nested.isObject()) {
      ((com.fasterxml.jackson.databind.node.ObjectNode) nested).remove("authentication");
    }
  }

  private static RunPlan compileRun(
      JsonNode run,
      Map<URI, ResolvedWorkflowResource> resources,
      String path,
      WorkflowDefinitionCatalog workflowCatalog) {
    RunPlan.Kind kind;
    JsonNode configuration;
    WorkflowResourceReference resource = null;
    ResolvedSubflow subflow = null;
    if (run.has("container")) {
      kind = RunPlan.Kind.CONTAINER;
      configuration = run.required("container");
    } else if (run.has("script")) {
      kind = RunPlan.Kind.SCRIPT;
      configuration = run.required("script");
      if (configuration.has("source")) {
        resource =
            callResource(
                configuration.required("source"),
                WorkflowResourceKind.SCRIPT_SOURCE,
                resources,
                path + "/script/source");
      }
    } else if (run.has("shell")) {
      kind = RunPlan.Kind.SHELL;
      configuration = run.required("shell");
    } else if (run.has("workflow")) {
      kind = RunPlan.Kind.WORKFLOW;
      configuration = run.required("workflow");
      String namespace = configuration.required("namespace").textValue();
      String name = configuration.required("name").textValue();
      String version = configuration.required("version").textValue();
      subflow =
          workflowCatalog
              .resolve(namespace, name, version)
              .orElseThrow(
                  () ->
                      unsupported(
                          path + "/workflow",
                          "child workflow "
                              + namespace
                              + "/"
                              + name
                              + "@"
                              + version
                              + " is not admitted in the tenant catalog"));
    } else {
      throw unsupported(path, "run must select container, script, shell or workflow");
    }
    return new RunPlan(
        kind,
        run.path("await").asBoolean(true),
        RunPlan.ReturnMode.parse(run.path("return").asText("stdout")),
        configuration,
        resource,
        subflow);
  }

  private static AsyncApiSubscriptionPlan compileAsyncApiSubscription(
      JsonNode subscription,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      String path) {
    String filter = optionalExpression(subscription, "filter", path, expressionMode);
    JsonNode consume = subscription.required("consume");
    AsyncApiSubscriptionPlan.Consumption.Mode mode;
    Integer amount = null;
    String condition = null;
    if (consume.has("amount")) {
      mode = AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT;
      amount = consume.required("amount").intValue();
    } else if (consume.has("while")) {
      mode = AsyncApiSubscriptionPlan.Consumption.Mode.WHILE;
      condition = optionalExpression(consume, "while", path + "/consume", expressionMode);
    } else {
      mode = AsyncApiSubscriptionPlan.Consumption.Mode.UNTIL;
      condition = optionalExpression(consume, "until", path + "/consume", expressionMode);
    }
    DurationPlan duration =
        consume.has("for")
            ? compileDuration(consume.required("for"), path + "/consume/for", expressionMode)
            : null;
    JsonNode foreach = subscription.get("foreach");
    String item = null;
    String index = null;
    TaskDataFlow dataFlow = null;
    if (foreach != null) {
      item = foreach.path("item").asText("item");
      index = foreach.path("at").asText("index");
      validateLoopVariable(item, path + "/foreach/item");
      validateLoopVariable(index, path + "/foreach/at");
      if (item.equals(index)) {
        throw unsupported(path + "/foreach", "item and at must name different variables");
      }
      dataFlow = taskDataFlow(foreach, path + "/foreach", expressionMode, resources);
    }
    return new AsyncApiSubscriptionPlan(
        filter,
        new AsyncApiSubscriptionPlan.Consumption(mode, amount, condition, duration),
        item,
        index,
        dataFlow);
  }

  private static WorkflowResourceReference callResource(
      JsonNode declaration,
      WorkflowResourceKind kind,
      Map<URI, ResolvedWorkflowResource> resources,
      String path) {
    URI uri = WorkflowResourceResolver.literalEndpoint(declaration.required("endpoint"));
    ResolvedWorkflowResource resolved = resources.get(withoutFragment(uri));
    if (resolved == null) {
      throw unsupported(path, "external resource " + uri + " was not resolved before publication");
    }
    return resolved.reference(kind);
  }

  private static ListenPlan compileListen(
      JsonNode task,
      JsonNode configuration,
      String path,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources) {
    EventConsumptionPlan consumption =
        compileEventConsumption(
            configuration.required("to"), path + "/listen/to", expressionMode, resources);
    EventReadMode readAs =
        EventReadMode.valueOf(
            configuration.path("read").asText("data").toUpperCase(java.util.Locale.ROOT));
    JsonNode foreach = task.get("foreach");
    if (foreach == null) {
      return new ListenPlan(consumption, readAs, null, null, null);
    }
    String item = foreach.path("item").asText("item");
    String at = foreach.path("at").asText("index");
    validateLoopVariable(item, path + "/foreach/item");
    validateLoopVariable(at, path + "/foreach/at");
    if (item.equals(at)) {
      throw unsupported(path + "/foreach", "item and at must name different variables");
    }
    return new ListenPlan(
        consumption,
        readAs,
        item,
        at,
        taskDataFlow(foreach, path + "/foreach", expressionMode, resources));
  }

  private static DurationPlan compileDuration(
      JsonNode duration, String path, ExpressionMode expressionMode) {
    if (duration.isObject()) {
      return new DurationPlan(DurationPlan.Kind.INLINE, duration);
    }
    String value = duration.textValue();
    if (isExpression(value)) {
      validateExpression(value, expressionMode, path);
      return new DurationPlan(DurationPlan.Kind.EXPRESSION, duration);
    }
    try {
      Iso8601Duration.validate(value);
    } catch (IllegalArgumentException failure) {
      throw unsupported(path, "invalid ISO 8601 duration literal: " + value);
    }
    return new DurationPlan(DurationPlan.Kind.LITERAL, duration);
  }

  private static EventConsumptionPlan compileEventConsumption(
      JsonNode strategy,
      String path,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources) {
    EventConsumptionPlan.Mode mode;
    JsonNode filters;
    if (strategy.has("one")) {
      mode = EventConsumptionPlan.Mode.ONE;
      filters = YAML.createArrayNode().add(strategy.required("one"));
    } else if (strategy.has("all")) {
      mode = EventConsumptionPlan.Mode.ALL;
      filters = strategy.required("all");
    } else {
      mode = EventConsumptionPlan.Mode.ANY;
      filters = strategy.required("any");
    }
    List<EventFilterPlan> compiledFilters = new ArrayList<>();
    for (int index = 0; index < filters.size(); index++) {
      JsonNode filter = filters.get(index);
      JsonNode properties = filter.required("with");
      validateTransform(
          properties,
          expressionMode,
          path + "/" + mode.name().toLowerCase(java.util.Locale.ROOT) + "/" + index + "/with");
      List<EventCorrelationPlan> correlations = new ArrayList<>();
      filter
          .path("correlate")
          .properties()
          .forEach(
              entry -> {
                String from = entry.getValue().required("from").textValue();
                validateExpression(
                    requiredExpression(from),
                    expressionMode,
                    path + "/correlate/" + escape(entry.getKey()) + "/from");
                String expected = entry.getValue().path("expect").asText(null);
                if (expected != null && isExpression(expected)) {
                  validateExpression(
                      expected,
                      expressionMode,
                      path + "/correlate/" + escape(entry.getKey()) + "/expect");
                }
                correlations.add(new EventCorrelationPlan(entry.getKey(), from, expected));
              });
      compiledFilters.add(
          new EventFilterPlan(
              properties,
              correlations,
              eventDataSchema(
                  properties,
                  path
                      + "/"
                      + mode.name().toLowerCase(java.util.Locale.ROOT)
                      + "/"
                      + index
                      + "/with/dataschema",
                  resources)));
    }
    String untilCondition = null;
    EventConsumptionPlan untilConsumed = null;
    if (mode == EventConsumptionPlan.Mode.ANY && strategy.has("until")) {
      JsonNode until = strategy.required("until");
      if (until.isTextual()) {
        untilCondition = until.textValue();
        validateExpression(requiredExpression(untilCondition), expressionMode, path + "/until");
      } else {
        untilConsumed = compileEventConsumption(until, path + "/until", expressionMode, resources);
      }
    }
    return new EventConsumptionPlan(mode, compiledFilters, untilCondition, untilConsumed);
  }

  private static ReusableComponents reusableComponents(JsonNode root) {
    JsonNode use = root.path("use");
    Set<String> secrets = new java.util.LinkedHashSet<>();
    use.path("secrets").forEach(secret -> secrets.add(secret.textValue()));
    return new ReusableComponents(
        use.path("authentications"),
        use.path("errors"),
        use.path("extensions"),
        use.path("retries"),
        use.path("functions"),
        use.path("timeouts"),
        use.path("catalogs"),
        Set.copyOf(secrets));
  }

  private static WorkflowDocumentMetadata documentMetadata(JsonNode document) {
    return new WorkflowDocumentMetadata(
        document.path("title").asText(null),
        document.path("summary").asText(null),
        document.get("tags"),
        document.get("metadata"));
  }

  private static TimeoutPlan compileTimeout(
      JsonNode configured,
      String path,
      ExpressionMode expressionMode,
      ReusableComponents reusable) {
    JsonNode definition = configured;
    String reusableName = null;
    if (configured.isTextual()) {
      reusableName = configured.textValue();
      definition = reusable.timeouts().get(reusableName);
      if (definition == null) {
        throw unsupported(
            path, "reusable timeout '" + reusableName + "' is not defined in use.timeouts");
      }
    }
    return new TimeoutPlan(
        compileDuration(definition.required("after"), path + "/after", expressionMode),
        reusableName);
  }

  private static SchedulePlan compileSchedule(
      JsonNode schedule,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources) {
    return new SchedulePlan(
        schedule.has("every")
            ? compileDuration(schedule.required("every"), "/schedule/every", expressionMode)
            : null,
        schedule.path("cron").asText(null),
        schedule.has("after")
            ? compileDuration(schedule.required("after"), "/schedule/after", expressionMode)
            : null,
        schedule.has("on")
            ? compileEventConsumption(
                schedule.required("on"), "/schedule/on", expressionMode, resources)
            : null,
        EventReadMode.valueOf(
            schedule.path("read").asText("data").toUpperCase(java.util.Locale.ROOT)));
  }

  private static ResolvedDataSchema eventDataSchema(
      JsonNode properties, String path, Map<URI, ResolvedWorkflowResource> resources) {
    JsonNode declared = properties.get("dataschema");
    if (declared == null || !declared.isTextual()) {
      return null;
    }
    String value = declared.textValue();
    if (isExpression(value) || value.contains("{")) {
      return null;
    }
    final URI uri;
    try {
      uri = URI.create(value).normalize();
    } catch (IllegalArgumentException invalid) {
      throw unsupported(path, "invalid event dataschema URI: " + value);
    }
    if (!uri.isAbsolute()) {
      throw unsupported(path, "event dataschema must be an absolute URI");
    }
    ResolvedWorkflowResource resolved = resources.get(withoutFragment(uri));
    if (resolved == null) {
      throw unsupported(path, "event schema " + uri + " was not resolved before publication");
    }
    return new ResolvedDataSchema(
        path, "json", uri, resolved.sha256(), WorkflowResourceResolver.parseSchema(resolved));
  }

  private static PlanStep withTimeout(PlanStep step, TimeoutPlan timeout) {
    return new PlanStep(
        step.name(),
        step.path(),
        step.kind(),
        step.definition(),
        step.configuration(),
        step.children(),
        step.switchCases(),
        step.forPlan(),
        step.forkPlan(),
        step.listenPlan(),
        step.waitPlan(),
        step.raisePlan(),
        step.tryPlan(),
        step.callPlan(),
        step.dataFlow(),
        step.runPlan(),
        timeout);
  }

  private static RaisePlan compileRaise(
      JsonNode value, String path, ExpressionMode expressionMode, ReusableComponents reusable) {
    JsonNode error = value;
    if (value.isTextual()) {
      error = reusable.errors().get(value.textValue());
      if (error == null) {
        throw unsupported(
            path, "reusable error '" + value.textValue() + "' is not defined in use.errors");
      }
    }
    return new RaisePlan(compileError(error, path, expressionMode));
  }

  private static ErrorPlan compileError(
      JsonNode error, String path, ExpressionMode expressionMode) {
    JsonNode type = error.required("type");
    validateDynamicText(type, expressionMode, path + "/type");
    for (String member : List.of("instance", "title", "detail")) {
      if (error.has(member)) {
        validateDynamicText(error.required(member), expressionMode, path + "/" + member);
      }
    }
    return new ErrorPlan(
        type,
        error.required("status").intValue(),
        error.get("instance"),
        error.get("title"),
        error.get("detail"));
  }

  private static CatchPlan compileCatch(
      JsonNode value,
      List<PlanStep> steps,
      String path,
      ExpressionMode expressionMode,
      ReusableComponents reusable) {
    String errorVariable = value.path("as").asText("error");
    validateLoopVariable(errorVariable, path + "/as");
    ErrorFilterPlan filter = null;
    JsonNode with = value.path("errors").path("with");
    if (!with.isMissingNode()) {
      filter =
          new ErrorFilterPlan(
              optionalText(with, "type"),
              with.has("status") ? with.required("status").intValue() : null,
              optionalText(with, "instance"),
              optionalText(with, "title"),
              optionalText(with, "detail"));
    }
    return new CatchPlan(
        filter,
        errorVariable,
        optionalExpression(value, "when", path, expressionMode),
        optionalExpression(value, "exceptWhen", path, expressionMode),
        value.has("retry")
            ? compileRetry(value.required("retry"), path + "/retry", expressionMode, reusable)
            : null,
        steps,
        value.has("then") ? value.required("then").asText() : null);
  }

  private static RetryPlan compileRetry(
      JsonNode value, String path, ExpressionMode expressionMode, ReusableComponents reusable) {
    JsonNode policy = value;
    if (value.isTextual()) {
      policy = reusable.retries().get(value.textValue());
      if (policy == null) {
        throw unsupported(
            path,
            "reusable retry policy '" + value.textValue() + "' is not defined in use.retries");
      }
    }
    JsonNode limit = policy.path("limit");
    JsonNode attempt = limit.path("attempt");
    JsonNode jitter = policy.path("jitter");
    RetryPlan.Backoff backoff = RetryPlan.Backoff.CONSTANT;
    if (policy.path("backoff").has("linear")) {
      backoff = RetryPlan.Backoff.LINEAR;
    } else if (policy.path("backoff").has("exponential")) {
      backoff = RetryPlan.Backoff.EXPONENTIAL;
    }
    return new RetryPlan(
        optionalExpression(policy, "when", path, expressionMode),
        optionalExpression(policy, "exceptWhen", path, expressionMode),
        policy.has("delay")
            ? compileDuration(policy.required("delay"), path + "/delay", expressionMode)
            : null,
        backoff,
        attempt.has("count") ? attempt.required("count").intValue() : null,
        attempt.has("duration")
            ? compileDuration(
                attempt.required("duration"), path + "/limit/attempt/duration", expressionMode)
            : null,
        limit.has("duration")
            ? compileDuration(limit.required("duration"), path + "/limit/duration", expressionMode)
            : null,
        jitter.has("from")
            ? compileDuration(jitter.required("from"), path + "/jitter/from", expressionMode)
            : null,
        jitter.has("to")
            ? compileDuration(jitter.required("to"), path + "/jitter/to", expressionMode)
            : null);
  }

  private static void validateDynamicText(
      JsonNode value, ExpressionMode expressionMode, String path) {
    if (!value.isTextual()) {
      throw unsupported(path, "must be a string");
    }
    if (isExpression(value.textValue())) {
      validateExpression(value.textValue(), expressionMode, path);
    }
  }

  private static String optionalExpression(
      JsonNode object, String member, String path, ExpressionMode expressionMode) {
    if (!object.has(member)) return null;
    String expression = requiredExpression(object.required(member).textValue());
    validateExpression(expression, expressionMode, path + "/" + member);
    return expression;
  }

  private static String optionalText(JsonNode object, String member) {
    return object.has(member) ? object.required(member).textValue() : null;
  }

  private record ReusableComponents(
      JsonNode authentications,
      JsonNode errors,
      JsonNode extensions,
      JsonNode retries,
      JsonNode functions,
      JsonNode timeouts,
      JsonNode catalogs,
      Set<String> secrets) {}

  private static boolean isExpression(String value) {
    String trimmed = value.trim();
    return trimmed.startsWith("${") && trimmed.endsWith("}");
  }

  /**
   * A few 1.0.3 fields are specified as expressions but are not represented by the schema's
   * runtimeExpression definition; normative examples therefore use bare jq even when the document's
   * general mode is strict.
   */
  public static String requiredExpression(String value) {
    return isExpression(value) ? value : "${ " + value + " }";
  }

  private static List<PlanStep> compileForkBranches(
      JsonNode branches,
      String listPath,
      ExpressionMode expressionMode,
      Map<URI, ResolvedWorkflowResource> resources,
      ReusableComponents reusable,
      Set<String> functionStack,
      WorkflowDefinitionCatalog workflowCatalog,
      boolean applyExtensions) {
    List<PlanStep> result = new ArrayList<>();
    Set<String> names = new java.util.HashSet<>();
    for (int index = 0; index < branches.size(); index++) {
      JsonNode branch = branches.get(index);
      String name = branch.fieldNames().next();
      if (!names.add(name)) {
        throw unsupported(listPath, "fork branch names must be unique: " + name);
      }
      List<PlanStep> compiled =
          compileTasks(
              YAML.createArrayNode().add(branch),
              listPath + "/" + index,
              expressionMode,
              resources,
              reusable,
              functionStack,
              workflowCatalog,
              applyExtensions);
      result.add(compiled.getFirst());
    }
    return List.copyOf(result);
  }

  private static ForPlan compileFor(
      JsonNode task, JsonNode configuration, String path, ExpressionMode expressionMode) {
    String itemVariable = configuration.path("each").asText("item");
    String indexVariable = configuration.path("at").asText("index");
    validateLoopVariable(itemVariable, path + "/for/each");
    validateLoopVariable(indexVariable, path + "/for/at");
    if (itemVariable.equals(indexVariable)) {
      throw unsupported(path + "/for", "each and at must name different variables");
    }
    JsonNode collection = configuration.required("in");
    if (collection.isTextual()) {
      collection = JsonNodeFactory.instance.textNode(requiredExpression(collection.textValue()));
      validateExpression(collection.textValue(), expressionMode, path + "/for/in");
    }
    String whileCondition = task.path("while").asText(null);
    if (whileCondition != null) {
      whileCondition = requiredExpression(whileCondition);
      validateExpression(whileCondition, expressionMode, path + "/while");
    }
    return new ForPlan(itemVariable, indexVariable, collection, whileCondition);
  }

  private static void validateLoopVariable(String name, String path) {
    try {
      RuntimeExpressionArguments.validateUserVariableName(name);
    } catch (IllegalArgumentException failure) {
      throw unsupported(path, failure.getMessage());
    }
  }

  private static List<SwitchCasePlan> compileSwitchCases(
      JsonNode cases, String path, ExpressionMode expressionMode) {
    List<SwitchCasePlan> result = new ArrayList<>();
    Set<String> names = new java.util.HashSet<>();
    int defaultCases = 0;
    for (int index = 0; index < cases.size(); index++) {
      JsonNode item = cases.get(index);
      Map.Entry<String, JsonNode> named = item.properties().iterator().next();
      String name = named.getKey();
      JsonNode definition = named.getValue();
      String casePath = path + "/" + index + "/" + escape(name);
      if (!names.add(name)) {
        throw unsupported(path, "switch case names must be unique: " + name);
      }
      String condition = definition.path("when").asText(null);
      if (condition == null) {
        defaultCases++;
        if (defaultCases > 1) {
          throw unsupported(casePath, "a switch may define only one default case");
        }
      } else {
        condition = requiredExpression(condition);
        validateExpression(condition, expressionMode, casePath + "/when");
      }
      result.add(new SwitchCasePlan(name, condition, definition.required("then").textValue()));
    }
    return List.copyOf(result);
  }

  private static WorkflowExpressionConfiguration expressionConfiguration(JsonNode root) {
    JsonNode evaluate = root.get("evaluate");
    String language = evaluate == null ? "jq" : evaluate.path("language").asText("jq");
    if (!"jq".equals(language)) {
      throw unsupported(
          "/evaluate/language", "expression language '" + language + "' is not implemented");
    }
    ExpressionMode mode;
    try {
      mode = ExpressionMode.parse(evaluate == null ? null : evaluate.path("mode").asText(null));
    } catch (IllegalArgumentException failure) {
      throw unsupported("/evaluate/mode", failure.getMessage());
    }
    return new WorkflowExpressionConfiguration(language, mode);
  }

  private static WorkflowDataFlow workflowDataFlow(
      JsonNode root, ExpressionMode mode, Map<URI, ResolvedWorkflowResource> resources) {
    return new WorkflowDataFlow(
        dataSchema(root, "input", "/input", resources),
        dataTransform(root, "input", "from", mode, "/input"),
        dataTransform(root, "output", "as", mode, "/output"),
        dataSchema(root, "output", "/output", resources));
  }

  private static TaskDataFlow taskDataFlow(
      JsonNode task,
      String path,
      ExpressionMode mode,
      Map<URI, ResolvedWorkflowResource> resources) {
    String condition = task.path("if").asText(null);
    if (condition != null) {
      condition = requiredExpression(condition);
      validateExpression(condition, mode, path + "/if");
    }
    String then = task.path("then").asText("continue");
    return new TaskDataFlow(
        condition,
        dataSchema(task, "input", path + "/input", resources),
        dataTransform(task, "input", "from", mode, path + "/input"),
        dataTransform(task, "output", "as", mode, path + "/output"),
        dataSchema(task, "output", path + "/output", resources),
        dataTransform(task, "export", "as", mode, path + "/export"),
        dataSchema(task, "export", path + "/export", resources),
        then);
  }

  private static JsonNode dataTransform(
      JsonNode owner, String sectionName, String transformName, ExpressionMode mode, String path) {
    JsonNode section = owner.get(sectionName);
    if (section == null) {
      return null;
    }
    JsonNode transform = section.get(transformName);
    if (transform != null) {
      if (transform.isTextual()) {
        transform = JsonNodeFactory.instance.textNode(requiredExpression(transform.textValue()));
      }
      validateTransform(transform, mode, path + "/" + transformName);
    }
    return transform;
  }

  private static ResolvedDataSchema dataSchema(
      JsonNode owner,
      String sectionName,
      String path,
      Map<URI, ResolvedWorkflowResource> resources) {
    JsonNode section = owner.get(sectionName);
    if (section == null || !section.has("schema")) {
      return null;
    }
    JsonNode schema = section.required("schema");
    String format = schema.path("format").asText("json");
    if (!format.equals("json") && !format.startsWith("json:")) {
      throw unsupported(
          path + "/schema/format", "schema format '" + format + "' is not implemented");
    }
    if (schema.has("document")) {
      JsonNode document = schema.required("document");
      String digest = sha256(canonicalJson(document));
      return new ResolvedDataSchema(path + "/schema", format, null, digest, document);
    }

    JsonNode resource = schema.required("resource");
    URI uri = WorkflowResourceResolver.literalEndpoint(resource.required("endpoint"));
    ResolvedWorkflowResource resolved = resources.get(withoutFragment(uri));
    if (resolved == null) {
      throw unsupported(
          path + "/schema/resource",
          "external schema " + uri + " was not resolved before publication");
    }
    JsonNode document = WorkflowResourceResolver.parseSchema(resolved);
    return new ResolvedDataSchema(path + "/schema", format, uri, resolved.sha256(), document);
  }

  private static void validateTransform(JsonNode transform, ExpressionMode mode, String path) {
    try {
      if (transform.isTextual()) {
        EXPRESSIONS.validateExpression(transform.textValue(), mode);
      } else {
        EXPRESSIONS.validateTemplate(transform, mode);
      }
    } catch (RuntimeExpressionException failure) {
      throw unsupported(path, failure.getMessage());
    }
  }

  private static void validateExpression(String expression, ExpressionMode mode, String path) {
    try {
      EXPRESSIONS.validateExpression(expression, mode);
    } catch (RuntimeExpressionException failure) {
      throw unsupported(path, failure.getMessage());
    }
  }

  private static void validateFlowTargets(List<PlanStep> steps, String listPath) {
    Set<String> names = new java.util.HashSet<>();
    for (PlanStep step : steps) {
      if (!names.add(step.name())) {
        throw unsupported(listPath, "task names must be unique within one scope: " + step.name());
      }
    }
    for (PlanStep step : steps) {
      String directive = step.dataFlow().thenDirective();
      validateFlowTarget(directive, names, step.path() + "/then");
      for (int index = 0; index < step.switchCases().size(); index++) {
        SwitchCasePlan switchCase = step.switchCases().get(index);
        validateFlowTarget(
            switchCase.thenDirective(),
            names,
            step.path() + "/switch/" + index + "/" + escape(switchCase.name()) + "/then");
      }
      if (step.kind() == PlanStepKind.TRY && step.tryPlan().catchPlan().thenDirective() != null) {
        validateFlowTarget(
            step.tryPlan().catchPlan().thenDirective(), names, step.path() + "/catch/then");
      }
    }
  }

  private static void validateFlowTarget(String directive, Set<String> names, String path) {
    if (!Set.of("continue", "exit", "end").contains(directive) && !names.contains(directive)) {
      throw unsupported(path, "flow target '" + directive + "' does not exist in the same scope");
    }
  }

  private static List<ResolvedWorkflowResource> validatedResources(
      Collection<ResolvedWorkflowResource> resources) {
    Map<URI, ResolvedWorkflowResource> byUri = new java.util.LinkedHashMap<>();
    long totalBytes = 0;
    for (ResolvedWorkflowResource resource : resources) {
      Objects.requireNonNull(resource, "workflow resource");
      if (byUri.putIfAbsent(resource.uri(), resource) != null) {
        throw unsupported("/", "duplicate resolved schema resource " + resource.uri());
      }
      totalBytes += resource.content().getBytes(StandardCharsets.UTF_8).length;
      if (totalBytes > WorkflowResourceResolver.MAX_TOTAL_BYTES) {
        throw unsupported(
            "/",
            "resolved workflow resources exceed "
                + WorkflowResourceResolver.MAX_TOTAL_BYTES
                + " bytes");
      }
    }
    if (byUri.size() > WorkflowResourceResolver.MAX_RESOURCES) {
      throw unsupported(
          "/", "resolved workflow resources exceed " + WorkflowResourceResolver.MAX_RESOURCES);
    }
    return List.copyOf(byUri.values());
  }

  private static void validateSchemas(
      WorkflowDataFlow workflow,
      SchedulePlan schedule,
      List<PlanStep> steps,
      List<ResolvedWorkflowResource> resources) {
    List<ResolvedDataSchema> schemas = new ArrayList<>();
    addSchema(schemas, workflow.inputSchema());
    addSchema(schemas, workflow.outputSchema());
    if (schedule != null) {
      collectEventSchemas(schedule.on(), schemas);
    }
    collectSchemas(steps, schemas);
    Map<URI, ResolvedWorkflowResource> byUri = new HashMap<>();
    resources.forEach(resource -> byUri.put(resource.uri(), resource));
    Set<URI> schemaResourceUris = new java.util.HashSet<>();
    for (ResolvedDataSchema schema : schemas) {
      URI schemaBase = schema.external() ? schema.resourceUri() : inlineBase(schema);
      if (schema.external()) {
        schemaResourceUris.add(withoutFragment(schema.resourceUri()));
      }
      validateReferences(
          schema.document(),
          schemaBase,
          schemaBase,
          byUri,
          new java.util.HashSet<>(),
          schemaResourceUris,
          declaredSchemaResourceUris(schema.document(), schemaBase));
    }
    var validator =
        new DataSchemaValidator(
            resources.stream()
                .filter(resource -> schemaResourceUris.contains(resource.uri()))
                .toList());
    for (ResolvedDataSchema schema : schemas) {
      try {
        validator.compile(schema);
      } catch (RuntimeException failure) {
        throw unsupported(schema.definitionPath(), "invalid JSON Schema: " + rootMessage(failure));
      }
    }
    Set<URI> usedResources = new java.util.HashSet<>(schemaResourceUris);
    collectOperationResources(steps, usedResources, byUri);
    for (ResolvedWorkflowResource resource : resources) {
      if (!usedResources.contains(resource.uri())) {
        throw unsupported(
            "/",
            "resolved workflow resource " + resource.uri() + " is not referenced by the workflow");
      }
    }
  }

  private static void collectOperationResources(
      List<PlanStep> steps, Set<URI> usedResources, Map<URI, ResolvedWorkflowResource> resources) {
    for (PlanStep step : steps) {
      if (step.callPlan() != null && step.callPlan().resource() != null) {
        collectOperationResourceGraph(
            step.callPlan().resource(), usedResources, resources, new java.util.HashSet<>());
      }
      if (step.runPlan() != null && step.runPlan().resource() != null) {
        collectOperationResourceGraph(
            step.runPlan().resource(), usedResources, resources, new java.util.HashSet<>());
      }
      collectOperationResources(step.children(), usedResources, resources);
    }
  }

  /**
   * Marks the exact immutable resource graph reachable from one compiled operation resource.
   *
   * <p>The publication resolver already loads these resources. This second traversal is
   * deliberately independent: compilation must prove that every supplied byte is reachable from the
   * admitted workflow and must not trust a caller-provided resource list merely because it was
   * labelled "resolved".
   */
  private static void collectOperationResourceGraph(
      WorkflowResourceReference reference,
      Set<URI> usedResources,
      Map<URI, ResolvedWorkflowResource> resources,
      Set<URI> visiting) {
    URI uri = withoutFragment(reference.uri());
    ResolvedWorkflowResource resource = resources.get(uri);
    if (resource == null) {
      throw unsupported("/", "operation resource graph is missing " + uri);
    }
    if (!resource.sha256().equals(reference.sha256())) {
      throw unsupported("/", "operation resource digest does not match " + uri);
    }
    collectOperationResourceGraph(resource, reference.kind(), usedResources, resources, visiting);
  }

  private static void collectOperationResourceGraph(
      ResolvedWorkflowResource resource,
      WorkflowResourceKind kind,
      Set<URI> usedResources,
      Map<URI, ResolvedWorkflowResource> resources,
      Set<URI> visiting) {
    URI uri = withoutFragment(resource.uri());
    usedResources.add(uri);
    if (!visiting.add(uri)) {
      return;
    }
    try {
      switch (kind) {
        case OPEN_API_DOCUMENT, ASYNC_API_DOCUMENT ->
            collectOperationDocumentReferences(
                WorkflowResourceResolver.parseDocument(resource, "API document"),
                uri,
                kind,
                usedResources,
                resources,
                visiting);
        case GRPC_PROTO -> {
          Matcher imports = PROTO_IMPORT.matcher(resource.content());
          while (imports.find()) {
            String imported = imports.group(1);
            if (imported.startsWith("google/protobuf/")) {
              continue;
            }
            URI importedUri = withoutFragment(uri.resolve(imported).normalize());
            ResolvedWorkflowResource importedResource = resources.get(importedUri);
            if (importedResource == null) {
              throw unsupported(
                  "/", "proto import " + importedUri + " was not resolved before " + "publication");
            }
            collectOperationResourceGraph(
                importedResource,
                WorkflowResourceKind.GRPC_PROTO,
                usedResources,
                resources,
                visiting);
          }
        }
        case DATA_SCHEMA -> {
          // Schema graphs are validated by validateReferences.
        }
        case FUNCTION_DEFINITION, A2A_AGENT_CARD, SCRIPT_SOURCE -> {
          // Nested function calls are represented as compiled child
          // steps. Agent cards and script sources have no transitive
          // resource syntax.
        }
      }
    } finally {
      visiting.remove(uri);
    }
  }

  private static void collectOperationDocumentReferences(
      JsonNode node,
      URI base,
      WorkflowResourceKind kind,
      Set<URI> usedResources,
      Map<URI, ResolvedWorkflowResource> resources,
      Set<URI> visiting) {
    if (node.isObject()) {
      URI effectiveBase = base;
      JsonNode id = node.get("$id");
      if (id != null && id.isTextual()) {
        try {
          effectiveBase = base.resolve(id.textValue());
        } catch (IllegalArgumentException invalid) {
          throw unsupported("/", "API document contains malformed $id " + id.textValue());
        }
      }
      for (String field : List.of("$ref", "$dynamicRef")) {
        JsonNode reference = node.get(field);
        if (reference == null || !reference.isTextual()) {
          continue;
        }
        final URI target;
        try {
          target = effectiveBase.resolve(reference.textValue());
        } catch (IllegalArgumentException invalid) {
          throw unsupported(
              "/", "API document contains malformed " + field + " " + reference.textValue());
        }
        URI targetResource = withoutFragment(target);
        if (!targetResource.equals(withoutFragment(base))) {
          ResolvedWorkflowResource resolved = resources.get(targetResource);
          if (resolved == null) {
            throw unsupported(
                "/",
                "API document reference "
                    + targetResource
                    + " was not resolved before "
                    + "publication");
          }
          collectOperationResourceGraph(resolved, kind, usedResources, resources, visiting);
        }
      }
      URI childBase = effectiveBase;
      node.properties()
          .forEach(
              entry ->
                  collectOperationDocumentReferences(
                      entry.getValue(), childBase, kind, usedResources, resources, visiting));
    } else if (node.isArray()) {
      node.forEach(
          child ->
              collectOperationDocumentReferences(
                  child, base, kind, usedResources, resources, visiting));
    }
  }

  private static void collectSchemas(List<PlanStep> steps, List<ResolvedDataSchema> schemas) {
    for (PlanStep step : steps) {
      addSchema(schemas, step.dataFlow().inputSchema());
      addSchema(schemas, step.dataFlow().outputSchema());
      addSchema(schemas, step.dataFlow().exportSchema());
      if (step.listenPlan() != null) {
        collectEventSchemas(step.listenPlan().consumption(), schemas);
      }
      collectSchemas(step.children(), schemas);
    }
  }

  private static void collectEventSchemas(
      EventConsumptionPlan consumption, List<ResolvedDataSchema> schemas) {
    if (consumption == null) {
      return;
    }
    consumption.filters().forEach(filter -> addSchema(schemas, filter.dataSchema()));
    collectEventSchemas(consumption.untilConsumed(), schemas);
  }

  private static void addSchema(List<ResolvedDataSchema> schemas, ResolvedDataSchema schema) {
    if (schema != null) schemas.add(schema);
  }

  private static URI inlineBase(ResolvedDataSchema schema) {
    JsonNode id = schema.document().get("$id");
    if (id != null && id.isTextual()) {
      try {
        URI uri = URI.create(id.textValue());
        if (uri.isAbsolute()) return uri;
      } catch (IllegalArgumentException ignored) {
        // The JSON Schema validator reports malformed $id values.
      }
    }
    return URI.create("urn:oks:inline-schema:" + schema.sha256());
  }

  private static void validateReferences(
      JsonNode node,
      URI base,
      URI containingResource,
      Map<URI, ResolvedWorkflowResource> resources,
      Set<URI> visitedResources,
      Set<URI> usedResources,
      Set<URI> localResourceUris) {
    if (node.isObject()) {
      URI effectiveBase = base;
      JsonNode id = node.get("$id");
      if (id != null && id.isTextual()) {
        effectiveBase = base.resolve(id.textValue());
      }
      for (String field : List.of("$ref", "$dynamicRef")) {
        JsonNode reference = node.get(field);
        if (reference == null || !reference.isTextual()) {
          continue;
        }
        URI target = effectiveBase.resolve(reference.textValue());
        URI targetResource = withoutFragment(target);
        if (!localResourceUris.contains(targetResource)) {
          ResolvedWorkflowResource resolved = resources.get(targetResource);
          if (resolved == null) {
            throw unsupported(
                "/", "schema reference " + targetResource + " was not resolved before publication");
          }
          usedResources.add(targetResource);
          if (visitedResources.add(targetResource)) {
            JsonNode resolvedDocument = WorkflowResourceResolver.parseSchema(resolved);
            validateReferences(
                resolvedDocument,
                targetResource,
                targetResource,
                resources,
                visitedResources,
                usedResources,
                declaredSchemaResourceUris(resolvedDocument, targetResource));
          }
        }
      }
      URI childBase = effectiveBase;
      node.properties()
          .forEach(
              entry ->
                  validateReferences(
                      entry.getValue(),
                      childBase,
                      containingResource,
                      resources,
                      visitedResources,
                      usedResources,
                      localResourceUris));
    } else if (node.isArray()) {
      node.forEach(
          child ->
              validateReferences(
                  child,
                  base,
                  containingResource,
                  resources,
                  visitedResources,
                  usedResources,
                  localResourceUris));
    }
  }

  /**
   * Collects the retrieval URI and every embedded JSON Schema resource introduced by {@code $id}. A
   * schema may be fetched from an internal, immutable service URI while retaining a public
   * canonical {@code $id}; fragment references against that identifier still belong to the same
   * supplied document and must never trigger a second network fetch.
   */
  private static Set<URI> declaredSchemaResourceUris(JsonNode document, URI retrievalUri) {
    Set<URI> declared = new java.util.HashSet<>();
    declared.add(withoutFragment(retrievalUri));
    collectDeclaredSchemaResourceUris(document, retrievalUri, declared);
    return Set.copyOf(declared);
  }

  private static void collectDeclaredSchemaResourceUris(
      JsonNode node, URI base, Set<URI> declared) {
    if (node.isObject()) {
      URI effectiveBase = base;
      JsonNode id = node.get("$id");
      if (id != null && id.isTextual()) {
        effectiveBase = base.resolve(id.textValue());
        declared.add(withoutFragment(effectiveBase));
      }
      URI childBase = effectiveBase;
      node.properties()
          .forEach(
              entry -> collectDeclaredSchemaResourceUris(entry.getValue(), childBase, declared));
    } else if (node.isArray()) {
      node.forEach(child -> collectDeclaredSchemaResourceUris(child, base, declared));
    }
  }

  private static URI withoutFragment(URI uri) {
    if (uri.getFragment() == null) return uri.normalize();
    return URI.create(uri.toString().substring(0, uri.toString().indexOf('#'))).normalize();
  }

  private static byte[] canonicalJson(JsonNode node) {
    try {
      return new ObjectMapper().writeValueAsBytes(node);
    } catch (IOException impossible) {
      throw new IllegalStateException("Validated JSON cannot be serialized", impossible);
    }
  }

  private static String rootMessage(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
  }

  private static WorkflowDefinitionException unsupported(String path, String detail) {
    return new WorkflowDefinitionException(List.of(path + ": " + detail));
  }

  private static String escape(String token) {
    return token.replace("~", "~0").replace("/", "~1");
  }

  private static Schema loadSchema() {
    try (InputStream input = OpenWorkflowCompiler.class.getResourceAsStream(SCHEMA_RESOURCE)) {
      if (input == null) {
        throw new IllegalStateException("Missing " + SCHEMA_RESOURCE);
      }
      byte[] bytes = input.readAllBytes();
      String actual = sha256(bytes);
      if (!SCHEMA_SHA256.equals(actual)) {
        throw new IllegalStateException(
            "OpenWorkflow 1.0.3 schema drift: expected " + SCHEMA_SHA256 + " but found " + actual);
      }
      String content = new String(bytes, StandardCharsets.UTF_8);
      // NetworkNT's YAML 1.1 loader interprets the unquoted scalar `on`
      // in the SDK 7.29 schema's dependentRequired array as boolean true.
      // Quoting it preserves the exact JSON Schema property name.
      content = content.replace("read: [ on ]", "read: [ \"on\" ]");
      return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
          .getSchema(content, InputFormat.YAML);
    } catch (IOException failure) {
      throw new IllegalStateException("Unable to load pinned OpenWorkflow schema", failure);
    }
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
