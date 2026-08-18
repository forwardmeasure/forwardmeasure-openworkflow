package com.forwardmeasure.openworkflow.operation.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pekko.Done;

/** Dynamic gRPC transport compiled exclusively from the persisted pinned proto. */
public final class DynamicGrpcOperationExecutor implements ProtocolOperationExecutor {
  private final Duration timeout;
  private final Clock clock;
  private final ChannelFactory channels;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final Map<String, CompiledMethod> methods = new ConcurrentHashMap<>();

  public DynamicGrpcOperationExecutor(Duration timeout) {
    this(
        timeout,
        (tenant, destination) -> {
          throw new SecurityException("gRPC egress policy is not configured");
        },
        SecretProvider.rejecting());
  }

  public DynamicGrpcOperationExecutor(Duration timeout, HttpEgressPolicy egress) {
    this(timeout, egress, SecretProvider.rejecting());
  }

  public DynamicGrpcOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), DynamicGrpcOperationExecutor::channel, egress, secrets);
  }

  DynamicGrpcOperationExecutor(Duration timeout, Clock clock, ChannelFactory channels) {
    this(timeout, clock, channels, HttpEgressPolicy.allowAllForTesting());
  }

  DynamicGrpcOperationExecutor(
      Duration timeout, Clock clock, ChannelFactory channels, HttpEgressPolicy egress) {
    this(timeout, clock, channels, egress, SecretProvider.rejecting());
  }

  DynamicGrpcOperationExecutor(
      Duration timeout,
      Clock clock,
      ChannelFactory channels,
      HttpEgressPolicy egress,
      SecretProvider secrets) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.channels = Objects.requireNonNull(channels, "channels");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.authentication =
        new HttpAuthenticationSupport(
            HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
            new com.fasterxml.jackson.databind.ObjectMapper(),
            timeout,
            egress,
            Objects.requireNonNull(secrets, "secrets"));
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException("gRPC timeout must be positive");
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.GRPC) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Dynamic gRPC executor received a non-gRPC operation"));
    }
    try {
      egress.authorize(executionId.tenantId(), operation.endpoint());
      CompiledMethod compiled =
          methods.computeIfAbsent(
              operation.document().sha256() + "|" + operation.protocolDependencies().hashCode(),
              ignored -> compile(operation));
      AuthenticationPlan configured = operation.authentication();
      if (configured != null && configured.kind() == AuthenticationPlan.Kind.DIGEST) {
        throw new IllegalArgumentException("Digest authentication is not valid for gRPC metadata");
      }
      var owned = new CompletableFuture<Done>();
      var active = new java.util.concurrent.atomic.AtomicReference<CompletableFuture<Done>>();
      CompletableFuture<HttpAuthenticationSupport.Credential> resolving =
          authentication
              .resolve(
                  executionId,
                  configured,
                  operation.authenticationContext(),
                  operation.operationId())
              .toCompletableFuture();
      owned.whenComplete(
          (done, failure) -> {
            if (owned.isCancelled()) {
              resolving.cancel(true);
              CompletableFuture<Done> running = active.get();
              if (running != null) running.cancel(true);
            }
          });
      resolving.whenComplete(
          (credential, authenticationFailure) -> {
            if (owned.isDone()) return;
            CompletionStage<Done> next =
                authenticationFailure == null
                    ? executeAuthenticated(operation, sink, compiled, credential)
                    : observeFailure(operation, sink, root(authenticationFailure));
            CompletableFuture<Done> running = next.toCompletableFuture();
            active.set(running);
            if (owned.isCancelled()) {
              running.cancel(true);
              return;
            }
            running.whenComplete(
                (done, failure) -> {
                  if (failure == null) owned.complete(done);
                  else owned.completeExceptionally(failure);
                });
          });
      return owned;
    } catch (Exception failure) {
      return observeFailure(operation, sink, failure);
    }
  }

  private CompletionStage<Done> executeAuthenticated(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      CompiledMethod compiled,
      HttpAuthenticationSupport.Credential credential) {
    try {
      ManagedChannel channel = channels.open(operation);
      Channel callChannel = authenticatedChannel(channel, credential);
      var result = new CompletableFuture<Done>();
      var responseObserver = new DurableResponseObserver(operation, sink, result, clock);
      CallOptions options =
          CallOptions.DEFAULT.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS);
      invoke(callChannel, compiled, operation.request(), responseObserver, options);
      var owned = new CompletableFuture<Done>();
      result.whenComplete(
          (done, failure) -> {
            channel.shutdown();
            if (failure == null) owned.complete(done);
            else owned.completeExceptionally(failure);
          });
      owned.whenComplete(
          (done, failure) -> {
            if (owned.isCancelled()) channel.shutdownNow();
          });
      return owned;
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletionStage<Done> observeFailure(
      ProtocolOperationDescriptor operation, ObservationSink sink, Throwable failure) {
    return sink.observe(
            clock.instant().toString(), problem(operation, failure), true, true, clock.instant())
        .thenApply(ignored -> Done.getInstance());
  }

  private static void invoke(
      Channel channel,
      CompiledMethod compiled,
      JsonNode request,
      StreamObserver<DynamicMessage> responseObserver,
      CallOptions options)
      throws Exception {
    var call = channel.newCall(compiled.method(), options);
    switch (compiled.method().getType()) {
      case UNARY ->
          ClientCalls.asyncUnaryCall(call, message(compiled.request(), request), responseObserver);
      case SERVER_STREAMING ->
          ClientCalls.asyncServerStreamingCall(
              call, message(compiled.request(), request), responseObserver);
      case CLIENT_STREAMING ->
          sendStream(
              ClientCalls.asyncClientStreamingCall(call, responseObserver),
              compiled.request(),
              request);
      case BIDI_STREAMING ->
          sendStream(
              ClientCalls.asyncBidiStreamingCall(call, responseObserver),
              compiled.request(),
              request);
      default ->
          throw new IllegalArgumentException(
              "Unsupported gRPC method type " + compiled.method().getType());
    }
  }

  private Channel authenticatedChannel(
      ManagedChannel channel, HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return channel;
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
        credential.authorization());
    return ClientInterceptors.intercept(
        channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private static Throwable root(Throwable failure) {
    Throwable current = failure;
    while (current instanceof java.util.concurrent.CompletionException
        && current.getCause() != null) current = current.getCause();
    return current;
  }

  private static void sendStream(
      StreamObserver<DynamicMessage> requests, Descriptors.Descriptor descriptor, JsonNode request)
      throws Exception {
    if (!request.isArray())
      throw new IllegalArgumentException(
          "A client-streaming gRPC request must evaluate to an array");
    for (JsonNode item : request) requests.onNext(message(descriptor, item));
    requests.onCompleted();
  }

  private static DynamicMessage message(Descriptors.Descriptor descriptor, JsonNode json)
      throws Exception {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    JsonFormat.parser().ignoringUnknownFields().merge(json.toString(), builder);
    return builder.build();
  }

  private static CompiledMethod compile(ProtocolOperationDescriptor operation) {
    Path directory = null;
    try {
      directory = Files.createTempDirectory("openworkflow-grpc-schema-");
      Path source = directory.resolve("operation.proto");
      Path descriptors = directory.resolve("operation.pb");
      Files.writeString(source, operation.protocolSchema(), StandardCharsets.UTF_8);
      for (Map.Entry<String, String> dependency : operation.protocolDependencies().entrySet()) {
        String name = dependency.getKey();
        Path relative = Path.of(name).normalize();
        if (name.contains("\\") || relative.isAbsolute() || relative.startsWith("..")) {
          throw new IllegalArgumentException("Unsafe pinned proto import path: " + name);
        }
        Path target = directory.resolve(relative).normalize();
        if (!target.startsWith(directory))
          throw new IllegalArgumentException("Unsafe pinned proto import path: " + name);
        if (target.getParent() != null) Files.createDirectories(target.getParent());
        Files.writeString(target, dependency.getValue(), StandardCharsets.UTF_8);
      }
      int exit =
          Protoc.runProtoc(
              new String[] {
                "--descriptor_set_out=" + descriptors,
                "--include_imports",
                "--proto_path=" + directory,
                source.toString()
              });
      if (exit != 0)
        throw new IllegalArgumentException(
            "Pinned proto could not be compiled (protoc exit " + exit + ")");
      DescriptorProtos.FileDescriptorSet set =
          DescriptorProtos.FileDescriptorSet.parseFrom(Files.readAllBytes(descriptors));
      Map<String, DescriptorProtos.FileDescriptorProto> remaining = new HashMap<>();
      set.getFileList().forEach(file -> remaining.put(file.getName(), file));
      Map<String, Descriptors.FileDescriptor> compiled = new HashMap<>();
      while (!remaining.isEmpty()) {
        boolean progressed = false;
        for (var iterator = remaining.entrySet().iterator(); iterator.hasNext(); ) {
          var entry = iterator.next();
          if (!compiled.keySet().containsAll(entry.getValue().getDependencyList())) {
            continue;
          }
          var dependencies =
              entry.getValue().getDependencyList().stream()
                  .map(compiled::get)
                  .toArray(Descriptors.FileDescriptor[]::new);
          compiled.put(
              entry.getKey(), Descriptors.FileDescriptor.buildFrom(entry.getValue(), dependencies));
          iterator.remove();
          progressed = true;
        }
        if (!progressed)
          throw new IllegalArgumentException("Pinned proto dependency graph cannot be linked");
      }
      String[] operationName = operation.operation().split("/", 2);
      if (operationName.length != 2)
        throw new IllegalArgumentException("gRPC operation must be service/method");
      for (Descriptors.FileDescriptor file : compiled.values()) {
        Descriptors.ServiceDescriptor service =
            file.findServiceByName(
                operationName[0].substring(operationName[0].lastIndexOf('.') + 1));
        if (service == null || !service.getFullName().equals(operationName[0])) continue;
        Descriptors.MethodDescriptor rpc = service.findMethodByName(operationName[1]);
        if (rpc == null) break;
        MethodDescriptor.MethodType type =
            rpc.isClientStreaming()
                ? rpc.isServerStreaming()
                    ? MethodDescriptor.MethodType.BIDI_STREAMING
                    : MethodDescriptor.MethodType.CLIENT_STREAMING
                : rpc.isServerStreaming()
                    ? MethodDescriptor.MethodType.SERVER_STREAMING
                    : MethodDescriptor.MethodType.UNARY;
        MethodDescriptor<DynamicMessage, DynamicMessage> method =
            MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(type)
                .setFullMethodName(
                    MethodDescriptor.generateFullMethodName(service.getFullName(), rpc.getName()))
                .setRequestMarshaller(
                    ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getInputType())))
                .setResponseMarshaller(
                    ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getOutputType())))
                .build();
        return new CompiledMethod(method, rpc.getInputType());
      }
      throw new IllegalArgumentException(
          "Pinned proto operation cannot be linked: " + operation.operation());
    } catch (Exception failure) {
      if (failure instanceof RuntimeException runtime) throw runtime;
      throw new IllegalArgumentException("Pinned proto compilation failed", failure);
    } finally {
      if (directory != null) delete(directory);
    }
  }

  private static ManagedChannel channel(ProtocolOperationDescriptor operation) {
    int port = operation.endpoint().getPort() < 0 ? 443 : operation.endpoint().getPort();
    ManagedChannelBuilder<?> builder =
        ManagedChannelBuilder.forAddress(operation.endpoint().getHost(), port);
    if ("grpcs".equals(operation.protocol())) builder.useTransportSecurity();
    else builder.usePlaintext();
    return builder.build();
  }

  private static void delete(Path directory) {
    try (var paths = Files.walk(directory)) {
      paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
              });
    } catch (Exception ignored) {
    }
  }

  private static ObjectNode problem(ProtocolOperationDescriptor operation, Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:grpc:transport")
        .put("status", 502)
        .put("title", "gRPC operation failed")
        .put(
            "detail",
            root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage())
        .put("instance", "urn:openworkflow:operation:" + operation.operationId());
  }

  private record CompiledMethod(
      MethodDescriptor<DynamicMessage, DynamicMessage> method, Descriptors.Descriptor request) {}

  @FunctionalInterface
  interface ChannelFactory {
    ManagedChannel open(ProtocolOperationDescriptor operation);
  }

  private static final class DurableResponseObserver implements StreamObserver<DynamicMessage> {
    private final ProtocolOperationDescriptor operation;
    private final ObservationSink sink;
    private final CompletableFuture<Done> result;
    private final Clock clock;
    private CompletionStage<ObservationDisposition> tail =
        CompletableFuture.completedFuture(ObservationDisposition.CONTINUE);

    private DurableResponseObserver(
        ProtocolOperationDescriptor operation,
        ObservationSink sink,
        CompletableFuture<Done> result,
        Clock clock) {
      this.operation = operation;
      this.sink = sink;
      this.result = result;
      this.clock = clock;
    }

    @Override
    public synchronized void onNext(DynamicMessage value) {
      try {
        JsonNode item =
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(JsonFormat.printer().print(value));
        tail =
            tail.thenCompose(
                disposition ->
                    disposition == ObservationDisposition.STOP
                        ? CompletableFuture.completedFuture(disposition)
                        : sink.observe(
                            "response-" + sequence++, item, false, false, clock.instant()));
        tail.thenAccept(
            disposition -> {
              if (disposition == ObservationDisposition.STOP) complete(null);
            });
      } catch (Exception failure) {
        onError(failure);
      }
    }

    @Override
    public synchronized void onError(Throwable failure) {
      tail =
          tail.thenCompose(
              disposition ->
                  disposition == ObservationDisposition.STOP
                      ? CompletableFuture.completedFuture(disposition)
                      : sink.observe(
                          "error-" + sequence,
                          problem(operation, failure),
                          true,
                          true,
                          clock.instant()));
      tail.whenComplete((done, rejected) -> complete(rejected));
    }

    @Override
    public synchronized void onCompleted() {
      tail =
          tail.thenCompose(
              disposition ->
                  disposition == ObservationDisposition.STOP
                      ? CompletableFuture.completedFuture(disposition)
                      : sink.observe(
                          "terminal-" + sequence,
                          JsonNodeFactory.instance.nullNode(),
                          false,
                          true,
                          clock.instant()));
      tail.whenComplete((done, rejected) -> complete(rejected));
    }

    private void complete(Throwable failure) {
      if (failure == null) result.complete(Done.getInstance());
      else result.completeExceptionally(failure);
    }

    private long sequence;
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
