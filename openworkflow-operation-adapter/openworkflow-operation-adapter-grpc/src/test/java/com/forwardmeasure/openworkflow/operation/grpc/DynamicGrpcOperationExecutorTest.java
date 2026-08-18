package com.forwardmeasure.openworkflow.operation.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationObservation;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;
import org.junit.jupiter.api.Test;

final class DynamicGrpcOperationExecutorTest {
  @Test
  void cancellingTheOwnedTransportActivelyCancelsTheGrpcCall() throws Exception {
    Descriptors.FileDescriptor file = descriptor();
    Descriptors.ServiceDescriptor service = file.findServiceByName("Classifier");
    Descriptors.MethodDescriptor rpc = service.findMethodByName("Watch");
    MethodDescriptor<DynamicMessage, DynamicMessage> method =
        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
            .setFullMethodName("evidence.Classifier/Watch")
            .setRequestMarshaller(
                ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getInputType())))
            .setResponseMarshaller(
                ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getOutputType())))
            .build();
    var cancelled = new CountDownLatch(1);
    String serverName = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(
                ServerServiceDefinition.builder(service.getFullName())
                    .addMethod(
                        method,
                        ServerCalls.asyncServerStreamingCall(
                            (ServerCalls.ServerStreamingMethod<DynamicMessage, DynamicMessage>)
                                (request, responses) ->
                                    ((ServerCallStreamObserver<DynamicMessage>) responses)
                                        .setOnCancelHandler(cancelled::countDown)))
                    .build())
            .build()
            .start();
    try {
      var executor =
          new DynamicGrpcOperationExecutor(
              Duration.ofSeconds(30),
              Clock.systemUTC(),
              ignored -> InProcessChannelBuilder.forName(serverName).directExecutor().build());
      CompletableFuture<Done> transport =
          executor
              .execute(
                  new ExecutionId(
                      new TenantId("did:web:forwardmeasure.com:tenant:grpc"), UUID.randomUUID()),
                  operation(),
                  (id, value, failed, terminal, at) ->
                      CompletableFuture.completedFuture(
                          com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                              .ObservationDisposition.CONTINUE))
              .toCompletableFuture();

      assertTrue(transport.cancel(true));
      assertTrue(cancelled.await(3, TimeUnit.SECONDS));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void invokesPinnedServerStreamAndBackpressuresStableObservations() throws Exception {
    Descriptors.FileDescriptor file = descriptor();
    Descriptors.ServiceDescriptor service = file.findServiceByName("Classifier");
    Descriptors.MethodDescriptor rpc = service.findMethodByName("Watch");
    MethodDescriptor<DynamicMessage, DynamicMessage> method =
        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
            .setFullMethodName("evidence.Classifier/Watch")
            .setRequestMarshaller(
                ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getInputType())))
            .setResponseMarshaller(
                ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(rpc.getOutputType())))
            .build();
    String serverName = InProcessServerBuilder.generateName();
    var authorization = new AtomicReference<String>();
    Server server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(
                ServerInterceptors.intercept(
                    ServerServiceDefinition.builder(service.getFullName())
                        .addMethod(
                            method,
                            ServerCalls.asyncServerStreamingCall(
                                (ServerCalls.ServerStreamingMethod<DynamicMessage, DynamicMessage>)
                                    (request, responses) -> {
                                      assertEquals(
                                          "ev-42",
                                          request.getField(
                                              rpc.getInputType().findFieldByName("id")));
                                      responses.onNext(result(rpc, "first"));
                                      responses.onNext(result(rpc, "second"));
                                      responses.onCompleted();
                                    }))
                        .build(),
                    new ServerInterceptor() {
                      @Override
                      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                          ServerCall<ReqT, RespT> call,
                          Metadata headers,
                          ServerCallHandler<ReqT, RespT> next) {
                        authorization.set(
                            headers.get(
                                Metadata.Key.of(
                                    "authorization", Metadata.ASCII_STRING_MARSHALLER)));
                        return next.startCall(call, headers);
                      }
                    }))
            .build()
            .start();
    try {
      var fixed = Clock.fixed(Instant.parse("2026-08-15T12:00:00Z"), ZoneOffset.UTC);
      var executor =
          new DynamicGrpcOperationExecutor(
              Duration.ofSeconds(5),
              fixed,
              ignored -> InProcessChannelBuilder.forName(serverName).directExecutor().build(),
              com.forwardmeasure.openworkflow.operation.HttpEgressPolicy.allowAllForTesting(),
              (tenant, name) -> "secret".toCharArray());
      var observations = new ArrayList<ProtocolOperationObservation>();
      executor
          .execute(
              new ExecutionId(
                  new TenantId("did:web:forwardmeasure.com:tenant:grpc"), UUID.randomUUID()),
              operation(
                  AuthenticationPlan.expressions(
                      AuthenticationPlan.Kind.BASIC,
                      "grpc-basic",
                      JsonNodeFactory.instance
                          .objectNode()
                          .put("username", "${ $input.username }")
                          .put("password", "${ $secrets[\"grpc-password\"] }"),
                      List.of("grpc-password"))),
              (observationId, value, failed, terminal, observedAt) -> {
                observations.add(
                    new ProtocolOperationObservation(
                        observationId, value, failed, terminal, observedAt));
                return CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                        .ObservationDisposition.CONTINUE);
              })
          .toCompletableFuture()
          .join();

      assertEquals(3, observations.size());
      assertEquals("response-0", observations.get(0).observationId());
      assertEquals("first", observations.get(0).value().required("classification").asText());
      assertEquals("response-1", observations.get(1).observationId());
      assertFalse(observations.get(1).terminal());
      assertEquals("terminal-2", observations.get(2).observationId());
      assertTrue(observations.get(2).terminal());
      assertEquals("Basic YXBpOnNlY3JldA==", authorization.get());

      HttpServer tokens = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      var tokenRequest = new AtomicReference<String>();
      tokens.createContext(
          "/token",
          exchange -> {
            tokenRequest.set(
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body =
                "{\"access_token\":\"grpc-edge-token\",\"token_type\":\"Bearer\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
          });
      tokens.start();
      try {
        URI authority = URI.create("http://127.0.0.1:" + tokens.getAddress().getPort());
        var oauth =
            JsonNodeFactory.instance
                .objectNode()
                .put("authority", authority.toString())
                .put("grant", "client_credentials");
        oauth.putObject("endpoints").put("token", "/token");
        oauth
            .putObject("client")
            .put("id", "grpc-client")
            .put("secret", "${ $secrets[\"grpc-password\"] }")
            .put("authentication", "client_secret_post");
        executor
            .execute(
                new ExecutionId(
                    new TenantId("did:web:forwardmeasure.com:tenant:grpc"), UUID.randomUUID()),
                operation(
                    AuthenticationPlan.expressions(
                        AuthenticationPlan.Kind.OAUTH2,
                        "grpc-oauth",
                        oauth,
                        List.of("grpc-password"))),
                (id, value, failed, terminal, observedAt) ->
                    CompletableFuture.completedFuture(
                        com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor
                            .ObservationDisposition.CONTINUE))
            .toCompletableFuture()
            .join();

        assertTrue(tokenRequest.get().contains("grant_type=client_credentials"));
        assertTrue(tokenRequest.get().contains("client_secret=secret"));
        assertEquals("Bearer grpc-edge-token", authorization.get());
      } finally {
        tokens.stop(0);
      }
    } finally {
      server.shutdownNow();
    }
  }

  private static ProtocolOperationDescriptor operation() {
    return operation(null);
  }

  private static ProtocolOperationDescriptor operation(AuthenticationPlan authentication) {
    AuthenticationExpressionContext authenticationContext =
        authentication != null && !authentication.secretBacked()
            ? new AuthenticationExpressionContext(
                null,
                JsonNodeFactory.instance.objectNode().put("username", "api"),
                null,
                null,
                null,
                null,
                null,
                Map.of())
            : null;
    return new ProtocolOperationDescriptor(
        "grpc-operation",
        ProtocolOperationDescriptor.Kind.GRPC,
        ProtocolOperationDescriptor.Mode.GRPC_SERVER_STREAM,
        new WorkflowResourceReference(
            WorkflowResourceKind.GRPC_PROTO,
            URI.create("https://contracts.example.test/evidence.proto"),
            "b".repeat(64)),
        "grpc",
        URI.create("grpc://unused.test:80"),
        "evidence.Classifier/Watch",
        JsonNodeFactory.instance.objectNode().put("id", "ev-42"),
        null,
        authentication,
        authenticationContext,
        """
        syntax = "proto3";
        package evidence;
        import "models/evidence.proto";
        service Classifier {
          rpc Watch (Evidence) returns (stream Result);
        }
        """,
        null,
        java.util.Map.of(
            "models/evidence.proto",
            """
            syntax = "proto3";
            package evidence;
            message Evidence { string id = 1; }
            message Result { string classification = 1; }
            """));
  }

  private static Descriptors.FileDescriptor descriptor() throws Exception {
    var evidence =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Evidence")
            .addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("id")
                    .setNumber(1)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
    var result =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Result")
            .addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("classification")
                    .setNumber(1)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
    var service =
        DescriptorProtos.ServiceDescriptorProto.newBuilder()
            .setName("Classifier")
            .addMethod(
                DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Watch")
                    .setInputType(".evidence.Evidence")
                    .setOutputType(".evidence.Result")
                    .setServerStreaming(true));
    return Descriptors.FileDescriptor.buildFrom(
        DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("evidence.proto")
            .setPackage("evidence")
            .setSyntax("proto3")
            .addMessageType(evidence)
            .addMessageType(result)
            .addService(service)
            .build(),
        new Descriptors.FileDescriptor[0]);
  }

  private static DynamicMessage result(Descriptors.MethodDescriptor rpc, String value) {
    return DynamicMessage.newBuilder(rpc.getOutputType())
        .setField(rpc.getOutputType().findFieldByName("classification"), value)
        .build();
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
