package com.forwardmeasure.openworkflow.operation.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.HttpEgressPolicy;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Native Google Pub/Sub, AWS SNS, and AWS SQS AsyncAPI transport driver. */
public final class AsyncApiCloudOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final ClientFactory clients;

  public AsyncApiCloudOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, AsyncApiCloudOperationExecutor::openClient);
  }

  AsyncApiCloudOperationExecutor(
      Duration timeout,
      Clock clock,
      HttpEgressPolicy egress,
      SecretProvider secrets,
      ClientFactory clients) {
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.authentication =
        new HttpAuthenticationSupport(
            HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(timeout)
                .build(),
            JSON,
            timeout,
            egress,
            Objects.requireNonNull(secrets, "secrets"));
    this.clients = Objects.requireNonNull(clients, "clients");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(operation, "operation");
    Objects.requireNonNull(sink, "sink");
    if (operation.kind() != ProtocolOperationDescriptor.Kind.ASYNC_API
        || !(operation.protocol().equals("googlepubsub")
            || operation.protocol().equals("sns")
            || operation.protocol().equals("sqs"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI cloud driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Cloud messaging does not support HTTP Digest authentication"));
    }
    try {
      egress.authorize(executionId.tenantId(), operation.endpoint());
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return authentication
        .resolve(
            executionId,
            operation.authentication(),
            operation.authenticationContext(),
            operation.operationId())
        .thenCompose(credential -> run(operation, sink, credential));
  }

  private CompletionStage<Done> run(
      ProtocolOperationDescriptor operation,
      ObservationSink sink,
      HttpAuthenticationSupport.Credential credential) {
    Client client = null;
    try {
      client = clients.open(operation.protocol(), operation, credential, timeout);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
        String id = client.publish(JSON.writeValueAsBytes(payload(operation)));
        Client opened = client;
        return sink.observe(
                id,
                JsonNodeFactory.instance
                    .objectNode()
                    .put("protocol", operation.protocol())
                    .put("messageId", id),
                false,
                true,
                clock.instant())
            .thenApply(disposition -> Done.getInstance())
            .whenComplete((done, failure) -> close(opened));
      }
      if (operation.protocol().equals("sns")) {
        close(client);
        return CompletableFuture.failedFuture(
            new IllegalArgumentException(
                "SNS is publish-only; subscribe through an SQS subscription"));
      }
      var completion = new CompletableFuture<Done>();
      Client opened = client;
      opened.subscribe(
          (id, value, acknowledge) -> {
            if (completion.isDone()) return;
            synchronized (completion) {
              if (completion.isDone()) return;
              try {
                ObservationDisposition disposition =
                    sink.observe(id, decode(value), false, false, clock.instant())
                        .toCompletableFuture()
                        .join();
                acknowledge.run();
                if (disposition == ObservationDisposition.STOP) {
                  completion.complete(Done.getInstance());
                }
              } catch (Exception failure) {
                completion.completeExceptionally(failure);
              }
            }
          },
          completion);
      completion.whenComplete((done, failure) -> close(opened));
      return completion;
    } catch (Exception failure) {
      if (client != null) close(client);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static Client openClient(
      String protocol,
      ProtocolOperationDescriptor operation,
      HttpAuthenticationSupport.Credential credential,
      Duration timeout)
      throws Exception {
    return switch (protocol) {
      case "sns" -> new Sns(operation, credential);
      case "sqs" -> new Sqs(operation, credential, timeout);
      case "googlepubsub" -> new GooglePubSub(operation, credential);
      default -> throw new IllegalArgumentException("Unsupported cloud protocol");
    };
  }

  private static JsonNode payload(ProtocolOperationDescriptor operation) {
    return operation.request().has("payload")
        ? operation.request().get("payload")
        : operation.request();
  }

  private static JsonNode decode(byte[] value) {
    try {
      return JSON.readTree(value);
    } catch (Exception ignored) {
      return JsonNodeFactory.instance.binaryNode(value);
    }
  }

  private static String header(
      ProtocolOperationDescriptor operation, String name, String fallback) {
    String value = operation.request().path("headers").path(name).asText();
    return value.isBlank() ? fallback : value;
  }

  private static String address(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    String value = path == null ? "" : path.replaceFirst("^/+", "");
    if (value.isBlank())
      throw new IllegalArgumentException(
          "Cloud messaging address must be present in the endpoint path or headers");
    return value;
  }

  private static String headerOrAddress(ProtocolOperationDescriptor operation, String name) {
    String value = operation.request().path("headers").path(name).asText();
    return value.isBlank() ? address(operation) : value;
  }

  private static String bearer(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return null;
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("A bearer access token is required");
    }
    return authorization.substring(7);
  }

  private static void close(Client client) {
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  interface ClientFactory {
    Client open(
        String protocol,
        ProtocolOperationDescriptor operation,
        HttpAuthenticationSupport.Credential credential,
        Duration timeout)
        throws Exception;
  }

  interface Client {
    String publish(byte[] value) throws Exception;

    void subscribe(DeliveryHandler handler, CompletableFuture<Done> completion) throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String id, byte[] value, Runnable acknowledge);
  }

  private static final class Sns implements Client {
    private final SnsClient client;
    private final String topicArn;

    private Sns(
        ProtocolOperationDescriptor operation, HttpAuthenticationSupport.Credential credential) {
      var builder = SnsClient.builder().region(Region.of(header(operation, "region", "us-east-1")));
      if (credential != null) {
        if (credential.kind() != AuthenticationPlan.Kind.BASIC) {
          throw new IllegalArgumentException(
              "AWS SNS requires access-key/secret-key authentication");
        }
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(credential.username(), credential.password())));
      }
      if (operation.endpoint().getScheme().startsWith("http")) {
        builder.endpointOverride(origin(operation.endpoint()));
      }
      client = builder.build();
      topicArn = headerOrAddress(operation, "topicArn");
    }

    @Override
    public String publish(byte[] value) {
      return client
          .publish(
              builder ->
                  builder
                      .topicArn(topicArn)
                      .message(new String(value, java.nio.charset.StandardCharsets.UTF_8)))
          .messageId();
    }

    @Override
    public void subscribe(DeliveryHandler handler, CompletableFuture<Done> completion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      client.close();
    }
  }

  private static final class Sqs implements Client {
    private final SqsClient client;
    private final String queueUrl;
    private final Duration timeout;

    private Sqs(
        ProtocolOperationDescriptor operation,
        HttpAuthenticationSupport.Credential credential,
        Duration timeout) {
      var builder = SqsClient.builder().region(Region.of(header(operation, "region", "us-east-1")));
      if (credential != null) {
        if (credential.kind() != AuthenticationPlan.Kind.BASIC) {
          throw new IllegalArgumentException(
              "AWS SQS requires access-key/secret-key authentication");
        }
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(credential.username(), credential.password())));
      }
      if (operation.endpoint().getScheme().startsWith("http")) {
        builder.endpointOverride(origin(operation.endpoint()));
      }
      client = builder.build();
      queueUrl =
          header(
              operation,
              "queueUrl",
              operation.endpoint().getScheme().startsWith("http")
                  ? operation.endpoint().toString()
                  : address(operation));
      this.timeout = timeout;
    }

    @Override
    public String publish(byte[] value) {
      return client
          .sendMessage(
              builder ->
                  builder
                      .queueUrl(queueUrl)
                      .messageBody(new String(value, java.nio.charset.StandardCharsets.UTF_8)))
          .messageId();
    }

    @Override
    public void subscribe(DeliveryHandler handler, CompletableFuture<Done> completion) {
      Thread.ofVirtual()
          .name("openworkflow-sqs")
          .start(
              () -> {
                try {
                  while (!completion.isDone()) {
                    var response =
                        client.receiveMessage(
                            builder ->
                                builder
                                    .queueUrl(queueUrl)
                                    .maxNumberOfMessages(1)
                                    .waitTimeSeconds(
                                        Math.min(
                                            20,
                                            Math.max(1, Math.toIntExact(timeout.toSeconds())))));
                    for (var message : response.messages()) {
                      handler.delivered(
                          message.messageId(),
                          message.body().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                          () ->
                              client.deleteMessage(
                                  builder ->
                                      builder
                                          .queueUrl(queueUrl)
                                          .receiptHandle(message.receiptHandle())));
                    }
                  }
                } catch (Exception failure) {
                  if (!completion.isDone()) completion.completeExceptionally(failure);
                }
              });
    }

    @Override
    public void close() {
      client.close();
    }
  }

  private static final class GooglePubSub implements Client {
    private final ProtocolOperationDescriptor operation;
    private final FixedCredentialsProvider credentials;
    private Publisher publisher;
    private Subscriber subscriber;

    private GooglePubSub(
        ProtocolOperationDescriptor operation, HttpAuthenticationSupport.Credential credential) {
      this.operation = operation;
      String token = bearer(credential);
      credentials =
          token == null
              ? null
              : FixedCredentialsProvider.create(
                  GoogleCredentials.create(
                      new AccessToken(
                          token, Date.from(java.time.Instant.now().plus(Duration.ofHours(1))))));
    }

    @Override
    public String publish(byte[] value) throws Exception {
      String project = header(operation, "project", null);
      String topic = headerOrAddress(operation, "topic");
      if (project == null)
        throw new IllegalArgumentException("Google Pub/Sub requires headers.project");
      var builder = Publisher.newBuilder(ProjectTopicName.of(project, topic));
      if (credentials != null) builder.setCredentialsProvider(credentials);
      publisher = builder.build();
      return publisher
          .publish(PubsubMessage.newBuilder().setData(ByteString.copyFrom(value)).build())
          .get();
    }

    @Override
    public void subscribe(DeliveryHandler handler, CompletableFuture<Done> completion) {
      String project = header(operation, "project", null);
      String subscription = headerOrAddress(operation, "subscription");
      if (project == null)
        throw new IllegalArgumentException("Google Pub/Sub requires headers.project");
      com.google.cloud.pubsub.v1.MessageReceiver receiver =
          (message, consumer) ->
              handler.delivered(
                  message.getMessageId(), message.getData().toByteArray(), consumer::ack);
      var builder =
          Subscriber.newBuilder(ProjectSubscriptionName.of(project, subscription), receiver);
      if (credentials != null) builder.setCredentialsProvider(credentials);
      subscriber = builder.build();
      subscriber.startAsync().awaitRunning();
    }

    @Override
    public void close() throws Exception {
      if (publisher != null) {
        publisher.shutdown();
        publisher.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
      }
      if (subscriber != null) subscriber.stopAsync().awaitTerminated();
    }
  }

  private static URI origin(URI endpoint) {
    try {
      return new URI(
          endpoint.getScheme(),
          endpoint.getUserInfo(),
          endpoint.getHost(),
          endpoint.getPort(),
          null,
          null,
          null);
    } catch (Exception failure) {
      throw new IllegalArgumentException("Invalid cloud endpoint", failure);
    }
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
