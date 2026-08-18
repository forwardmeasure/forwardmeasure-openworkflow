package com.forwardmeasure.openworkflow.operation.stomp;

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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLSocketFactory;
import org.apache.pekko.Done;

/** Native STOMP 1.2 AsyncAPI publish/subscribe driver. */
public final class AsyncApiStompOperationExecutor implements ProtocolOperationExecutor {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Duration timeout;
  private final Clock clock;
  private final HttpEgressPolicy egress;
  private final HttpAuthenticationSupport authentication;
  private final ClientFactory clients;

  public AsyncApiStompOperationExecutor(
      Duration timeout, HttpEgressPolicy egress, SecretProvider secrets) {
    this(timeout, Clock.systemUTC(), egress, secrets, SocketClient::new);
  }

  AsyncApiStompOperationExecutor(
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
        || !operation.protocol().equals("stomp")) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI STOMP driver received an incompatible operation"));
    }
    if (operation.authentication() != null
        && operation.authentication().kind() == AuthenticationPlan.Kind.DIGEST) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("STOMP does not support HTTP Digest authentication"));
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
        .thenCompose(credential -> run(operation, sink, credentials(credential)));
  }

  private CompletionStage<Done> run(
      ProtocolOperationDescriptor operation, ObservationSink sink, Credentials credentials) {
    Client client = null;
    try {
      client =
          clients.open(
              operation.endpoint().getHost(),
              port(operation),
              operation.endpoint().getScheme().equals("stomps"),
              credentials.username(),
              credentials.password(),
              timeout);
      if (operation.mode() == ProtocolOperationDescriptor.Mode.PUBLISH) {
        String receipt =
            client.publish(
                destination(operation), JSON.writeValueAsBytes(payload(operation)), timeout);
        Client opened = client;
        return sink.observe(
                receipt,
                JsonNodeFactory.instance
                    .objectNode()
                    .put("destination", destination(operation))
                    .put("receipt", receipt),
                false,
                true,
                clock.instant())
            .thenApply(disposition -> Done.getInstance())
            .whenComplete((done, failure) -> close(opened));
      }
      var completion = new CompletableFuture<Done>();
      Client opened = client;
      opened.subscribe(
          destination(operation),
          (id, body, acknowledge) -> {
            if (completion.isDone()) return;
            synchronized (completion) {
              if (completion.isDone()) return;
              try {
                ObservationDisposition disposition =
                    sink.observe(id, decode(body), false, false, clock.instant())
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

  private static int port(ProtocolOperationDescriptor operation) {
    if (operation.endpoint().getPort() >= 0) return operation.endpoint().getPort();
    return operation.endpoint().getScheme().equals("stomps") ? 61614 : 61613;
  }

  private static String destination(ProtocolOperationDescriptor operation) {
    String path = operation.endpoint().getPath();
    if (path == null || path.equals("/"))
      throw new IllegalArgumentException(
          "AsyncAPI STOMP destination must be present in the endpoint path");
    return path;
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

  private static Credentials credentials(HttpAuthenticationSupport.Credential credential) {
    if (credential == null) return new Credentials(null, null);
    if (credential.kind() == AuthenticationPlan.Kind.BASIC) {
      return new Credentials(credential.username(), credential.password());
    }
    String authorization = credential.authorization();
    if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
      throw new IllegalArgumentException("STOMP bearer authentication requires a token");
    }
    return new Credentials("token", authorization.substring(7));
  }

  private static void close(Client client) {
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  private record Credentials(String username, String password) {}

  @FunctionalInterface
  interface ClientFactory {
    Client open(
        String host, int port, boolean tls, String username, String password, Duration timeout)
        throws Exception;
  }

  interface Client {
    String publish(String destination, byte[] payload, Duration timeout) throws Exception;

    void subscribe(String destination, DeliveryHandler handler, CompletableFuture<Done> completion)
        throws Exception;

    void close() throws Exception;
  }

  @FunctionalInterface
  interface DeliveryHandler {
    void delivered(String id, byte[] payload, Runnable acknowledge);
  }

  private static final class SocketClient implements Client {
    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;

    private SocketClient(
        String host, int port, boolean tls, String username, String password, Duration timeout)
        throws Exception {
      socket = tls ? SSLSocketFactory.getDefault().createSocket() : new Socket();
      socket.connect(new InetSocketAddress(host, port), Math.toIntExact(timeout.toMillis()));
      socket.setSoTimeout(Math.toIntExact(timeout.toMillis()));
      input = socket.getInputStream();
      output = socket.getOutputStream();
      var headers = new LinkedHashMap<String, String>();
      headers.put("accept-version", "1.2");
      headers.put("host", host);
      headers.put("heart-beat", "0,0");
      if (username != null) headers.put("login", username);
      if (password != null) headers.put("passcode", password);
      write("CONNECT", headers, new byte[0]);
      Frame connected = read();
      if (!connected.command().equals("CONNECTED")) {
        throw new IllegalStateException("STOMP broker rejected connection: " + connected.command());
      }
    }

    @Override
    public String publish(String destination, byte[] payload, Duration timeout) throws Exception {
      String receipt = "ow-" + UUID.randomUUID();
      write(
          "SEND",
          Map.of(
              "destination", destination, "content-type", "application/json", "receipt", receipt),
          payload);
      Frame response = read();
      if (!response.command().equals("RECEIPT")
          || !receipt.equals(response.headers().get("receipt-id"))) {
        throw new IllegalStateException("STOMP publish receipt was not confirmed");
      }
      return receipt;
    }

    @Override
    public void subscribe(
        String destination, DeliveryHandler handler, CompletableFuture<Done> completion)
        throws Exception {
      String subscription = "ow-" + UUID.randomUUID();
      write(
          "SUBSCRIBE",
          Map.of("id", subscription, "destination", destination, "ack", "client-individual"),
          new byte[0]);
      Thread.ofVirtual()
          .name("openworkflow-stomp")
          .start(
              () -> {
                try {
                  while (!completion.isDone()) {
                    Frame frame = read();
                    if (frame.command().equals("ERROR")) {
                      throw new IllegalStateException("STOMP subscription failed");
                    }
                    if (!frame.command().equals("MESSAGE")) continue;
                    String ack = frame.headers().get("ack");
                    String id = frame.headers().getOrDefault("message-id", ack);
                    handler.delivered(id, frame.body(), () -> acknowledge(ack));
                  }
                } catch (Exception failure) {
                  if (!completion.isDone()) completion.completeExceptionally(failure);
                }
              });
    }

    private void acknowledge(String id) {
      if (id == null)
        throw new IllegalStateException("STOMP MESSAGE omitted its acknowledgement id");
      try {
        write("ACK", Map.of("id", id), new byte[0]);
      } catch (Exception failure) {
        throw new IllegalStateException("STOMP acknowledgement failed", failure);
      }
    }

    private synchronized void write(String command, Map<String, String> headers, byte[] body)
        throws Exception {
      var frame = new ByteArrayOutputStream();
      frame.write((command + "\n").getBytes(StandardCharsets.UTF_8));
      for (var header : headers.entrySet()) {
        frame.write(
            (escape(header.getKey()) + ":" + escape(header.getValue()) + "\n")
                .getBytes(StandardCharsets.UTF_8));
      }
      if (body.length > 0)
        frame.write(("content-length:" + body.length + "\n").getBytes(StandardCharsets.UTF_8));
      frame.write('\n');
      frame.write(body);
      frame.write(0);
      output.write(frame.toByteArray());
      output.flush();
    }

    private synchronized Frame read() throws Exception {
      int first;
      do {
        first = input.read();
      } while (first == '\n' || first == '\r');
      if (first < 0) throw new IllegalStateException("STOMP connection closed");
      String command = readLine(first);
      var headers = new LinkedHashMap<String, String>();
      String line;
      while (!(line = readLine(input.read())).isEmpty()) {
        int colon = line.indexOf(':');
        if (colon > 0)
          headers.put(unescape(line.substring(0, colon)), unescape(line.substring(colon + 1)));
      }
      var body = new ByteArrayOutputStream();
      String length = headers.get("content-length");
      if (length != null) {
        body.write(input.readNBytes(Integer.parseInt(length)));
        if (input.read() != 0) throw new IllegalStateException("Malformed STOMP frame terminator");
      } else {
        int value;
        while ((value = input.read()) > 0) body.write(value);
        if (value < 0) throw new IllegalStateException("Truncated STOMP frame");
      }
      return new Frame(command, Map.copyOf(headers), body.toByteArray());
    }

    private String readLine(int first) throws Exception {
      if (first < 0) throw new IllegalStateException("Truncated STOMP frame");
      var line = new ByteArrayOutputStream();
      int value = first;
      while (value != '\n') {
        if (value != '\r') line.write(value);
        value = input.read();
        if (value < 0) throw new IllegalStateException("Truncated STOMP frame");
      }
      return line.toString(StandardCharsets.UTF_8);
    }

    private static String escape(String value) {
      return value
          .replace("\\", "\\\\")
          .replace("\r", "\\r")
          .replace("\n", "\\n")
          .replace(":", "\\c");
    }

    private static String unescape(String value) {
      return value
          .replace("\\c", ":")
          .replace("\\n", "\n")
          .replace("\\r", "\r")
          .replace("\\\\", "\\");
    }

    @Override
    public void close() throws Exception {
      socket.close();
    }
  }

  private record Frame(String command, Map<String, String> headers, byte[] body) {}
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
