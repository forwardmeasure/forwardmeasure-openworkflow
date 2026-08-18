package com.forwardmeasure.openworkflow.operation.kafka;

import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

/** Kafka login callback backed by an ephemeral tenant-authorized token supplier. */
public final class WorkflowOAuthLoginCallbackHandler implements AuthenticateCallbackHandler {
  static final String CONTEXT_CONFIG = "openworkflow.oauth.context";
  private static final Map<String, Context> CONTEXTS = new ConcurrentHashMap<>();
  private Context context;

  static String register(Context context) {
    String handle = UUID.randomUUID().toString();
    CONTEXTS.put(handle, Objects.requireNonNull(context, "context"));
    return handle;
  }

  static void unregister(String handle) {
    if (handle != null) CONTEXTS.remove(handle);
  }

  @Override
  public void configure(
      Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
    if (!"OAUTHBEARER".equals(mechanism))
      throw new IllegalArgumentException("Workflow OAuth callback requires OAUTHBEARER");
    String handle = Objects.toString(configs.get(CONTEXT_CONFIG), null);
    context = CONTEXTS.get(handle);
    if (context == null)
      throw new IllegalArgumentException("Workflow OAuth callback context is absent or expired");
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    if (context == null)
      throw new IllegalStateException("Workflow OAuth callback is not configured");
    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerTokenCallback token) {
        token.token(context.token());
      } else if (callback instanceof SaslExtensionsCallback extensions) {
        extensions.extensions(new SaslExtensions(Map.of()));
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override
  public void close() {
    context = null;
  }

  record Context(
      Supplier<HttpAuthenticationSupport.Credential> tokens,
      Set<String> scopes,
      String principal,
      Clock clock,
      Duration lifetime) {
    Context {
      Objects.requireNonNull(tokens, "tokens");
      scopes = Set.copyOf(scopes);
      if (principal == null || principal.isBlank())
        throw new IllegalArgumentException("OAuth principal must be non-blank");
      Objects.requireNonNull(clock, "clock");
      Objects.requireNonNull(lifetime, "lifetime");
      if (lifetime.isZero() || lifetime.isNegative())
        throw new IllegalArgumentException("OAuth callback lifetime must be positive");
    }

    OAuthBearerToken token() throws IOException {
      try {
        HttpAuthenticationSupport.Credential credential = tokens.get();
        String authorization = credential.authorization();
        if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
          throw new IllegalArgumentException("Kafka OAuth resolver did not produce a bearer token");
        }
        Instant now = clock.instant();
        return new Token(
            authorization.substring(7),
            scopes,
            now.plus(lifetime).toEpochMilli(),
            principal,
            now.toEpochMilli());
      } catch (CompletionException failure) {
        throw new IOException("Kafka OAuth token acquisition failed", failure.getCause());
      } catch (RuntimeException failure) {
        throw new IOException("Kafka OAuth token acquisition failed", failure);
      }
    }
  }

  private record Token(
      String value, Set<String> scope, long lifetimeMs, String principalName, Long startTimeMs)
      implements OAuthBearerToken {
    Token {
      if (value == null || value.isBlank())
        throw new IllegalArgumentException("OAuth token must be non-blank");
      scope = Set.copyOf(scope);
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
