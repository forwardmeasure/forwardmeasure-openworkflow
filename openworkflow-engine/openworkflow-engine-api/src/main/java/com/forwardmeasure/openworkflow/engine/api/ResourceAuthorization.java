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
package com.forwardmeasure.openworkflow.engine.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource-aware authorization evaluated from claims in an already verified token.
 *
 * <p>Grants use {@code action@resource-type@resource-id}. Each component may be {@code *}. A token
 * carrying delegation claims must pass both its own grants and the delegation's narrower grant set.
 * This class deliberately does not verify a JWT; that remains the responsibility of the hosting
 * framework's OIDC resource server.
 */
public final class ResourceAuthorization {
  private static final Logger AUDIT =
      LoggerFactory.getLogger("com.forwardmeasure.openworkflow.security.audit");
  public static final String GRANTS_CLAIM = "openworkflow_permissions";
  public static final String DELEGATOR_CLAIM = "delegator_did";
  public static final String DELEGATE_CLAIM = "delegate_did";
  public static final String DELEGATION_TENANT_CLAIM = "delegation_tenant_did";
  public static final String DELEGATION_GRANTS_CLAIM = "delegation_permissions";
  public static final String DELEGATION_EXPIRY_CLAIM = "delegation_expires_at";

  private ResourceAuthorization() {}

  public static ActorIdentity authorize(
      ActorIdentity actor,
      String action,
      String resourceType,
      String resourceId,
      Map<String, ?> claims) {
    return authorize(actor, action, resourceType, resourceId, claims, Clock.systemUTC());
  }

  static ActorIdentity authorize(
      ActorIdentity actor,
      String action,
      String resourceType,
      String resourceId,
      Map<String, ?> claims,
      Clock clock) {
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(action, "action");
    Objects.requireNonNull(resourceType, "resourceType");
    Objects.requireNonNull(resourceId, "resourceId");
    Objects.requireNonNull(claims, "claims");
    Objects.requireNonNull(clock, "clock");

    try {
      ActorIdentity authorized =
          authorizeUnchecked(actor, action, resourceType, resourceId, claims, clock);
      audit("allow", actor, action, resourceType, resourceId, claims, null);
      return authorized;
    } catch (SecurityException denied) {
      audit("deny", actor, action, resourceType, resourceId, claims, denied.getMessage());
      throw denied;
    }
  }

  private static ActorIdentity authorizeUnchecked(
      ActorIdentity actor,
      String action,
      String resourceType,
      String resourceId,
      Map<String, ?> claims,
      Clock clock) {
    requiredText(claims, "sub", "JWT must contain a provider subject");
    if (!hasText(claims.get("client_id")) && !hasText(claims.get("azp"))) {
      throw new SecurityException("JWT must contain client_id or azp");
    }
    authenticationTime(claims.get("iat"), clock);
    requireGrant(
        grants(claims.get(GRANTS_CLAIM)),
        action,
        resourceType,
        resourceId,
        "JWT does not grant this OpenWorkflow action");

    boolean delegated =
        claims.containsKey(DELEGATOR_CLAIM)
            || claims.containsKey(DELEGATE_CLAIM)
            || claims.containsKey(DELEGATION_TENANT_CLAIM)
            || claims.containsKey(DELEGATION_GRANTS_CLAIM)
            || claims.containsKey(DELEGATION_EXPIRY_CLAIM);
    if (!delegated) {
      return actor;
    }

    String delegator = requiredDid(claims, DELEGATOR_CLAIM);
    String delegate = requiredDid(claims, DELEGATE_CLAIM);
    String tenant = requiredDid(claims, DELEGATION_TENANT_CLAIM);
    if (delegator.equals(delegate)) {
      throw new SecurityException("Delegator and delegate must be distinct");
    }
    if (!delegate.equals(actor.actorDid())) {
      throw new SecurityException("Delegation delegate does not match actor_did");
    }
    if (!tenant.equals(actor.tenantId().value())) {
      throw new SecurityException("Delegation tenant does not match tenant_did");
    }
    Instant expiresAt = expiry(claims.get(DELEGATION_EXPIRY_CLAIM));
    if (!expiresAt.isAfter(clock.instant())) {
      throw new SecurityException("Delegation has expired");
    }
    requireGrant(
        grants(claims.get(DELEGATION_GRANTS_CLAIM)),
        action,
        resourceType,
        resourceId,
        "Delegation does not grant this OpenWorkflow action");
    return actor;
  }

  private static void audit(
      String decision,
      ActorIdentity actor,
      String action,
      String resourceType,
      String resourceId,
      Map<String, ?> claims,
      String reason) {
    // Emit only an allowlisted metadata set. Raw tokens, workflow data, and
    // permission text never enter the audit record.
    AUDIT.info(
        "event=authorization decision={} tenant_did={} actor_did={} "
            + "subject={} client_id={} authenticated_at={} "
            + "action={} resource_type={} resource_id={} "
            + "grants_sha256={} delegated={} delegator_did={} reason={}",
        clean(decision),
        clean(actor.tenantId().value()),
        clean(actor.actorDid()),
        clean(claims.get("sub")),
        clean(claims.get("client_id")),
        clean(claims.get("iat")),
        clean(action),
        clean(resourceType),
        clean(resourceId),
        grantDigest(claims.get(GRANTS_CLAIM)),
        Boolean.toString(claims.containsKey(DELEGATOR_CLAIM)),
        clean(claims.get(DELEGATOR_CLAIM)),
        clean(reason));
  }

  private static String grantDigest(Object grants) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(
                  String.join("\n", ResourceAuthorization.grants(grants).stream().sorted().toList())
                      .getBytes(StandardCharsets.UTF_8));
      return java.util.HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private static String clean(Object value) {
    if (value == null) return "-";
    return value.toString().replaceAll("[\\p{Cntrl}\\s]+", "_");
  }

  private static String requiredDid(Map<String, ?> claims, String name) {
    Object value = claims.get(name);
    if (!(value instanceof String did) || did.isBlank() || !did.startsWith("did:")) {
      throw new SecurityException("Delegation requires a valid " + name + " claim");
    }
    return did;
  }

  private static String requiredText(Map<String, ?> claims, String name, String message) {
    Object value = claims.get(name);
    if (!(value instanceof String text) || text.isBlank()) {
      throw new SecurityException(message);
    }
    return text;
  }

  private static boolean hasText(Object value) {
    return value instanceof String text && !text.isBlank();
  }

  private static Instant authenticationTime(Object value, Clock clock) {
    try {
      Instant issuedAt;
      if (value instanceof Instant instant) {
        issuedAt = instant;
      } else if (value instanceof java.util.Date date) {
        issuedAt = date.toInstant();
      } else if (value instanceof Number epochSeconds) {
        issuedAt = Instant.ofEpochSecond(epochSeconds.longValue());
      } else if (value instanceof String text && !text.isBlank()) {
        issuedAt =
            text.chars().allMatch(Character::isDigit)
                ? Instant.ofEpochSecond(Long.parseLong(text))
                : Instant.parse(text);
      } else {
        throw new SecurityException("JWT must contain iat");
      }
      if (issuedAt.isAfter(clock.instant().plusSeconds(60))) {
        throw new SecurityException("JWT iat is in the future");
      }
      return issuedAt;
    } catch (SecurityException failure) {
      throw failure;
    } catch (RuntimeException malformed) {
      throw new SecurityException("Invalid JWT iat claim", malformed);
    }
  }

  private static Instant expiry(Object value) {
    try {
      if (value instanceof Instant instant) {
        return instant;
      }
      if (value instanceof java.util.Date date) {
        return date.toInstant();
      }
      if (value instanceof Number epochSeconds) {
        return Instant.ofEpochSecond(epochSeconds.longValue());
      }
      if (value instanceof String text && !text.isBlank()) {
        return text.chars().allMatch(Character::isDigit)
            ? Instant.ofEpochSecond(Long.parseLong(text))
            : Instant.parse(text);
      }
    } catch (RuntimeException malformed) {
      throw new SecurityException("Invalid delegation_expires_at claim", malformed);
    }
    throw new SecurityException("Delegation requires delegation_expires_at");
  }

  private static Set<String> grants(Object value) {
    var result = new LinkedHashSet<String>();
    if (value instanceof String text) {
      for (String grant : text.split("[ ,]+")) {
        if (!grant.isBlank()) result.add(grant);
      }
    } else if (value instanceof Collection<?> collection) {
      for (Object grant : collection) {
        if (grant instanceof String text && !text.isBlank()) result.add(text);
      }
    }
    return Set.copyOf(result);
  }

  private static void requireGrant(
      Set<String> grants, String action, String resourceType, String resourceId, String message) {
    boolean allowed =
        grants.stream().anyMatch(grant -> matches(grant, action, resourceType, resourceId));
    if (!allowed) throw new SecurityException(message);
  }

  private static boolean matches(
      String grant, String action, String resourceType, String resourceId) {
    String[] parts = grant.split("@", -1);
    return parts.length == 3
        && component(parts[0], action)
        && component(parts[1], resourceType)
        && component(parts[2], resourceId);
  }

  private static boolean component(String granted, String requested) {
    return "*".equals(granted) || granted.equals(requested);
  }
}
