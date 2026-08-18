package com.forwardmeasure.openworkflow.adapter.http;

import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** RFC 7616 Digest challenge response generation. */
final class DigestAuthentication {
  private static final SecureRandom RANDOM = new SecureRandom();

  private DigestAuthentication() {}

  static String authorize(
      ResolvedAuthentication authentication,
      String challenge,
      String method,
      URI uri,
      byte[] body) {
    if (authentication.kind() != ResolvedAuthentication.Kind.DIGEST) {
      throw new IllegalArgumentException("Digest credentials are required");
    }
    Map<String, String> parameters = parse(challenge);
    String realm = required(parameters, "realm");
    String nonce = required(parameters, "nonce");
    String algorithm = parameters.getOrDefault("algorithm", "MD5");
    String digestAlgorithm = digestAlgorithm(algorithm);
    Charset charset =
        "UTF-8".equalsIgnoreCase(parameters.get("charset"))
            ? StandardCharsets.UTF_8
            : StandardCharsets.ISO_8859_1;
    String qop = chooseQop(parameters.get("qop"));
    String cnonce = randomHex(16);
    String nonceCount = "00000001";
    String digestUri = uri.getRawPath();
    if (digestUri == null || digestUri.isEmpty()) {
      digestUri = "/";
    }
    if (uri.getRawQuery() != null) {
      digestUri += "?" + uri.getRawQuery();
    }

    char[] usernameValue = authentication.value("username");
    char[] passwordValue = authentication.value("password");
    try {
      String username = new String(usernameValue);
      String password = new String(passwordValue);
      String ha1 = hash(digestAlgorithm, username + ":" + realm + ":" + password, charset);
      if (algorithm.toLowerCase(Locale.ROOT).endsWith("-sess")) {
        ha1 = hash(digestAlgorithm, ha1 + ":" + nonce + ":" + cnonce, charset);
      }
      String ha2 =
          "auth-int".equals(qop)
              ? hash(
                  digestAlgorithm,
                  method + ":" + digestUri + ":" + hash(digestAlgorithm, body, charset),
                  charset)
              : hash(digestAlgorithm, method + ":" + digestUri, charset);
      String response =
          qop == null
              ? hash(digestAlgorithm, ha1 + ":" + nonce + ":" + ha2, charset)
              : hash(
                  digestAlgorithm,
                  ha1 + ":" + nonce + ":" + nonceCount + ":" + cnonce + ":" + qop + ":" + ha2,
                  charset);
      boolean userhash = Boolean.parseBoolean(parameters.getOrDefault("userhash", "false"));
      String headerUsername =
          userhash ? hash(digestAlgorithm, username + ":" + realm, charset) : username;

      StringBuilder header = new StringBuilder("Digest ");
      appendQuoted(header, "username", headerUsername);
      appendQuoted(header, "realm", realm);
      appendQuoted(header, "nonce", nonce);
      appendQuoted(header, "uri", digestUri);
      appendUnquoted(header, "algorithm", algorithm);
      appendQuoted(header, "response", response);
      if (parameters.containsKey("opaque")) {
        appendQuoted(header, "opaque", parameters.get("opaque"));
      }
      if (qop != null) {
        appendUnquoted(header, "qop", qop);
        appendUnquoted(header, "nc", nonceCount);
        appendQuoted(header, "cnonce", cnonce);
      }
      if (userhash) {
        appendUnquoted(header, "userhash", "true");
      }
      return header.toString();
    } finally {
      Arrays.fill(usernameValue, '\0');
      Arrays.fill(passwordValue, '\0');
    }
  }

  private static Map<String, String> parse(String challenge) {
    if (challenge == null || !challenge.regionMatches(true, 0, "Digest ", 0, 7)) {
      throw new IllegalArgumentException("HTTP response has no Digest challenge");
    }
    String source = challenge.substring(7);
    Map<String, String> values = new LinkedHashMap<>();
    int cursor = 0;
    while (cursor < source.length()) {
      while (cursor < source.length()
          && (source.charAt(cursor) == ',' || Character.isWhitespace(source.charAt(cursor)))) {
        cursor++;
      }
      int nameStart = cursor;
      while (cursor < source.length() && source.charAt(cursor) != '=') {
        cursor++;
      }
      if (cursor == source.length()) {
        break;
      }
      String name = source.substring(nameStart, cursor).trim().toLowerCase(Locale.ROOT);
      cursor++;
      String value;
      if (cursor < source.length() && source.charAt(cursor) == '"') {
        cursor++;
        StringBuilder decoded = new StringBuilder();
        boolean escaped = false;
        while (cursor < source.length()) {
          char character = source.charAt(cursor++);
          if (escaped) {
            decoded.append(character);
            escaped = false;
          } else if (character == '\\') {
            escaped = true;
          } else if (character == '"') {
            break;
          } else {
            decoded.append(character);
          }
        }
        value = decoded.toString();
      } else {
        int valueStart = cursor;
        while (cursor < source.length() && source.charAt(cursor) != ',') {
          cursor++;
        }
        value = source.substring(valueStart, cursor).trim();
      }
      if (!name.isEmpty()) {
        values.put(name, value);
      }
    }
    return values;
  }

  private static String chooseQop(String configured) {
    if (configured == null || configured.isBlank()) {
      return null;
    }
    String authInt = null;
    for (String value : configured.split(",")) {
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      if ("auth".equals(normalized)) {
        return "auth";
      }
      if ("auth-int".equals(normalized)) {
        authInt = "auth-int";
      }
    }
    if (authInt != null) {
      return authInt;
    }
    throw new IllegalArgumentException("Digest challenge has no supported qop");
  }

  private static String digestAlgorithm(String algorithm) {
    String base = algorithm.toUpperCase(Locale.ROOT).replace("-SESS", "");
    return switch (base) {
      case "MD5" -> "MD5";
      case "SHA-256" -> "SHA-256";
      case "SHA-512-256" -> "SHA-512/256";
      default -> throw new IllegalArgumentException("Unsupported Digest algorithm " + algorithm);
    };
  }

  private static String hash(String algorithm, String value, Charset charset) {
    byte[] bytes = value.getBytes(charset);
    try {
      return hash(algorithm, bytes, charset);
    } finally {
      Arrays.fill(bytes, (byte) 0);
    }
  }

  private static String hash(String algorithm, byte[] value, Charset ignored) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance(algorithm).digest(value));
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalArgumentException("Digest algorithm is unavailable " + algorithm, failure);
    }
  }

  private static String randomHex(int bytes) {
    byte[] value = new byte[bytes];
    RANDOM.nextBytes(value);
    try {
      return HexFormat.of().formatHex(value);
    } finally {
      Arrays.fill(value, (byte) 0);
    }
  }

  private static String required(Map<String, String> parameters, String name) {
    String value = parameters.get(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Digest challenge requires " + name);
    }
    return value;
  }

  private static void appendQuoted(StringBuilder target, String name, String value) {
    separator(target);
    target
        .append(name)
        .append("=\"")
        .append(value.replace("\\", "\\\\").replace("\"", "\\\""))
        .append('"');
  }

  private static void appendUnquoted(StringBuilder target, String name, String value) {
    separator(target);
    target.append(name).append('=').append(value);
  }

  private static void separator(StringBuilder target) {
    if (target.length() > "Digest ".length()) {
      target.append(", ");
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
