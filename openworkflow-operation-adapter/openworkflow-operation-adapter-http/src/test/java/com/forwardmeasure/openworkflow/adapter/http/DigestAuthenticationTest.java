package com.forwardmeasure.openworkflow.adapter.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link DigestAuthentication}'s RFC 7616 challenge-response generation: HA1/HA2 hash
 * computation, qop negotiation (auth vs. auth-int vs. absent), the assembled {@code Authorization}
 * header's field names and quoting, and per-call nonce-count/cnonce behaviour.
 *
 * <p>{@code DigestAuthentication} is package-private, and this test lives in the same package so it
 * can call {@link DigestAuthentication#authorize} directly without reflection.
 *
 * <p>The class generates its own random {@code cnonce} internally (via {@code SecureRandom}) and it
 * cannot be seeded from a test. Where the response digest formula does not involve the cnonce (no
 * {@code qop}), tests assert against fixed hex values computed independently of this
 * implementation, from the RFC 2617 &sect;3.5 "Mufasa" / "Circle of Life" worked example, using the
 * command-line {@code md5sum}/{@code openssl dgst -sha256} tools (not copied from memory of the RFC
 * text). Where the formula does involve the cnonce (qop present), tests parse the actual cnonce out
 * of the produced header and independently recompute the expected response digest using the same
 * RFC formula, then assert it matches what the production code produced.
 */
class DigestAuthenticationTest {

  @Test
  void computesTheRfc2617WorkedExampleResponseDigestWhenNoQopIsOffered() {
    // Expected hex values, derived independently (not from memory of the RFC text):
    //   HA1      = md5("Mufasa:testrealm@host.com:Circle of Life")
    //            = 7650d211d93fae2c3f56cdb1f1af23b2      (verified: `printf ... | md5sum`)
    //   HA2      = md5("GET:/dir/index.html")
    //            = 39aff3a2bab6126f332b942af96d3366      (verified: `printf ... | md5sum`)
    //   response = md5(HA1 + ":" + nonce + ":" + HA2)
    //            = 2951cdbad33b2271fcb6b8e7b8feac23      (verified: `printf ... | md5sum`)
    String challenge =
        "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", "
            + "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"";
    ResolvedAuthentication auth = digestCredentials("Mufasa", "Circle of Life");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/dir/index.html"), new byte[0]);

      assertEquals("Mufasa", field(header, "username"));
      assertEquals("testrealm@host.com", field(header, "realm"));
      assertEquals("dcd98b7102dd2f0e8b11d0f600bfb0c093", field(header, "nonce"));
      assertEquals("/dir/index.html", field(header, "uri"));
      assertEquals("5ccc069c403ebaf9f0171e9517f40e41", field(header, "opaque"));
      assertNull(field(header, "qop"), "no qop was offered so none should be echoed back");
      assertNull(field(header, "nc"), "nonce-count only applies once qop is negotiated");
      assertNull(field(header, "cnonce"), "cnonce only applies once qop is negotiated");
      assertEquals("2951cdbad33b2271fcb6b8e7b8feac23", field(header, "response"));
    } finally {
      auth.close();
    }
  }

  @Test
  void computesASha256ResponseDigestWhenTheChallengeRequestsSha256() {
    // Same identity/secret, SHA-256 instead of MD5, to exercise the algorithm-selection switch.
    // response = sha256(HA1 + ":" + nonce + ":" + HA2), verified via `openssl dgst -sha256`.
    String challenge =
        "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", "
            + "algorithm=SHA-256";
    ResolvedAuthentication auth = digestCredentials("Mufasa", "Circle of Life");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/dir/index.html"), new byte[0]);

      assertEquals("SHA-256", field(header, "algorithm"));
      assertEquals(
          "205ca89237097586d371bb378381cba91f9024ed8811a58d7b88aaf4b4c49f14",
          field(header, "response"));
    } finally {
      auth.close();
    }
  }

  @Test
  void authQopIncludesNonceCountOneAndAFreshCnonceWithAnIndependentlyVerifiableResponse() {
    String challenge =
        "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", "
            + "qop=\"auth\", opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"";
    ResolvedAuthentication auth = digestCredentials("Mufasa", "Circle of Life");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/dir/index.html"), new byte[0]);

      assertTrue(header.startsWith("Digest "));
      assertEquals("auth", field(header, "qop"));
      assertFalse(header.contains("qop=\"auth\""), "qop is a token, must not be quoted");
      assertEquals("00000001", field(header, "nc"));
      assertFalse(header.contains("nc=\"00000001\""), "nc is a token, must not be quoted");
      String cnonce = field(header, "cnonce");
      assertNotNull(cnonce);
      assertTrue(header.contains("cnonce=\"" + cnonce + "\""), "cnonce must be quoted");

      String ha1 = md5Hex("Mufasa:testrealm@host.com:Circle of Life");
      String ha2 = md5Hex("GET:/dir/index.html");
      String expectedResponse =
          md5Hex(ha1 + ":dcd98b7102dd2f0e8b11d0f600bfb0c093:00000001:" + cnonce + ":auth:" + ha2);
      assertEquals(expectedResponse, field(header, "response"));
    } finally {
      auth.close();
    }
  }

  @Test
  void authIntQopHashesTheRequestBodyIntoHa2() {
    String challenge =
        "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\", "
            + "qop=\"auth-int\"";
    byte[] body = "hello world".getBytes(StandardCharsets.ISO_8859_1);
    ResolvedAuthentication auth = digestCredentials("Mufasa", "Circle of Life");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "POST", URI.create("https://host.com/dir/index.html"), body);

      assertEquals("auth-int", field(header, "qop"));
      String cnonce = field(header, "cnonce");

      String ha1 = md5Hex("Mufasa:testrealm@host.com:Circle of Life");
      String bodyHash = md5Hex(body);
      String ha2 = md5Hex("POST:/dir/index.html:" + bodyHash);
      String expectedResponse =
          md5Hex(
              ha1 + ":dcd98b7102dd2f0e8b11d0f600bfb0c093:00000001:" + cnonce + ":auth-int:" + ha2);
      assertEquals(expectedResponse, field(header, "response"));
    } finally {
      auth.close();
    }
  }

  @Test
  void prefersAuthQopOverAuthIntWhenBothAreOfferedRegardlessOfListOrder() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      String bothOrderedAuthIntFirst =
          DigestAuthentication.authorize(
              auth,
              "Digest realm=\"r\", nonce=\"n1\", qop=\"auth-int,auth\"",
              "GET",
              URI.create("https://host.com/x"),
              new byte[0]);
      assertEquals("auth", field(bothOrderedAuthIntFirst, "qop"));

      String bothOrderedAuthFirst =
          DigestAuthentication.authorize(
              auth,
              "Digest realm=\"r\", nonce=\"n1\", qop=\"auth,auth-int\"",
              "GET",
              URI.create("https://host.com/x"),
              new byte[0]);
      assertEquals("auth", field(bothOrderedAuthFirst, "qop"));

      String onlyAuthInt =
          DigestAuthentication.authorize(
              auth,
              "Digest realm=\"r\", nonce=\"n2\", qop=\"auth-int\"",
              "GET",
              URI.create("https://host.com/x"),
              new byte[0]);
      assertEquals("auth-int", field(onlyAuthInt, "qop"));
    } finally {
      auth.close();
    }
  }

  @Test
  void rejectsAChallengeWhoseQopIsNeitherAuthNorAuthInt() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth,
                  "Digest realm=\"r\", nonce=\"n\", qop=\"token68\"",
                  "GET",
                  URI.create("https://host.com/x"),
                  new byte[0]));
    } finally {
      auth.close();
    }
  }

  @Test
  void everyCallStartsAFreshNonceCountOfOneWithANewCnonceEvenForTheSameServerNonce() {
    // DigestAuthentication keeps no per-nonce request counter between calls: it always sends
    // nc=00000001, paired with a newly random cnonce, on every authorize() invocation. Documented
    // here as the class's actual behaviour, not changed: a server that rejects a repeated
    // (nonce, nc) pair as a replay could refuse a second such request against the same challenge,
    // but each call is at least paired with a distinct cnonce.
    String challenge = "Digest realm=\"r\", nonce=\"same-nonce\", qop=\"auth\"";
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      String first =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/x"), new byte[0]);
      String second =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/x"), new byte[0]);

      assertEquals("00000001", field(first, "nc"));
      assertEquals("00000001", field(second, "nc"));
      assertNotEquals(
          field(first, "cnonce"), field(second, "cnonce"), "each call must use a fresh cnonce");
      assertNotEquals(
          field(first, "response"),
          field(second, "response"),
          "different cnonce must yield a different response digest");
    } finally {
      auth.close();
    }
  }

  @Test
  void sessAlgorithmVariantFoldsTheNonceAndCnonceIntoHa1() {
    String challenge = "Digest realm=\"r\", nonce=\"sess-nonce\", qop=\"auth\", algorithm=MD5-sess";
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/x"), new byte[0]);

      assertEquals("MD5-sess", field(header, "algorithm"));
      String cnonce = field(header, "cnonce");

      String baseHa1 = md5Hex("u:r:p");
      String sessHa1 = md5Hex(baseHa1 + ":sess-nonce:" + cnonce);
      String ha2 = md5Hex("GET:/x");
      String expected = md5Hex(sessHa1 + ":sess-nonce:00000001:" + cnonce + ":auth:" + ha2);
      assertEquals(expected, field(header, "response"));
    } finally {
      auth.close();
    }
  }

  @Test
  void userhashTrueReplacesTheUsernameFieldWithAHashOfUsernameAndRealm() {
    // md5("Mufasa:testrealm@host.com") = 74f54fe2c8045a5ffda7d02fd97f1716 (verified via md5sum).
    String challenge = "Digest realm=\"testrealm@host.com\", nonce=\"n\", userhash=true";
    ResolvedAuthentication auth = digestCredentials("Mufasa", "Circle of Life");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/x"), new byte[0]);

      assertEquals("74f54fe2c8045a5ffda7d02fd97f1716", field(header, "username"));
      assertEquals("true", field(header, "userhash"));
    } finally {
      auth.close();
    }
  }

  @Test
  void digestUriFieldIncludesTheRawQueryAndDefaultsToSlashWhenThePathIsEmpty() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      String withQuery =
          DigestAuthentication.authorize(
              auth,
              "Digest realm=\"r\", nonce=\"n\"",
              "GET",
              URI.create("https://host.com/search?q=a&limit=2"),
              new byte[0]);
      assertEquals("/search?q=a&limit=2", field(withQuery, "uri"));

      String noPath =
          DigestAuthentication.authorize(
              auth,
              "Digest realm=\"r\", nonce=\"n\"",
              "GET",
              URI.create("https://host.com"),
              new byte[0]);
      assertEquals("/", field(noPath, "uri"));
    } finally {
      auth.close();
    }
  }

  @Test
  void quotedFieldsEscapeBackslashesAndDoubleQuotesInTheRealm() {
    // Round-trips a realm containing both an embedded double-quote and an embedded backslash
    // through parse() (which must unescape it from the incoming challenge) and appendQuoted()
    // (which must re-escape it identically into the outgoing header). This guards against
    // Authorization-header injection via an attacker-influenced realm.
    String rawRealm = "tricky\"realm\\here";
    String escapedForChallenge = rawRealm.replace("\\", "\\\\").replace("\"", "\\\"");
    String challenge = "Digest realm=\"" + escapedForChallenge + "\", nonce=\"n\"";
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      String header =
          DigestAuthentication.authorize(
              auth, challenge, "GET", URI.create("https://host.com/x"), new byte[0]);

      String expectedQuotedRealm = "realm=\"" + escapedForChallenge + "\"";
      assertTrue(
          header.contains(expectedQuotedRealm), () -> "expected escaped realm field in: " + header);
    } finally {
      auth.close();
    }
  }

  @Test
  void rejectsAuthenticationThatIsNotDigestKind() {
    ResolvedAuthentication basic =
        new ResolvedAuthentication(
            ResolvedAuthentication.Kind.BASIC,
            Map.of("username", "u".toCharArray(), "password", "p".toCharArray()));
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  basic,
                  "Digest realm=\"r\", nonce=\"n\"",
                  "GET",
                  URI.create("https://host.com/x"),
                  new byte[0]));
    } finally {
      basic.close();
    }
  }

  @Test
  void rejectsAChallengeThatIsNotADigestChallenge() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth, "Basic realm=\"r\"", "GET", URI.create("https://host.com/x"), new byte[0]));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth, null, "GET", URI.create("https://host.com/x"), new byte[0]));
    } finally {
      auth.close();
    }
  }

  @Test
  void rejectsAChallengeMissingRealmOrNonce() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth,
                  "Digest nonce=\"n\"",
                  "GET",
                  URI.create("https://host.com/x"),
                  new byte[0]));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth,
                  "Digest realm=\"r\"",
                  "GET",
                  URI.create("https://host.com/x"),
                  new byte[0]));
    } finally {
      auth.close();
    }
  }

  @Test
  void rejectsAnUnsupportedAlgorithm() {
    ResolvedAuthentication auth = digestCredentials("u", "p");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DigestAuthentication.authorize(
                  auth,
                  "Digest realm=\"r\", nonce=\"n\", algorithm=DES",
                  "GET",
                  URI.create("https://host.com/x"),
                  new byte[0]));
    } finally {
      auth.close();
    }
  }

  @Test
  void wrongPasswordProducesADifferentResponseDigestThanTheCorrectPassword() {
    String challenge =
        "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\"";
    ResolvedAuthentication correct = digestCredentials("Mufasa", "Circle of Life");
    ResolvedAuthentication wrong = digestCredentials("Mufasa", "wrong-password");
    try {
      String correctHeader =
          DigestAuthentication.authorize(
              correct,
              challenge,
              "GET",
              URI.create("https://host.com/dir/index.html"),
              new byte[0]);
      String wrongHeader =
          DigestAuthentication.authorize(
              wrong, challenge, "GET", URI.create("https://host.com/dir/index.html"), new byte[0]);

      assertEquals("2951cdbad33b2271fcb6b8e7b8feac23", field(correctHeader, "response"));
      assertNotEquals(field(correctHeader, "response"), field(wrongHeader, "response"));
    } finally {
      correct.close();
      wrong.close();
    }
  }

  private static ResolvedAuthentication digestCredentials(String username, String password) {
    return new ResolvedAuthentication(
        ResolvedAuthentication.Kind.DIGEST,
        Map.of("username", username.toCharArray(), "password", password.toCharArray()));
  }

  private static String md5Hex(String value) {
    return md5Hex(value.getBytes(StandardCharsets.ISO_8859_1));
  }

  private static String md5Hex(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  /** Extracts a Digest-header field value, quoted or unquoted, guarding against name collisions. */
  private static String field(String header, String name) {
    Matcher quoted =
        Pattern.compile("(?<![A-Za-z0-9])" + Pattern.quote(name) + "=\"([^\"]*)\"").matcher(header);
    if (quoted.find()) {
      return quoted.group(1);
    }
    Matcher unquoted =
        Pattern.compile("(?<![A-Za-z0-9])" + Pattern.quote(name) + "=([^,]+)").matcher(header);
    if (unquoted.find()) {
      return unquoted.group(1).trim();
    }
    return null;
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
