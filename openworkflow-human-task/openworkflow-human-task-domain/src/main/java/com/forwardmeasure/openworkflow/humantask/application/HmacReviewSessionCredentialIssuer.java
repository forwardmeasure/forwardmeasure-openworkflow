/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** HMAC-backed issuer: retries reproduce credentials without persisting the raw bearer token. */
public final class HmacReviewSessionCredentialIssuer implements ReviewSessionCredentialIssuer {
  private final byte[] secret;

  public HmacReviewSessionCredentialIssuer(byte[] secret) {
    if (secret == null || secret.length < 32) {
      throw new IllegalArgumentException("Review credential secret must contain at least 32 bytes");
    }
    this.secret = secret.clone();
  }

  @Override
  public Credential issue(HumanTaskId taskId, String commandId, String actorId) {
    String material = taskId.value() + "\u0000" + commandId + "\u0000" + actorId;
    String token = HexFormat.of().formatHex(hmac(material));
    return new Credential("hrs-" + token.substring(0, 32), token);
  }

  private byte[] hmac(String material) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      return mac.doFinal(material.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException impossible) {
      throw new IllegalStateException("HmacSHA256 is unavailable", impossible);
    }
  }
}
