import Keycloak from "keycloak-js";

export type StudioIdentity = {
  keycloak: Keycloak;
  token: string;
};

export async function initialize(): Promise<StudioIdentity> {
  const value = window.__OPENWORKFLOW_STUDIO_CONFIG__;
  if (!value?.oidcUrl || !value.oidcRealm || !value.oidcClientId) {
    throw new Error("Studio runtime configuration is incomplete");
  }
  const keycloak = new Keycloak({
    url: value.oidcUrl,
    realm: value.oidcRealm,
    clientId: value.oidcClientId,
  });
  const authenticated = await keycloak.init({
    onLoad: "login-required",
    checkLoginIframe: false,
    pkceMethod: "S256",
  });
  if (!authenticated || !keycloak.token) {
    throw new Error("Studio authentication did not produce an access token");
  }
  return { keycloak, token: keycloak.token };
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
