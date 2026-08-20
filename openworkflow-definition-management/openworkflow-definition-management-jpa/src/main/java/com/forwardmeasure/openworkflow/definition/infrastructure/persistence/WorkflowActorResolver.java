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
package com.forwardmeasure.openworkflow.definition.infrastructure.persistence;

import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.jpa.identity.entity.IdentityType;
import com.forwardmeasure.jpa.identity.repository.ActorRepository;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import java.util.Objects;

/**
 * Resolves the trusted subject identifier from the security context to an {@link Actor},
 * provisioning one on first sight. Shared across this capability's repositories.
 *
 * <p>Deliberately does not catch-and-retry the lookup within this method on a unique-constraint
 * race: per the JPA spec, a failed {@code flush()} marks the enclosing transaction rollback-only,
 * so nothing done afterward in the same transaction survives — a same-transaction retry would not
 * actually recover anything even if the retried lookup succeeded. What this does do is translate
 * the race into a clean, typed exception instead of letting a raw Hibernate exception escape, so
 * the transaction rolls back predictably and the caller retries the whole original request — at
 * which point the lookup finds the actor the other request created. This race is not rare: it's
 * ordinary parallel requests from one new actor's first browser session, not a contrived scenario.
 */
public final class WorkflowActorResolver {
  private static final String IDENTITY_PROVIDER = "keycloak";

  private final ActorRepository actors;

  public WorkflowActorResolver(EntityManager entityManager) {
    Objects.requireNonNull(entityManager, "entityManager");
    this.actors = new ActorRepository();
    this.actors.bindPersistenceContext(entityManager);
  }

  public Actor resolve(String subjectIdentifier) {
    return actors
        .findByIdentity(IDENTITY_PROVIDER, subjectIdentifier)
        .orElseGet(() -> provision(subjectIdentifier));
  }

  private Actor provision(String subjectIdentifier) {
    Actor actor =
        Actor.builder()
            .subjectIdentifier(subjectIdentifier)
            .type(IdentityType.HUMAN)
            .identityProvider(IDENTITY_PROVIDER)
            .build();
    try {
      actors.persist(actor);
      actors.flush();
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict(
          "Actor " + subjectIdentifier + " was concurrently provisioned; retry the request");
    }
    return actor;
  }
}
