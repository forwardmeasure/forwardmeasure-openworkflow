/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.definition.persistence;

import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import java.util.List;
import java.util.Optional;

public final class WorkflowRevisionRepository
    extends AbstractBaseRepository<WorkflowRevisionEntity, Long> {
  public Optional<WorkflowRevisionEntity> findRevision(String definitionKey, int revisionNumber) {
    List<WorkflowRevisionEntity> matches =
        entityManager()
            .createQuery(
                "select revision from WorkflowRevisionEntity revision "
                    + "join fetch revision.definition definition "
                    + "where definition.definitionKey = :key "
                    + "and revision.revisionNumber = :revisionNumber",
                WorkflowRevisionEntity.class)
            .setParameter("key", definitionKey)
            .setParameter("revisionNumber", revisionNumber)
            .setMaxResults(1)
            .getResultList();
    return matches.stream().findFirst();
  }

  public List<WorkflowRevisionEntity> list() {
    return entityManager()
        .createQuery(
            "select revision from WorkflowRevisionEntity revision "
                + "join fetch revision.definition definition "
                + "order by definition.definitionKey, revision.revisionNumber",
            WorkflowRevisionEntity.class)
        .getResultList();
  }

  public int nextRevisionNumber(String definitionKey) {
    Integer maximum =
        entityManager()
            .createQuery(
                "select max(revision.revisionNumber) from WorkflowRevisionEntity revision "
                    + "where revision.definition.definitionKey = :key",
                Integer.class)
            .setParameter("key", definitionKey)
            .getSingleResult();
    return maximum == null ? 1 : maximum + 1;
  }
}
