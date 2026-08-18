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

import com.forwardmeasure.jpa.core.entity.AuditedEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.io.Serial;
import java.util.Objects;

@Entity
@Table(name = "workflow_definition")
public class WorkflowDefinitionEntity extends AuditedEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_definition_ids")
  @SequenceGenerator(
      name = "workflow_definition_ids",
      sequenceName = "workflow_definition_id_seq",
      allocationSize = 1)
  private Long id;

  @Column(name = "definition_key", nullable = false, updatable = false, length = 160)
  private String definitionKey;

  @Column(name = "display_name", nullable = false, length = 320)
  private String displayName;

  protected WorkflowDefinitionEntity() {}

  public WorkflowDefinitionEntity(String definitionKey, String displayName) {
    this.definitionKey = required(definitionKey, "definitionKey");
    this.displayName = required(displayName, "displayName");
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public void setId(Long id) {
    this.id = id;
  }

  public String getDefinitionKey() {
    return definitionKey;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void rename(String name) {
    displayName = required(name, "displayName");
  }

  private static String required(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
