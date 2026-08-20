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
package com.forwardmeasure.openworkflow.definition.domain.entity;

import com.forwardmeasure.jpa.identity.entity.OwnedEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.io.Serial;
import java.util.Objects;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The stable, named, owned workflow identity. Content and governance live on {@link
 * WorkflowDefinition}.
 */
@Entity
@Table(name = "workflow")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Workflow extends OwnedEntity<Long> {
  @Serial private static final long serialVersionUID = 1L;
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-z][a-z0-9]*(?:-[a-z0-9]+)*");

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "workflow_ids")
  @SequenceGenerator(name = "workflow_ids", sequenceName = "workflow_id_seq", allocationSize = 50)
  @Setter
  private Long id;

  @Column(name = "name", nullable = false, length = 128)
  private String name;

  @Column(name = "title", nullable = false, length = 512)
  private String title;

  @Column(name = "description", length = 4096)
  @Setter
  private String description;

  public Workflow(String name, String title, String description) {
    this.name = validateName(name);
    this.title = validateTitle(title);
    this.description = description;
  }

  public void setName(String name) {
    this.name = validateName(name);
  }

  public void setTitle(String title) {
    this.title = validateTitle(title);
  }

  private static String validateName(String value) {
    if (!NAME_PATTERN.matcher(required(value, "name")).matches()) {
      throw new IllegalArgumentException(
          "name must match " + NAME_PATTERN.pattern() + ": " + value);
    }
    return value;
  }

  private static String validateTitle(String value) {
    return required(value, "title");
  }

  private static String required(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
