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
package com.forwardmeasure.openworkflow.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TenantSchemaRoutingTest {
  @Test
  void schemaIsDerivedOnlyFromValidatedTenantId() {
    TenantId tenantId = TenantId.parse("01234567-89ab-cdef-0123-456789abcdef");
    assertEquals("t_0123456789abcdef0123456789abcdef", TenantSchema.forTenant(tenantId).value());
  }

  @Test
  void arbitrarySchemaInputIsRejectedByTheJpaFoundation() {
    assertThrows(
        IllegalArgumentException.class, () -> new TenantSchema("tenant; drop schema public"));
  }

  @Test
  void distinctTenantsRouteToDistinctSchemas() {
    TenantSchema first = TenantSchema.forTenant(new TenantId(UUID.randomUUID()));
    TenantSchema second = TenantSchema.forTenant(new TenantId(UUID.randomUUID()));
    assertNotEquals(first, second);
  }
}
