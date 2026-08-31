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
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.migration.ProvisionedTenantSchemas;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkflowSharding/WorkflowScheduleSharding/SubworkflowCoordinatorSharding pick up a brand-new
 * tenant automatically the moment its first command arrives - Pekko's own plugin-instance cache
 * (see {@link TenantPersistencePlugins}) needs no pre-registration or restart. Any {@code
 * ShardedDaemonProcess}-based per-tenant projection (the CloudEvent/subworkflow outbox and
 * subscription projections, and the operation-adapter's HTTP/protocol operation outboxes) is not
 * self-healing the same way: it must be explicitly {@code .start(...)}ed once per tenant. This
 * periodically re-scans for newly-provisioned tenant schemas and starts every registered projection
 * for each one - the alternative (documenting a restart-catches-new-tenants limitation) would leave
 * a newly onboarded tenant's outbound/inbound routing silently dead, with no crash and no visible
 * error, until the next rolling deploy.
 */
public final class TenantProjectionSupervisor {
  private static final Logger LOG = LoggerFactory.getLogger(TenantProjectionSupervisor.class);

  private TenantProjectionSupervisor() {}

  /** One tenant's projections, started together once its schema is first discovered. */
  @FunctionalInterface
  public interface TenantProjections {
    void start(
        ActorSystem<?> system,
        DataSource dataSource,
        TenantSchema schema,
        PostgresConnectionSettings connection);
  }

  public static void start(
      ActorSystem<?> system,
      DataSource dataSource,
      PostgresConnectionSettings connection,
      Duration rescanInterval,
      List<TenantProjections> projections) {
    Objects.requireNonNull(system, "system");
    Objects.requireNonNull(dataSource, "dataSource");
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(rescanInterval, "rescanInterval");
    Objects.requireNonNull(projections, "projections");
    Set<TenantSchema> started = ConcurrentHashMap.newKeySet();
    Runnable rescan =
        () -> {
          try {
            for (TenantSchema schema : ProvisionedTenantSchemas.scan(dataSource)) {
              if (started.add(schema)) {
                LOG.info(
                    "Starting eventing projections for newly-discovered tenant schema {}", schema);
                for (TenantProjections projection : projections) {
                  projection.start(system, dataSource, schema, connection);
                }
              }
            }
          } catch (RuntimeException failure) {
            LOG.warn("Tenant schema re-scan failed - will retry on the next tick", failure);
          }
        };
    system
        .scheduler()
        .scheduleWithFixedDelay(Duration.ZERO, rescanInterval, rescan, system.executionContext());
  }
}
