# OpenWorkflow operations

This runbook is the operational authority for the `1.0.x` unified platform. Commands use the
Helmfile profiles under `deploy/helmfile`; production values are deliberately supplied through
environment-backed secret and endpoint inputs and are never committed.

Identity is an external platform dependency in every profile. OpenWorkflow
never installs or upgrades Keycloak; ensure the shared platform identity
service and credential projection are healthy before synchronizing releases.

## Deployment profiles

| Profile | Purpose | Dependencies |
|---|---|---|
| `local` | Single-workstation development | Bundled PostgreSQL, Cassandra and Redpanda; shared Keycloak |
| `development` | Shared development | Bundled data dependencies, shared Keycloak, ingress and optional OTLP collector |
| `ci` | Ephemeral validation | Bundled data dependencies, shared Keycloak and network policies |
| `kind` | Deterministic acceptance | Four-node Kind cluster, platform-owned Keycloak and K1-K7 fixtures |
| `production-postgresql` | Production with Pekko/PostgreSQL and Kafka Streams | External PostgreSQL, Keycloak, Kafka, OTLP and an existing credentials Secret |
| `production-cassandra` | Production with Pekko/Cassandra and Kafka Streams | External PostgreSQL product plane, Cassandra, Keycloak, Kafka, OTLP and an existing credentials Secret |

Render before every production change. A production profile fails before rendering if a required
endpoint, version or secret name is absent.

```bash
helmfile -f deploy/helmfile/helmfile.yaml.gotmpl -e production-postgresql template
helmfile -f deploy/helmfile/helmfile.yaml.gotmpl -e production-postgresql diff
helmfile -f deploy/helmfile/helmfile.yaml.gotmpl -e production-postgresql sync
```

## Safe migration and rolling upgrade

1. Back up PostgreSQL and the selected engine store. Record Kafka topic offsets and the deployed
   image/chart versions.
2. Run the migration image for the target version once for the explicit tenant list. Liquibase
   locking makes concurrent starts serialize; do not bypass a live lock. The job must finish before
   application rollout.
3. Verify the migration job logs and schema checksum table. Never enable ORM schema generation.
4. Roll definition services, then the inactive/secondary engine, then the primary engine, then
   Studio. Deployments use `maxUnavailable: 0` for gateways and `maxUnavailable: 1` for three-node
   runtimes. PDBs retain the required quorum.
5. Wait for startup and readiness probes after every component. Readiness removes a process before
   termination; the pre-stop delay allows in-flight HTTP and partition/cluster handoff, and the
   termination grace period bounds shutdown.
6. Execute K2, K3, K4, K5 and K6 against the upgraded environment.

Durable formats are append-compatible. Pekko wire compatibility is guarded by the versioned wire
golden and recovery tests. Kafka command/event/state compatibility is guarded by the durable wire
and restoration suites. A release that changes either contract requires an explicit compatibility
reader, a golden update with migration evidence, and a second full regression.

## Projection rebuild

Canonical projections are derived data. Stop the affected projection consumer, preserve its
checkpoint, truncate only the tenant projection tables named in the release notes, and restart with
the documented rebuild flag/version. Compare execution counts, maximum sequence, terminal counts
and a sample of complete histories to the engine journal/history topic before replacing the saved
checkpoint. Never delete Pekko journals, Kafka source topics, workflow revisions or publication
history during a rebuild.

## Backup and restore

- PostgreSQL: use a transactionally consistent physical backup or `pg_dump` including every
  `t_<uuid>` schema, Liquibase tables and large objects. Restore to an isolated instance and run
  read-only integrity queries before directing workloads to it.
- Cassandra: snapshot the Pekko keyspace on every node, retain schema CQL and incremental backups,
  and restore with matching partitioner and datacenter configuration.
- Kafka: use broker-native replicated snapshots/mirroring for every topic with the configured
  prefix. Preserve topic configuration, partition count and committed offsets.
- Keycloak: export the realm, Organizations, client authorization resources and policies. Keep
  client/admin secrets in the secret manager, not in the export bundle.

Quarterly restore tests must start one known PostgreSQL execution and one known Cassandra execution,
recover them on replacement runtime pods, query canonical history, and pass AuthZEN tenant-isolation
checks. Record recovery point and recovery time; this repository does not invent target values.

## Disaster recovery

Declare the source region read-only, fence its Kafka producers and database writers, restore the
product database and engine stores to the recovery region, then restore Keycloak and credentials.
Bring up migrations first, gateways second, one engine at a time, and Studio last. Validate topic
offsets, Pekko cluster identity, tenant schemas, AuthZEN decisions and projection parity before
enabling ingress. Failback is another backup/restore operation, never a bidirectional merge.

## Telemetry and incident correlation

Application charts set `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` and the environment-supplied
OTLP endpoint without coupling domain modules to a vendor. Keep tenant identifiers out of metric
labels. Propagate `traceparent` and `X-Correlation-ID`; use the canonical correlation ID to join API,
AuthZEN, command, engine-event and projection logs. Alert on failed readiness, migration failures,
consumer lag, projection lag, rejected durable commands, AuthZEN unavailability and PDB exhaustion.

## Rollback

Stop ingress for writes, retain the failed release's logs and checkpoints, and use `helm rollback`
only when the compatibility matrix says the prior binary can read the new durable data. Otherwise
restore the pre-upgrade backups into a new environment. After rollback, rerun K2-K6 and compare
projection counts and terminal histories before reopening writes.
