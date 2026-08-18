# Compatibility matrix

| Product | Java | Kubernetes | PostgreSQL | Cassandra | Kafka API | Keycloak | Open Workflow |
|---|---:|---:|---:|---:|---:|---:|---:|
| `1.0.x` | 25 | 1.32-1.34 | 18 | 5.0 | 4.3 client / compatible broker | 26.7 | 1.0.3 pinned |

Both Pekko persistence profiles and Kafka Streams implement the same public execution contract.
Quarkus, Spring Boot and Micronaut expose the generated common APIs; Studio assets are shared across
all three hosts. Maven and runtime dependency versions are owned by
`forwardmeasure-java-parent`, never overridden by this product reactor.

Upgrade compatibility is forward-only within a migration step. A prior binary may be used for
rollback only when release notes explicitly confirm it can read all durable records and schema
changes written by the newer version. Live executions stay in their pinned engine and persistence
profile.
