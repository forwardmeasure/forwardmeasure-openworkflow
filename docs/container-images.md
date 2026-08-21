# Container image ownership

Maven is the only supported application-image build entry point. Activate the
`container-image` profile; the Java parent owns registry, group, tag, base JRE,
plugin versions, and build/push switches. Each deployable leaf owns only its
image name, entry point, packaging plugin, and Dockerfile.

```shell
./mvnw -Pcontainer-image -DskipTests package
./mvnw -Pcontainer-image -Dcontainer-image.push=true -DskipTests package
```

| Capability | Maven modules and images |
|---|---|
| Definition management | `openworkflow-definition-management-{quarkus,spring,micronaut}` |
| Execution management | `openworkflow-execution-management-{quarkus,spring,micronaut}-service` → `openworkflow-execution-management-{framework}` |
| Kafka Streams engine | `openworkflow-engine-kafka-streams-{quarkus,spring,micronaut}` |
| Pekko engine | `openworkflow-engine-pekko-{quarkus,spring,micronaut}` |
| Operation adapter | `openworkflow-operation-adapter-{quarkus,spring,micronaut}` |
| Studio | `openworkflow-studio-{quarkus,spring,micronaut}` |
| Tenant provisioning | `openworkflow-tenant-provisioning` |
| Database migrations | `openworkflow-migrations` |

Every row is an independently deployable and scalable workload. Kafka Streams
engine images contain no Pekko runtime, and Pekko engine images contain no
Kafka Streams implementation. Operation-adapter images consume both Kafka
effects and Pekko durable outboxes but host no workflow engine entities.

The shared framework bindings are capability-neutral libraries and never
produce product images. The former generic framework service images and the
former generic definition/Kafka/Pekko Helm charts are intentionally removed;
capabilities must not be selected by runtime flags inside a mixed image.
