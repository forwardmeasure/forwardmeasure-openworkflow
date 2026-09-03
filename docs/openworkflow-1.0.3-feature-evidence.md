# Open Workflow 1.0.3 feature evidence

This ledger is the Milestone 4 coverage index for the digest-pinned Open Workflow 1.0.3 schema.
`Implemented` means the construct is admitted into the immutable common plan and exercised through
both engine semantics; transport rows additionally name their edge-adapter evidence. A green row is
not inferred from schema validation alone.

| Specification surface | Status | Compiler and common contract evidence | Engine and edge evidence |
|---|---|---|---|
| Document identity, metadata, input, output, export and schema validation | Implemented | `OpenWorkflowCompilerTest`, `OpenWorkflowCompilerGoldenTest`, `WorkflowDefinitionContractTest` | `WorkflowEntityTest`, `WorkflowExecutionEngineTest`, portable contract serialization |
| `set`, task input/output, `if`, switch directives and nested task scopes | Implemented | compiler golden plan and contract analyzer fixtures | common engine contract, Pekko behavior matrix, Kafka restoration suite |
| `for`, while/until iteration and foreach concurrency | Implemented | immutable `ForPlan` compiler fixtures | Pekko iteration/fork behavior and Kafka durable restoration fixtures |
| fork, compete, nested forks and branch isolation | Implemented | immutable `ForkPlan` validation | Pekko fork matrix and Kafka restoration suite |
| wait durations, schedules, workflow/task timeouts and durable timers | Implemented | `DurationPlan`, `SchedulePlan`, `TimeoutPlan` validation | schedule planner/entity tests, timer processor tests, K5 relocation acceptance |
| retry limits, delays, exponential backoff and attempt deadlines | Implemented | immutable `RetryPlan` validation | Pekko retry/replay fixtures and Kafka restoration suite |
| raise, try/catch, error filters and runtime error propagation | Implemented | `TryPlan`, `CatchPlan`, `ErrorPlan` compiler fixtures | common behavioral fixtures for root, nested and fork scopes |
| emit, listen, event filters, correlation and consumption strategies | Implemented | event consumption/correlation plan validation | CloudEvents mapper, correlation semantics, durable listen and fork-listen fixtures |
| workflow calls and scheduled child workflows | Implemented | pinned workflow call plan | subworkflow coordinator, schedule entity and both-engine recovery fixtures |
| direct HTTP and OpenAPI calls | Implemented | pinned call/resource/authentication plans | HTTP/OpenAPI adapter suites, Kafka real-broker dispatcher, Pekko durable outbox |
| gRPC unary and streaming calls | Implemented | pinned proto graph and stream-mode validation | dynamic gRPC adapter suite and durable protocol coordinator |
| AsyncAPI publish/subscribe and correlation | Implemented | pinned AsyncAPI graph and subscription plan | AMQP, cloud, JMS, Kafka, MQTT, NATS, Pulsar, Redis and STOMP adapter suites |
| A2A and MCP calls | Implemented | pinned AgentCard/A2A and MCP call descriptors | JSON-RPC HTTP, AgentCard security and MCP stdio adapter suites |
| run/await and detached execution | Implemented | immutable `RunPlan` and pinned script resource | local-process and OCI runner policy/cancellation suites |
| authentication, secrets and runtime expressions | Implemented | credential-free durable authentication plan and jq expression contract | mounted secret resolution, credential wiping, fail-closed AuthZEN and tenant egress tests |
| cancellation, compensation and recovery interaction | Implemented | common cancellation/compensation plan semantics | both-engine cancellation/recovery fixtures and K5 owner-relocation matrix |
| human tasks (ForwardMeasure extension) | In progress | compiler extension recognition plus `openworkflow-human-task-domain` sealed states, commands, events, formal transition authority, and focused tests | Kafka path remains unreachable, Pekko remains capability-rejected, and persistence/API/Studio/deployment evidence is pending |
| canonical execution query, history and Studio visualization | Implemented | engine-neutral projection contract | PostgreSQL query adapter, three Studio hosts and browser acceptance suite |

The executable gates are the affected module-family tests, the shared engine contracts, the real
broker and persistence integration suites, three-framework acceptance, Studio browser acceptance,
and `deploy/acceptance/verify-k5.sh`. Transport-specific Kubernetes fixtures are recorded by the
Milestone 4 acceptance script alongside this ledger; no row may be downgraded to schema inspection
without changing its status here.
