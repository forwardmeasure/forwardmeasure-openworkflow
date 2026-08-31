#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
#

set -euo pipefail

# Kafka-Streams engine releases are framework-suffixed (execution-engines.yaml.gotmpl:
# openworkflow-engine-kafka-streams-<framework>), so this needs the same framework argument
# every other per-framework acceptance script takes.
framework=${1:?usage: verify-k3b.sh quarkus|spring|micronaut}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OPENWORKFLOW_K3A_RUNTIME=openworkflow-engine-kafka-streams-$framework \
OPENWORKFLOW_K3A_PERSISTENCE=Kafka-Streams \
OPENWORKFLOW_ACCEPTANCE_ENGINE=kafka-streams \
OPENWORKFLOW_ACCEPTANCE_CHECKPOINT=K3B \
  exec "$script_dir/verify-k3a.sh"
