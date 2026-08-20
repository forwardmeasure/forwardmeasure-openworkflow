-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Pekko Persistence Cassandra journal.
CREATE KEYSPACE IF NOT EXISTS openworkflow_journal
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_journal.messages (
  persistence_id text, partition_nr bigint, sequence_nr bigint,
  timestamp timeuuid, timebucket text, writer_uuid text,
  ser_id int, ser_manifest text, event_manifest text, event blob,
  meta_ser_id int, meta_ser_manifest text, meta blob, tags set<text>,
  PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp)
) WITH gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_views (
  tag_name text, persistence_id text, sequence_nr bigint, timebucket bigint,
  timestamp timeuuid, tag_pid_sequence_nr bigint, writer_uuid text,
  ser_id int, ser_manifest text, event_manifest text, event blob,
  meta_ser_id int, meta_ser_manifest text, meta blob,
  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr)
) WITH gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_write_progress (
  persistence_id text, tag text, sequence_nr bigint,
  tag_pid_sequence_nr bigint, offset timeuuid,
  PRIMARY KEY (persistence_id, tag)
);
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_scanning (
  persistence_id text, sequence_nr bigint, PRIMARY KEY (persistence_id)
);
CREATE TABLE IF NOT EXISTS openworkflow_journal.metadata (
  persistence_id text PRIMARY KEY, deleted_to bigint,
  properties map<text,text>
);
CREATE TABLE IF NOT EXISTS openworkflow_journal.all_persistence_ids (
  persistence_id text PRIMARY KEY
);

-- Pekko Persistence Cassandra snapshots.
CREATE KEYSPACE IF NOT EXISTS openworkflow_snapshot
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_snapshot.snapshots (
  persistence_id text, sequence_nr bigint, timestamp bigint,
  ser_id int, ser_manifest text, snapshot_data blob, snapshot blob,
  meta_ser_id int, meta_ser_manifest text, meta blob,
  PRIMARY KEY (persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (sequence_nr DESC)
  AND gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Pekko Projection Cassandra offsets and management state.
CREATE KEYSPACE IF NOT EXISTS openworkflow_projection
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_projection.offset_store (
  projection_name text, partition int, projection_key text,
  offset text, manifest text, last_updated timestamp,
  PRIMARY KEY ((projection_name, partition), projection_key)
);
CREATE TABLE IF NOT EXISTS openworkflow_projection.projection_management (
  projection_name text, partition int, projection_key text,
  paused boolean, last_updated timestamp,
  PRIMARY KEY ((projection_name, partition), projection_key)
);

-- OpenWorkflow Pekko application projections. Definition and execution management remain in the
-- canonical PostgreSQL/JPA stores and are intentionally not duplicated here.
CREATE KEYSPACE IF NOT EXISTS ${applicationKeyspace}
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS ${applicationKeyspace}.openworkflow_event_subscription (
  tenant_did text, target_kind text, target_entity_id text,
  task_path text, event_types set<text>, revision bigint, active boolean,
  PRIMARY KEY ((tenant_did), target_kind, target_entity_id)
);
