-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license
-- agreements. See the NOTICE file distributed with this work for additional information regarding
-- copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
CREATE KEYSPACE IF NOT EXISTS openworkflow_journal WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_journal.messages (persistence_id text, partition_nr bigint, sequence_nr bigint, timestamp timeuuid, timebucket text, writer_uuid text, ser_id int, ser_manifest text, event_manifest text, event blob, meta_ser_id int, meta_ser_manifest text, meta blob, tags set<text>, PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp));
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_views (tag_name text, persistence_id text, sequence_nr bigint, timebucket bigint, timestamp timeuuid, tag_pid_sequence_nr bigint, writer_uuid text, ser_id int, ser_manifest text, event_manifest text, event blob, meta_ser_id int, meta_ser_manifest text, meta blob, PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr));
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_write_progress (persistence_id text, tag text, sequence_nr bigint, tag_pid_sequence_nr bigint, offset timeuuid, PRIMARY KEY (persistence_id, tag));
CREATE TABLE IF NOT EXISTS openworkflow_journal.tag_scanning (persistence_id text, sequence_nr bigint, PRIMARY KEY (persistence_id));
CREATE TABLE IF NOT EXISTS openworkflow_journal.metadata (persistence_id text PRIMARY KEY, deleted_to bigint, properties map<text,text>);
CREATE TABLE IF NOT EXISTS openworkflow_journal.all_persistence_ids (persistence_id text PRIMARY KEY);
CREATE KEYSPACE IF NOT EXISTS openworkflow_snapshot WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_snapshot.snapshots (persistence_id text, sequence_nr bigint, timestamp bigint, ser_id int, ser_manifest text, snapshot_data blob, snapshot blob, meta_ser_id int, meta_ser_manifest text, meta blob, PRIMARY KEY (persistence_id, sequence_nr)) WITH CLUSTERING ORDER BY (sequence_nr DESC);
CREATE KEYSPACE IF NOT EXISTS openworkflow_projection WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS openworkflow_projection.offset_store (projection_name text, partition int, projection_key text, offset text, manifest text, last_updated timestamp, PRIMARY KEY ((projection_name, partition), projection_key));
CREATE TABLE IF NOT EXISTS openworkflow_projection.projection_management (projection_name text, partition int, projection_key text, paused boolean, last_updated timestamp, PRIMARY KEY ((projection_name, partition), projection_key));
