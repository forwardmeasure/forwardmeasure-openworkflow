#!/usr/bin/env node
//
// Licensed to the Apache Software Foundation (ASF) under one or more contributor license
// agreements. See the NOTICE file distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License. You may obtain a
// copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
// law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
// for the specific language governing permissions and limitations under the License.
//
// Adapted from openworkflow-actor-engine's deploy/acceptance/run-lifecycle-load.mjs. Not a verbatim
// port - that script's assumed REST contract doesn't match this repo's real execution-management
// API (checked against openworkflow-api-specifications/.../execution-management.openapi.yaml before
// writing this, per the engine-parity plan's own caution): starting an execution here requires a
// published, governance-approved revisionId (create/submit/approve/publish, three separate actor
// roles - see verify-k2.sh/verify-k5.sh for the proven pattern this mirrors), not a bare inline YAML
// document; start/pause/cancel return 202 with an If-Match-guarded optimistic-concurrency `version`,
// not 200/201 with an `accepted`/`revision` envelope; the execution view is a flat `state`/`version`
// record with no `activeTaskPaths`/`currentTaskPath` fields to assert on. The timing/percentile
// measurement this script exists for is unchanged.

import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

const baseUrl = (process.env.OPENWORKFLOW_BASE_URL ?? 'http://127.0.0.1:18117').replace(/\/$/, '');
const authorToken = process.env.OPENWORKFLOW_AUTHOR_TOKEN;
const approverToken = process.env.OPENWORKFLOW_APPROVER_TOKEN;
const publisherToken = process.env.OPENWORKFLOW_PUBLISHER_TOKEN;
const controllerToken = process.env.OPENWORKFLOW_CONTROLLER_TOKEN;
const journeys = positiveInteger('OPENWORKFLOW_LOAD_JOURNEYS', 100);
const concurrency = positiveInteger('OPENWORKFLOW_LOAD_CONCURRENCY', 20);
const projectionTimeoutMillis = positiveInteger('OPENWORKFLOW_PROJECTION_TIMEOUT_MILLIS', 30_000);
const projectionPollMillis = positiveInteger('OPENWORKFLOW_PROJECTION_POLL_MILLIS', 100);
const requireThresholds = booleanValue('OPENWORKFLOW_LOAD_REQUIRE_THRESHOLDS', false);
const thresholds = {
  minimumJourneysPerSecond: optionalPositiveNumber(
    'OPENWORKFLOW_LOAD_MIN_JOURNEYS_PER_SECOND'),
  maximumStartP99Millis: optionalPositiveNumber(
    'OPENWORKFLOW_LOAD_MAX_START_P99_MS'),
  maximumPauseP99Millis: optionalPositiveNumber(
    'OPENWORKFLOW_LOAD_MAX_PAUSE_P99_MS'),
  maximumCancelP99Millis: optionalPositiveNumber(
    'OPENWORKFLOW_LOAD_MAX_CANCEL_P99_MS'),
  maximumProjectionP99Millis: optionalPositiveNumber(
    'OPENWORKFLOW_LOAD_MAX_PROJECTION_P99_MS')
};

for (const [name, value] of Object.entries({
  OPENWORKFLOW_AUTHOR_TOKEN: authorToken,
  OPENWORKFLOW_APPROVER_TOKEN: approverToken,
  OPENWORKFLOW_PUBLISHER_TOKEN: publisherToken,
  OPENWORKFLOW_CONTROLLER_TOKEN: controllerToken
})) {
  assert(value, `${name} is required`);
}
if (requireThresholds) {
  for (const [name, value] of Object.entries(thresholds)) {
    assert.notEqual(value, null, `${name} threshold is required`);
  }
}

const definitionKey = `load-pause-cancel-${Date.now()}`;
const sourceDocument = `document:
  dsl: '1.0.3'
  namespace: forwardmeasure-load
  name: ${definitionKey}
  version: '1.0.0'
do:
  - hold:
      wait: PT5M
  - forbidden-after-cancel:
      set:
        cancellationInvariantBroken: true
`;

const definitionApi = `${baseUrl}/v1/workflow-definitions`;
const executionApi = `${baseUrl}/api/v1/executions`;

await request(definitionApi, {
  method: 'POST',
  headers: { Authorization: `Bearer ${authorToken}`, 'Content-Type': 'application/json' },
  body: JSON.stringify({ definitionKey, displayName: 'Load test', sourceDocument })
}, 201);
await request(`${definitionApi}/${definitionKey}/revisions/1/submit`, {
  method: 'POST',
  headers: { Authorization: `Bearer ${authorToken}` }
}, 200);
await request(`${definitionApi}/${definitionKey}/revisions/1/approve`, {
  method: 'POST',
  headers: { Authorization: `Bearer ${approverToken}` }
}, 200);
const published = await request(`${definitionApi}/${definitionKey}/revisions/1/publish`, {
  method: 'POST',
  headers: { Authorization: `Bearer ${publisherToken}` }
}, 200);
const revisionId = published.revisionId;
assert(revisionId, 'publish response did not include revisionId');

const durations = { start: [], pause: [], cancel: [], projection: [] };
let nextIndex = 0;
const startedAt = performance.now();

await Promise.all(Array.from({ length: Math.min(concurrency, journeys) }, async () => {
  while (nextIndex < journeys) {
    const index = nextIndex++;
    await lifecycle(index);
  }
}));

const elapsedMillis = performance.now() - startedAt;
const latencyMillis = Object.fromEntries(
  Object.entries(durations).map(([stage, values]) => [stage, distribution(values)]));
const result = {
  invariant: 'WAITING -> PAUSED -> CANCELLED; terminal projection has no output',
  journeys,
  concurrency,
  elapsedMillis: rounded(elapsedMillis),
  journeysPerSecond: rounded(journeys / (elapsedMillis / 1000)),
  latencyMillis,
  thresholds
};
console.log(JSON.stringify(result, null, 2));
assertThresholds(result);

async function lifecycle(index) {
  const input = { loadJourney: index };
  const controllerHeaders = { Authorization: `Bearer ${controllerToken}` };

  const start = await timed('start', () => request(executionApi, {
    method: 'POST',
    headers: {
      ...controllerHeaders,
      'Content-Type': 'application/json',
      'Idempotency-Key': randomUUID()
    },
    body: JSON.stringify({ revisionId, input })
  }, 202));
  assert.equal(start.state, 'WAITING');

  const paused = await timed('pause', () => request(`${executionApi}/${start.id}/pause`, {
    method: 'POST',
    headers: { ...controllerHeaders, 'Idempotency-Key': randomUUID(), 'If-Match': String(start.version) }
  }, 202));
  assert.equal(paused.state, 'PAUSED');

  const cancelled = await timed('cancel', () => request(`${executionApi}/${start.id}/cancel`, {
    method: 'POST',
    headers: {
      ...controllerHeaders,
      'Idempotency-Key': randomUUID(),
      'If-Match': String(paused.version)
    }
  }, 202));
  assert.equal(cancelled.state, 'CANCELLED');

  const projectedAt = performance.now();
  const view = await projected(start.id, controllerHeaders);
  durations.projection.push(performance.now() - projectedAt);
  assert.equal(view.state, 'CANCELLED');
  assert.deepEqual(view.input, input);
  assert.equal(view.output, null);
}

async function projected(executionId, headers) {
  const deadline = performance.now() + projectionTimeoutMillis;
  while (performance.now() < deadline) {
    const response = await fetch(`${executionApi}/${executionId}`, { headers });
    if (response.status === 200) {
      const view = await response.json();
      if (view.state === 'CANCELLED') {
        return view;
      }
    } else if (response.status !== 404) {
      throw await responseError(response, `/api/v1/executions/${executionId}`);
    }
    await new Promise(resolve => setTimeout(resolve, projectionPollMillis));
  }
  throw new Error(`execution ${executionId} did not project CANCELLED within ${projectionTimeoutMillis}ms`);
}

async function timed(stage, operation) {
  const before = performance.now();
  try {
    return await operation();
  } finally {
    durations[stage].push(performance.now() - before);
  }
}

async function request(url, init, expectedStatus) {
  const response = await fetch(url, init);
  if (response.status !== expectedStatus) {
    throw await responseError(response, url, expectedStatus);
  }
  return response.json();
}

async function responseError(response, url, expectedStatus) {
  const body = await response.text();
  const expectation = expectedStatus == null ? '' : `, expected ${expectedStatus}`;
  return new Error(`${url} returned ${response.status}${expectation}: ${body.slice(0, 1000)}`);
}

function distribution(values) {
  const sorted = values.toSorted((left, right) => left - right);
  return {
    min: rounded(sorted[0]),
    p50: rounded(percentile(sorted, 0.50)),
    p95: rounded(percentile(sorted, 0.95)),
    p99: rounded(percentile(sorted, 0.99)),
    max: rounded(sorted.at(-1))
  };
}

function percentile(sorted, fraction) {
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function positiveInteger(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  assert(Number.isSafeInteger(value) && value > 0, `${name} must be a positive integer`);
  return value;
}

function optionalPositiveNumber(name) {
  const configured = process.env[name];
  if (configured == null || configured === '') return null;
  const value = Number(configured);
  assert(Number.isFinite(value) && value > 0, `${name} must be a positive number`);
  return value;
}

function booleanValue(name, fallback) {
  const configured = process.env[name];
  if (configured == null || configured === '') return fallback;
  assert(['true', 'false'].includes(configured), `${name} must be true or false`);
  return configured === 'true';
}

function assertThresholds(result) {
  const checks = [
    ['minimum journeys/second', thresholds.minimumJourneysPerSecond,
      result.journeysPerSecond, (actual, target) => actual >= target],
    ['maximum start p99 ms', thresholds.maximumStartP99Millis,
      result.latencyMillis.start.p99, (actual, target) => actual <= target],
    ['maximum pause p99 ms', thresholds.maximumPauseP99Millis,
      result.latencyMillis.pause.p99, (actual, target) => actual <= target],
    ['maximum cancel p99 ms', thresholds.maximumCancelP99Millis,
      result.latencyMillis.cancel.p99, (actual, target) => actual <= target],
    ['maximum projection p99 ms', thresholds.maximumProjectionP99Millis,
      result.latencyMillis.projection.p99, (actual, target) => actual <= target]
  ];
  for (const [label, target, actual, accepted] of checks) {
    if (target != null) assert(accepted(actual, target),
      `${label} failed: actual ${actual}, target ${target}`);
  }
}

function rounded(value) {
  return Math.round(value * 1000) / 1000;
}
