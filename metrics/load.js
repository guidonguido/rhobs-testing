import { check, fail, sleep } from "k6";
import http from "k6/http";
import exec from "k6/execution";
import remote from "k6/x/remotewrite";

// Normal load the environment is expected to handle.
const NORMAL_SAMPLES_PER_SECOND = Math.max(1, Number(__ENV.NORMAL_SAMPLES_PER_SECOND || 3500));

const LOAD_STAGES = parse_load_stages(__ENV.LOAD_STAGES || "[1]");
const STAGE_DURATION = __ENV.STAGE_DURATION || "5m";
const FINAL_STAGE_DURATION = __ENV.MAX_BURST_DURATION || "15m";
const STAGE_DURATION_SECONDS = duration_seconds(STAGE_DURATION);
const FINAL_STAGE_DURATION_SECONDS = duration_seconds(FINAL_STAGE_DURATION);

// Time between stages when the next stage is gated by a check for load shedding.
const STAGE_GATE_DELAY_SECONDS = Math.max(10, Number(__ENV.STAGE_GATE_DELAY_SECONDS || 120));
const SHEDDING_CHECK_INTERVAL = __ENV.SHEDDING_CHECK_INTERVAL || "5s";
const AGENT_MAX_LAG_SECONDS = Math.max(0, Number(__ENV.AGENT_MAX_LAG_SECONDS || 5));

// Remote Write payload size.
const SAMPLES_PER_BATCH = Math.max(1, Number(__ENV.SAMPLES_PER_BATCH || 2000));
const SERIES_BATCHES = Math.max(1, Number(__ENV.SERIES_BATCHES || 10));
// 2500 samples is a conservative limit to avoid exceeding the Prometheus Agent's default 10MB request size limit.
// In addition, the Prometheus Agent defaults to max_samples_per_send: 2000 
if (SAMPLES_PER_BATCH > 2500) {
  throw new Error(
    `SAMPLES_PER_BATCH must not exceed 2500`,
  );
}

const PROMETHEUS_RECEIVER_URL = required(__ENV.PROMETHEUS_RECEIVER_URL, "PROMETHEUS_RECEIVER_URL");
const PROMETHEUS_METRICS_URL = required(__ENV.PROMETHEUS_METRICS_URL, "PROMETHEUS_METRICS_URL");
const REMOTE_WRITE_NAME = __ENV.REMOTE_WRITE_NAME || "rhobs-load-test";
const KUBE_API_SERVER_URL = required(__ENV.KUBE_API_SERVER_URL, "KUBE_API_SERVER_URL");
const KUBE_NAMESPACE = required(__ENV.KUBE_NAMESPACE, "KUBE_NAMESPACE");
const ROUTER_DEPLOYMENT_NAME = required(__ENV.ROUTER_DEPLOYMENT_NAME, "ROUTER_DEPLOYMENT_NAME");
const SERVICE_ACCOUNT_TOKEN = open(required(
  __ENV.SERVICE_ACCOUNT_TOKEN_FILE, "SERVICE_ACCOUNT_TOKEN_FILE"),).trim();

// Unique identifier for this load test run, used to annotate
// the router deployment and identify generated metric samples.
const RUN_ID = __ENV.RUN_ID || "manual";

const rw_client = new remote.Client({
  url: PROMETHEUS_RECEIVER_URL,
  timeout: "30s",
});
const label_template = remote.precompileLabelTemplates({
  __name__: "rhobs_load_test_sample",
  run_id: RUN_ID,
  series_id: "${series_id}",
  cardinality_10: "${series_id/10}",
  cardinality_100: "${series_id/100}",
});

//#region Scenarios setup

// Scenarios are the stages of the load test, each instantiating VUs.
// Each intermediate stage includes 4 scenarios: 
// 1. Generate load
// 2. Log the start of the stage
// 3. Log the end of the stage
// 4. Next stage shedding gate
// Logging is required becouse the test report is intended to be correlated 
// with the load-shedding, router and prometheus agent dashboards.
const scenarios = {};
let next_stage_start_seconds = 0;
for (let stage_index = 0; stage_index < LOAD_STAGES.length; stage_index += 1) {
  const STAGE_NUMBER = stage_index + 1;
  const STAGE_START_SECONDS = next_stage_start_seconds;
  const STAGE_END_SECONDS = STAGE_START_SECONDS + STAGE_DURATION_SECONDS;

  // First scenario of the stage: generate load at the configured multiplier.
  scenarios[`load_stage_${STAGE_NUMBER}`] = {
    executor: "constant-arrival-rate",
    exec: "send_batch",
    // Number of requests per second i.e. iterations, must match NORMAL_SAMPLES_PER_SECOND * multiplier / SAMPLES_PER_BATCH.
    rate: Math.max(1, 
      Math.ceil(
        Math.max(1, 
          Math.round(NORMAL_SAMPLES_PER_SECOND * LOAD_STAGES[stage_index])) 
          / SAMPLES_PER_BATCH)),
    timeUnit: "1s",
    duration: STAGE_DURATION,
    startTime: `${STAGE_START_SECONDS}s`,
    preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || 100),
    maxVUs: Number(__ENV.MAX_VUS || 2000),
  };

  // Second and third scenarios of the stage: log the start and end of the stage.
  scenarios[`log_load_stage_${STAGE_NUMBER}_start`] = stage_log_scenario(
    STAGE_START_SECONDS,
  );
  scenarios[`log_load_stage_${STAGE_NUMBER}_end`] = stage_log_scenario(STAGE_END_SECONDS);

  // Fourth scenario of the stage: gate the next stage by checking for load shedding.
  next_stage_start_seconds = STAGE_END_SECONDS;
  if (STAGE_NUMBER < LOAD_STAGES.length) {
    scenarios[`gate_load_stage_${STAGE_NUMBER + 1}`] = {
      executor: "per-vu-iterations",
      exec: "gate_next_stage",
      vus: 1,
      iterations: 1,
      startTime: `${STAGE_END_SECONDS}s`,
      maxDuration: `${STAGE_GATE_DELAY_SECONDS}s`,
    }
    next_stage_start_seconds += STAGE_GATE_DELAY_SECONDS;
  }
}

const UPDATE_START_SECONDS = next_stage_start_seconds;
const TOTAL_TEST_SECONDS = UPDATE_START_SECONDS + FINAL_STAGE_DURATION_SECONDS;

// If shedding did not happen yet, keep the highest load level running while the Receive routers restart.
scenarios.final_stage_hold = {
  executor: "constant-arrival-rate",
  exec: "send_batch",
  rate: Math.max(1, 
    Math.ceil(
      Math.max(1, 
        Math.round(NORMAL_SAMPLES_PER_SECOND * LOAD_STAGES[LOAD_STAGES.length - 1])) 
        / SAMPLES_PER_BATCH)),
  timeUnit: "1s",
  duration: FINAL_STAGE_DURATION,
  startTime: `${UPDATE_START_SECONDS}s`,
  preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || 100),
  maxVUs: Number(__ENV.MAX_VUS || 2000),
};
scenarios.log_final_stage_hold_start = stage_log_scenario(UPDATE_START_SECONDS);
scenarios.log_final_stage_hold_end = stage_log_scenario(TOTAL_TEST_SECONDS);
scenarios.shedding_monitor = {
  executor: "constant-vus",
  exec: "monitor_shedding",
  vus: 1,
  duration: `${TOTAL_TEST_SECONDS}s`,
  startTime: "0s",
  gracefulStop: "0s",
};
scenarios.router_restart = {
  executor: "per-vu-iterations",
  exec: "restart_routers",
  vus: 1,
  iterations: 1,
  startTime: `${UPDATE_START_SECONDS}s`,
  maxDuration: "10m",
};

export const options = {
  setupTimeout: "1m",
  teardownTimeout: "1m",
  scenarios: scenarios,
};

export function setup() {
  const DEPLOYMENT = get_router_deployment();
  const AGENT_COUNTERS = get_agent_counters();

  console.log(
    `run_id=${RUN_ID} event=setup utc=${new Date().toISOString()} ` +
      `router_replicas=${DEPLOYMENT.spec.replicas} samples_per_batch=${SAMPLES_PER_BATCH} `,
  );

  return {
    agent_counters: AGENT_COUNTERS,
    router_replicas: DEPLOYMENT.spec.replicas,
  };
}

function stage_log_scenario(start_seconds) {
  return {
    executor: "per-vu-iterations",
    exec: "log_stage_boundary",
    vus: 1,
    iterations: 1,
    startTime: `${start_seconds}s`,
    maxDuration: "30s",
  };
}

//#endregion Scenarios setup


//#region Scenarios execution
export function log_stage_boundary() {
  const SCENARIO_NAME = exec.scenario.name;
  const LOAD_STAGE = /^log_load_stage_(\d+)_(start|end)$/.exec(SCENARIO_NAME);

  if (LOAD_STAGE) {
    const STAGE_INDEX = Number(LOAD_STAGE[1]) - 1;
    const MULTIPLIER = LOAD_STAGES[STAGE_INDEX];
    console.log(
      `run_id=${RUN_ID} load_stage=${STAGE_INDEX + 1} event=${LOAD_STAGE[2]} ` +
        `utc=${new Date().toISOString()} multiplier=${MULTIPLIER} ` +
        `target_samples_per_second=${stage_sample_rate(MULTIPLIER)} ` +
        `scheduled_samples_per_second=${scheduled_sample_rate(MULTIPLIER)} ` +
        `samples_per_batch=${SAMPLES_PER_BATCH}`,
    );
    return;
  }

  const FINAL_STAGE_HOLD = /^log_final_stage_hold_(start|end)$/.exec(SCENARIO_NAME);
  if (FINAL_STAGE_HOLD) {
    const MULTIPLIER = LOAD_STAGES[LOAD_STAGES.length - 1];
    console.log(
      `run_id=${RUN_ID} load_stage=final_hold event=${FINAL_STAGE_HOLD[1]} ` +
        `utc=${new Date().toISOString()} multiplier=${MULTIPLIER} ` +
        `target_samples_per_second=${stage_sample_rate(MULTIPLIER)} ` +
        `scheduled_samples_per_second=${scheduled_sample_rate(MULTIPLIER)} ` +
        `samples_per_batch=${SAMPLES_PER_BATCH}`,
    );
  }
}

export function send_batch() {
  const BATCH_INDEX = exec.scenario.iterationInTest % SERIES_BATCHES;
  const SERIES_ID_START = BATCH_INDEX * SAMPLES_PER_BATCH;
  const SERIES_ID_END = SERIES_ID_START + SAMPLES_PER_BATCH;
  const RESPONSE = rw_client.storeFromPrecompiledTemplates(
    0,
    100,
    Date.now(),
    SERIES_ID_START,
    SERIES_ID_END,
    label_template,
  );

  check(RESPONSE, {
    "Prometheus Agent accepted batch": (response) =>
      response.status >= 200 && response.status < 300,
  });
  if (RESPONSE.status < 200 || RESPONSE.status >= 300) {
    const MESSAGE = `run_id=${RUN_ID} event=send_batch_failed utc=${new Date().toISOString()} ` +
        `status=${RESPONSE.status} body=${RESPONSE.body}`;
    exec.test.abort(MESSAGE);
    throw new Error(MESSAGE);
  }
}

export function monitor_shedding(data) {
  abort_if_shedding(data.agent_counters, "continuous-monitor");
  sleep(duration_seconds(SHEDDING_CHECK_INTERVAL));
}

export function gate_next_stage(data) {
  const DEADLINE = Date.now() + (STAGE_GATE_DELAY_SECONDS - 1) * 1000;
  let current;

  while (Date.now() < DEADLINE) {
    abort_if_shedding(data.agent_counters, exec.scenario.name);
    current = get_agent_counters();

    if (
      current.pending_samples === 0 &&
      agent_lag_seconds(current) <= AGENT_MAX_LAG_SECONDS
    ) {
      console.log(
        `run_id=${RUN_ID} event=stage-gate-passed utc=${new Date().toISOString()} ` +
          `next_stage=${exec.scenario.name.replace("gate_load_stage_", "")}`,
      );
      return;
    }

    sleep(5);
  }

  const MESSAGE =
    `agent did not drain: pending_samples=${current?.pending_samples || 0} ` +
    `lag_seconds=${current ? agent_lag_seconds(current) : 0}`;
  exec.test.abort(MESSAGE);
  throw new Error(MESSAGE);
}

export function restart_routers(data) {
  abort_if_shedding(data.agent_counters, "pre-router-restart");

  console.log(
    `run_id=${RUN_ID} event=router-restart-start utc=${new Date().toISOString()} ` +
      `deployment=${ROUTER_DEPLOYMENT_NAME} replicas=${data.router_replicas}`,
  );
  rollout_restart_router_deployment();
  wait_for_router_deployment(data.router_replicas);
  console.log(
    `run_id=${RUN_ID} event=router-restart-end utc=${new Date().toISOString()} ` +
      `deployment=${ROUTER_DEPLOYMENT_NAME} replicas=${data.router_replicas}`,
  );
}

export function teardown() {
  const AGENT = get_agent_counters();
  const ROUTER = get_router_deployment();
  console.log(
    `run_id=${RUN_ID} event=teardown utc=${new Date().toISOString()} ` +
      `failed_samples=${AGENT.failed_samples} retried_samples=${AGENT.retried_samples} ` +
      `pending_samples=${AGENT.pending_samples} agent_lag_seconds=${agent_lag_seconds(AGENT)} ` +
      `router_replicas=${ROUTER.spec.replicas} updated_replicas=${ROUTER.status?.updatedReplicas || 0} ` +
      `available_replicas=${ROUTER.status?.availableReplicas || 0} ` +
      `unavailable_replicas=${ROUTER.status?.unavailableReplicas || 0}`,
  );
}

//#endregion Scenarios execution


//#region Utilities

function abort_if_shedding(baseline, source) {
  if (!check_shedding(baseline)) {
    return;
  }

  const MESSAGE =
    `run_id=${RUN_ID} event=shedding-detected utc=${new Date().toISOString()} ` +
    `source=${source}; aborting before another load stage starts`;
  console.error(MESSAGE);
  exec.test.abort(MESSAGE);
}

// Failed/retried remote writes is a syntom for load shedding.
function check_shedding(baseline) {
  const CURRENT = get_agent_counters();
  return (
    CURRENT.failed_samples > baseline.failed_samples ||
    CURRENT.retried_samples > baseline.retried_samples
  );
}

function agent_lag_seconds(counters) {
  return Math.max(0, counters.highest_timestamp - counters.highest_sent_timestamp);
}

// Get Prometheus Agent RW metrics.
function get_agent_counters() {
  // Get with retries to avoid failures for unready Prometheus Agent.
  const RESPONSE = http.get(PROMETHEUS_METRICS_URL, { timeout: "2s" });
  if (RESPONSE.status !== 200) {
    const MESSAGE =
      `failed to read Prometheus Agent metrics: HTTP ${RESPONSE.status}: ${RESPONSE.body}`;
    console.error(`run_id=${RUN_ID} event=shedding-check-failed utc=${new Date().toISOString()}`);
    exec.test.abort(MESSAGE);
    throw new Error(MESSAGE);
  }

  return {
    failed_samples: metric_sum(
      RESPONSE.body,
      "prometheus_remote_storage_samples_failed_total",
    ),
    retried_samples: metric_sum(
      RESPONSE.body,
      "prometheus_remote_storage_samples_retried_total",
    ),
    pending_samples: metric_sum(
      RESPONSE.body,
      "prometheus_remote_storage_samples_pending",
    ),
    highest_timestamp: metric_sum(
      RESPONSE.body,
      "prometheus_remote_storage_queue_highest_timestamp_seconds",
    ),
    highest_sent_timestamp: metric_sum(
      RESPONSE.body,
      "prometheus_remote_storage_queue_highest_sent_timestamp_seconds",
    ),
  };
}

function metric_sum(metrics, metric_name) {
  let total = 0;
  for (const LINE of metrics.split("\n")) {
    if (
      !LINE.startsWith(metric_name) ||
      (LINE.includes("{") && !LINE.includes(`remote_name="${REMOTE_WRITE_NAME}"`))
    ) {
      continue;
    }
    const MATCH = new RegExp(`^${metric_name}(?:\\{[^}]*\\})?\\s+([^\\s]+)`).exec(LINE);
    if (MATCH) {
      total += Number(MATCH[1]);
    }
  }
  return total;
}

function get_router_deployment() {
  const RESPONSE = http.get(deployment_url(), kube_request_params());
  if (RESPONSE.status !== 200) {
    fail(
      `failed to get ${ROUTER_DEPLOYMENT_NAME}: HTTP ${RESPONSE.status}: ${RESPONSE.body}`,
    );
  }
  return RESPONSE.json();
}

// Same PATCH produced by `kubectl rollout restart deployment/...`.
function rollout_restart_router_deployment() {
  const PATCH = {
    spec: {
      template: {
        metadata: {
          annotations: {
            "kubectl.kubernetes.io/restartedAt": new Date().toISOString(),
          },
        },
      },
    },
  };
  const RESPONSE = http.patch(
    deployment_url(),
    JSON.stringify(PATCH),
    kube_request_params("application/strategic-merge-patch+json"),
  );
  if (RESPONSE.status !== 200) {
    fail(
      `failed to restart ${ROUTER_DEPLOYMENT_NAME}: HTTP ${RESPONSE.status}: ${RESPONSE.body}`,
    );
  }
}

function wait_for_router_deployment(replicas) {
  const TIMEOUT_SECONDS = duration_seconds(__ENV.ROLLING_UPDATE_TIMEOUT || "10m");
  const DEADLINE = Date.now() + TIMEOUT_SECONDS * 1000;

  while (Date.now() < DEADLINE) {
    const DEPLOYMENT = get_router_deployment();
    const STATUS = DEPLOYMENT.status || {};
    if (
      STATUS.observedGeneration >= DEPLOYMENT.metadata.generation &&
      STATUS.updatedReplicas === replicas &&
      STATUS.availableReplicas === replicas &&
      (STATUS.unavailableReplicas || 0) === 0
    ) {
      return;
    }
    sleep(5);
  }

  fail(
    `timed out waiting for ${ROUTER_DEPLOYMENT_NAME} to have ${replicas} updated replicas`,
  );
}

function deployment_url() {
  return (
    `${KUBE_API_SERVER_URL}/apis/apps/v1/namespaces/${KUBE_NAMESPACE}` +
    `/deployments/${ROUTER_DEPLOYMENT_NAME}`
  );
}

function kube_request_params(content_type) {
  const HEADERS = { Authorization: `Bearer ${SERVICE_ACCOUNT_TOKEN}` };
  if (content_type) {
    HEADERS["Content-Type"] = content_type;
  }
  return { headers: HEADERS, timeout: "30s" };
}

function stage_sample_rate(multiplier) {
  return Math.max(1, Math.round(NORMAL_SAMPLES_PER_SECOND * multiplier));
}

function stage_request_rate(multiplier) {
  return Math.max(1, Math.ceil(stage_sample_rate(multiplier) / SAMPLES_PER_BATCH));
}

function scheduled_sample_rate(multiplier) {
  return stage_request_rate(multiplier) * SAMPLES_PER_BATCH;
}

function parse_load_stages(value) {
  let stages;
  try {
    stages = JSON.parse(value);
  } catch (error) {
    throw new Error(`LOAD_STAGES must be a JSON array: ${error.message}`);
  }

  if (!Array.isArray(stages) || stages.length === 0) {
    throw new Error("LOAD_STAGES must contain at least one multiplier");
  }

  return stages.map(stage => Number(stage));
}

function duration_seconds(value) {
  const MATCH = /^(\d+)(s|m|h)$/.exec(value);
  if (!MATCH) {
    throw new Error(`unsupported duration ${value}; use s, m, or h`);
  }

  const UNITS = { s: 1, m: 60, h: 3600 };
  return Number(MATCH[1]) * UNITS[MATCH[2]];
}

function required(value, name) {
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

//#endregion Utilities
