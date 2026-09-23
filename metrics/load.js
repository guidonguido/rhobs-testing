import { check } from "k6";
import exec from "k6/execution";
import remote from "k6/x/remotewrite";

// Samples per second to reach 1% ingesters memory usage.
// e.g. for an ingester set with 120GiB usable memory, 1% is 1.2GiB, which is ~600 samples/s
// based on 6.51GiB per 3200 samples/s observation.
const UNIT_SAMPLE_PER_SECOND_LOAD = Math.max(1, Number(__ENV.UNIT_SAMPLE_PER_SECOND_LOAD || 600));

// Load stages are multipliers of the base load, 
// e.g. [10, 20, 40] means 10%, 20% and 40% of the ingesters memory usage
// under the same conditions.
const LOAD_STAGES = parse_load_stages(__ENV.LOAD_STAGES || "[25, 50, 80, 100]");

// Duration of each load stage in minutes. 
// The list must have the same number of elements as LOAD_STAGES
// e.g. [10, 10, 10] for 3 stages of 10 minutes each.
const STAGE_DURATION_SECONDS = validate_stage_duration(
  __ENV.STAGE_DURATION_MINUTES || "[125, 125, 125, 125]",
);

// Time between stages. It must cover the preceding scenario's graceful-stop window 
// so the two load-producing scenarios cannot write the same series concurrently.
const SCENARIO_GRACEFUL_STOP_SECONDS = Number(__ENV.SCENARIO_GRACEFUL_STOP_SECONDS) || 90;

// Remote Write payload size.
const SAMPLES_PER_BATCH = Math.min(2000, UNIT_SAMPLE_PER_SECOND_LOAD);

// List of target Prometheus Agent endpoints.
const PROMETHEUS_RECEIVER_URLS = parse_prometheus_receiver_urls(
  required(__ENV.PROMETHEUS_RECEIVER_URL, "PROMETHEUS_RECEIVER_URL"));

// Number of series batches to generate. It must be a multiple of the number of Prometheus Agents.
const SERIES_BATCHES = validate_series_batches(
  __ENV.SERIES_BATCHES || "12",
  PROMETHEUS_RECEIVER_URLS.length,
);

// Unique identifier for this load test run, used to annotate
// the router deployment and identify generated metric samples.
const RUN_ID = __ENV.RUN_ID || "manual";

const rw_clients_pool = [];
for (const url of PROMETHEUS_RECEIVER_URLS) {
  rw_clients_pool.push(new remote.Client({
    url: url,
    timeout: "30s",
  }));
}
const label_template = remote.precompileLabelTemplates({
  __name__: "rhobs_load_test_sample",
  run_id: RUN_ID,
  series_id: "${series_id}",
  cardinality_10: "${series_id/10}",
  cardinality_100: "${series_id/100}",
});

//#region Scenarios setup

// Each stage uses one scenario that sends batches in a deterministic request cycle, 
// plus one scenario for each stage boundary log.
const scenarios = {};
let next_stage_start_seconds = 0;
for (let stage_index = 0; stage_index < LOAD_STAGES.length; stage_index += 1) {
  const STAGE_NUMBER = stage_index + 1;
  const STAGE_START_SECONDS = next_stage_start_seconds;
  const STAGE_END_SECONDS = STAGE_START_SECONDS + STAGE_DURATION_SECONDS[stage_index];

  // First scenario of the stage: generate load at the configured multiplier.
  scenarios[`load_stage_${STAGE_NUMBER}`] = {
    executor: "constant-arrival-rate",
    exec: "send_batch",
    // request/s i.e. iterations, exactly matching the stage sample rate.
    rate: stage_request_rate(LOAD_STAGES[stage_index]),
    timeUnit: "1s",
    duration: `${STAGE_DURATION_SECONDS[stage_index]}s`,
    startTime: `${STAGE_START_SECONDS}s`,
    gracefulStop: `${SCENARIO_GRACEFUL_STOP_SECONDS}s`,
    preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || 100),
    maxVUs: Number(__ENV.MAX_VUS || 2000),
  };

  // Second and third scenarios of the stage: log the start and end of the stage.
  scenarios[`log_load_stage_${STAGE_NUMBER}_start`] = stage_log_scenario(
    STAGE_START_SECONDS,
  );
  scenarios[`log_load_stage_${STAGE_NUMBER}_end`] = stage_log_scenario(STAGE_END_SECONDS);

  next_stage_start_seconds = STAGE_END_SECONDS + SCENARIO_GRACEFUL_STOP_SECONDS;
}

export const options = {
  setupTimeout: "1m",
  teardownTimeout: "1m",
  thresholds: {
    dropped_iterations: [{
      threshold: "count==0",
      abortOnFail: true,
      delayAbortEval: "0s",
    }],
  },
  scenarios: scenarios,
};

export function setup() {
  console.log(
    `run_id=${RUN_ID} event=setup utc=${new Date().toISOString()} ` +
      `samples_per_batch=${SAMPLES_PER_BATCH} `,
  );

  return
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

  const STAGE_INDEX = Number(LOAD_STAGE[1]) - 1;
  const TARGET_MEM = LOAD_STAGES[STAGE_INDEX];
  console.log(
    `run_id=${RUN_ID} load_stage=${STAGE_INDEX + 1} event=${LOAD_STAGE[2]} ` +
      `utc=${new Date().toISOString()} target_mem=${TARGET_MEM} ` +
      `target_samples_per_second=${stage_sample_rate(TARGET_MEM)} ` +
      `requests_per_second=${stage_request_rate(TARGET_MEM)} ` +
      `samples_per_batch=${SAMPLES_PER_BATCH}`,
  );
  return;
}

export function send_batch() {
  const BATCH_INDEX = exec.scenario.iterationInTest % SERIES_BATCHES;
  const CLIENT_INDEX = BATCH_INDEX % rw_clients_pool.length;
  const SERIES_ID_START = BATCH_INDEX * SAMPLES_PER_BATCH;
  const SERIES_ID_END = SERIES_ID_START + SAMPLES_PER_BATCH;
  const RESPONSE = rw_clients_pool[CLIENT_INDEX].storeFromPrecompiledTemplates(
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

//#endregion Scenarios execution


//#region Utilities

function stage_sample_rate(multiplier) {
  const samples_per_second = UNIT_SAMPLE_PER_SECOND_LOAD * multiplier;
  if (!Number.isSafeInteger(samples_per_second)) {
    throw new Error(
      `stage sample rate must be a safe integer: ${UNIT_SAMPLE_PER_SECOND_LOAD} * ${multiplier}`,
    );
  }
  return samples_per_second;
}

function stage_request_rate(multiplier) {
  const samples_per_second = stage_sample_rate(multiplier);
  if (samples_per_second % SAMPLES_PER_BATCH !== 0) {
    throw new Error(
      `stage sample rate ${samples_per_second} must divide evenly into ${SAMPLES_PER_BATCH}-sample batches`,
    );
  }
  return samples_per_second / SAMPLES_PER_BATCH;
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

  return stages.map((stage, index) => {
    if (!Number.isSafeInteger(stage) || stage <= 0) {
      throw new Error(`LOAD_STAGES[${index}] must be a positive integer`);
    }
    return stage;
  });
}

function parse_prometheus_receiver_urls(value) {
  let urls;
  try {
    urls = JSON.parse(value);
  } catch (error) {
    throw new Error(`PROMETHEUS_RECEIVER_URL must be a JSON array: ${error.message}`);
  }

  if (!Array.isArray(urls) || urls.length === 0) {
    throw new Error("PROMETHEUS_RECEIVER_URL must contain at least one URL");
  }

  return urls.map(url => String(url));
}

function required(value, name) {
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function validate_series_batches(value, agent_count) {
  const batches = Number(value);
  if (!Number.isSafeInteger(batches) || batches <= 0) {
    throw new Error("SERIES_BATCHES must be a positive integer");
  }
  if (batches % agent_count !== 0) {
    throw new Error(
      `SERIES_BATCHES (${batches}) must be a multiple of ${agent_count} (Prometheus Agents)`,
    );
  }
  return batches;
}

function validate_stage_duration(value) {
  let durations;
  try {
    durations = JSON.parse(value);
  } catch (error) {
    throw new Error(`STAGE_DURATION must be a JSON array: ${error.message}`);
  }

  if (!Array.isArray(durations) || durations.length === 0 || durations.length !== LOAD_STAGES.length) {
    throw new Error("STAGE_DURATION_MINUTES must contain the same number of elements as LOAD_STAGES");
  }

  return durations.map(duration => duration * 60);
}

//#endregion Utilities
