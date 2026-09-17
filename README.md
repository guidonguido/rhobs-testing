# RHOBS Testing

Tests collection for validating RHOBS environment behavior. Tests are intended to run with environment-specific endpoints and credentials supplied by the workload that deploys them; this repository is kept intentionally generic and does not contain deployment manifests or secrets.

## Available tests

| Path | Purpose |
| --- | --- |
| [`metrics/`](metrics/) | Drives Prometheus remote-write traffic with k6 and validates the load-shedding path while restarting a Receive router deployment. |

## Metrics load test

[`metrics/load.js`](metrics/load.js) uses a [`k6`](https://github.com/grafana/xk6) binary built with the [`xk6-client-prometheus-remote`](https://github.com/grafana/xk6-client-prometheus-remote) extension. It sends batches of the `rhobs_load_test_sample` metric, with a per-run `run_id` label and bounded synthetic cardinality labels.

The test increases traffic through the configured `LOAD_STAGES`, waits for the Prometheus Agent's remote-write queue to drain between stages, and continuously checks the Observatorium `throttle_rejected_total{handler="metrics"}` counter. If that counter increases, the test aborts before scheduling more load. If no throttling is detected, it holds the highest rate and performs a rollout restart of the configured Receive router deployment.

## License

RHOBS Testing is distributed under the [AGPL-3.0 License](LICENSE).
