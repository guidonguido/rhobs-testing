# RHOBS Testing

Tests collection for validating RHOBS environment behavior. Tests are intended to run with environment-specific endpoints and credentials supplied by the workload that deploys them; this repository is kept intentionally generic and does not contain deployment manifests or secrets.

## Available tests

| Path | Purpose |
| --- | --- |
| [`metrics/`](metrics/) | Generates controlled Prometheus remote-write traffic through a configured pool of Prometheus Agents. |

## Metrics load test

[`metrics/load.js`](metrics/load.js) uses a [`k6`](https://github.com/grafana/xk6) binary built with the [`xk6-client-prometheus-remote`](https://github.com/grafana/xk6-client-prometheus-remote) extension. It sends batches of the `rhobs_load_test_sample` metric, with a per-run `run_id` label and bounded synthetic cardinality labels.

The workload generates remote-write traffic only, controlling the series cardinality and saples rate. A non-2xx response from an Agent aborts the k6 run immediately.

## License

RHOBS Testing is distributed under the [AGPL-3.0 License](LICENSE).
