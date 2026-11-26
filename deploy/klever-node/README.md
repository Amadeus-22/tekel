# KleverChain node dashboard and alerts

A Grafana dashboard and Prometheus alert rules for klever-go nodes (validators and
observers), built from the metrics the node already serves on `/node/metrics`. Tekel
itself is not needed: point Prometheus at your node.

| File | What it is |
|---|---|
| `prometheus.yml` | scrape config for `/node/metrics` (port 8080 by default), loads `alerts.yml` |
| `alerts.yml` | 9 alert rules: node down, stalled, behind, syncing, few peers, disk, clock offset, missed leader slots, low consensus participation |
| `alerts_test.yml` | `promtool test rules` cases for the rules above |
| `klever-node.json` | Grafana dashboard: health, chain, validator, machine |

## Use

1. Add the `klever-node` job from `prometheus.yml` to your Prometheus, with one target
   per node and a `node` label to name it. Copy `alerts.yml` next to it.
2. In Grafana: Dashboards → New → Import → upload `klever-node.json`, pick your
   Prometheus data source.
3. Route the alerts with Alertmanager (or Grafana alerting) to Telegram, Slack or e-mail.

Check the rules: `promtool check config prometheus.yml` and `promtool test rules alerts_test.yml`.

## Notes

- `/validator/statistics` returns JSON, not the Prometheus text format, so Prometheus
  cannot scrape it. Everything here comes from `/node/metrics`.
- The validator panels and alerts use `klv_count_leader`, `klv_count_accepted_blocks`,
  `klv_count_consensus` and `klv_count_consensus_accepted_blocks`. They stay at 0 on
  observers, so those panels show "no slots" and the alerts stay quiet.
- Scrape your own node, not a load-balanced public endpoint: behind a load balancer each
  scrape can reach a different node, and running totals such as
  `klv_num_transactions_processed` jump between them.
- Checked against the public mainnet node (klever-go v1.7.23): every dashboard query
  returns data. The validator panels were only checked with `promtool` test data.
