# RedSuite

Black-box test harness for the MagicBlock validator. Each test bootstraps its
own topology (base L1 + ephemeral rollup), drives it purely through the public
surface — transactions, JSON-RPC, WebSocket and the Prometheus `/metrics`
endpoint — and emits a performance / correctness / security report that can be
diffed release-over-release.

RedSuite is built on top of [redline](https://github.com/magicblock-labs/redline),
the MagicBlock validator load-testing tool: redline's engine (transport pools,
rate limiting, confirmation tracking, streaming stats, account prep) is being
extracted into `redsuite-core`, topped with a net-new topology harness and a
scenario/assertion layer.

## Layout

    crates/redsuite-core/   the engine: topology, contexts, transport, prep, scenario, stats
    redline/                performance scenarios (load)
    redshift/               correctness scenarios (observed state vs expected model)
    redhat/                 security scenarios (adversarial, must-be-rejected)
    programs/               on-chain SBF fixture programs, one per family

## Writing a scenario

One file per scenario under `<family>/tests/`, one `Scenario` impl, one
`#[tokio::test]` calling `run_scenario` — the harness owns process spawning,
ports, funding and teardown. See `redshift/tests/commit_roundtrip.rs`.

## Running

    cargo nextest run -p redshift --test commit_roundtrip   # one scenario
    cargo nextest run -p redline                            # one family
    cargo nextest run                                       # everything

`.config/nextest.toml` caps how many topologies run concurrently.

## Status

Skeleton. The layout and public API compile; the redline engine port and the
topology harness land next, so scenario tests are `#[ignore]`d for now.
