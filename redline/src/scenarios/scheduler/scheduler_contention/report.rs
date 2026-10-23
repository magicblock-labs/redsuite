use super::{background::LoadMode, BatchOutcome, SchedulerContention, OPEN_CONCURRENCY};
use redsuite_core::{
    report::Unit::{Count, Micros, Ratio, Seconds, Tps},
    ScenarioReport,
};

impl SchedulerContention {
    pub(super) fn summarize(
        &self,
        batches: &[BatchOutcome],
        executors: f64,
        contention_lanes: usize,
    ) -> ScenarioReport {
        let chain_txs: u64 = batches.iter().map(BatchOutcome::chain_txs).sum();
        let independent_txs: u64 = batches.iter().map(BatchOutcome::independent_txs).sum();
        let chains: u64 =
            batches.iter().map(|batch| batch.chains() * self.profile.pairs as u64).sum();
        let dependencies: f64 = batches.iter().map(|batch| batch.dependencies).sum();
        let engine_txs: f64 = batches.iter().map(|batch| batch.engine_txs).sum();
        let longest_stall = batches
            .iter()
            .flat_map(|batch| batch.phases.iter())
            .map(|phase| phase.samples.value.max_stall)
            .max()
            .unwrap_or_default();
        let slowest_drain =
            batches.iter().map(|batch| batch.drained.elapsed).max().unwrap_or_default();
        let mut summary = ScenarioReport::ok()
            .setting("profile", self.profile.name)
            .setting(
                "shape",
                "X(A) -> Y(A,B) -> Z(B) hash-fold chains, one payer per \
                 step, beside high-cu work on disjoint accounts",
            )
            .setting("pairs", self.profile.pairs)
            .setting("chains per pair per batch", self.profile.chains)
            .setting("batches", batches.len())
            .setting("heavy step iters", self.profile.heavy_iters)
            .setting("independent accounts", self.profile.independent_accounts)
            .setting("independent iters", self.profile.independent_iters)
            .setting("driver threads", self.profile.driver_threads)
            .setting("phase span s", self.profile.phase_span.as_secs())
            .setting("executors", executors)
            .setting("contention lanes", contention_lanes)
            .setting(
                "contention load",
                LoadMode::Bounded { lanes: contention_lanes }.shape(),
            )
            .setting(
                "backpressure load",
                LoadMode::Open { concurrency: OPEN_CONCURRENCY }.shape(),
            )
            .metric("chains", Count, chains as f64)
            .metric("chain txs", Count, chain_txs as f64)
            .metric("independent txs", Count, independent_txs as f64)
            .metric("engine txs", Count, engine_txs)
            .metric("ordering dependencies", Count, dependencies)
            .metric(
                "dependencies per chain",
                Ratio,
                dependencies / chains.max(1) as f64,
            )
            .metric("longest stall s", Seconds, longest_stall.as_secs_f64())
            .metric("slowest drain s", Seconds, slowest_drain.as_secs_f64());
        for (index, batch) in batches.iter().enumerate() {
            summary = summary
                .metric(
                    format!("batch{index} ordering dependencies"),
                    Count,
                    batch.dependencies,
                )
                .metric(
                    format!("batch{index} drain s"),
                    Seconds,
                    batch.drained.elapsed.as_secs_f64(),
                );
            for phase in &batch.phases {
                for (label, unit, value) in [
                    ("chain send p50 us", Micros, phase.send.median as f64),
                    ("chain send p95 us", Micros, phase.send.quantile95 as f64),
                    (
                        "busy executors mean",
                        Count,
                        phase.samples.value.busy.mean(),
                    ),
                    (
                        "independent tps",
                        Tps,
                        phase.load.delivered as f64 / phase.load.wall.as_secs_f64().max(1e-9),
                    ),
                ] {
                    summary = summary.metric(
                        format!("batch{index} {} {label}", phase.label),
                        unit,
                        value,
                    );
                }
            }
        }
        summary
    }
}
