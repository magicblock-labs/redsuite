//! Unified public-API client (JSON-RPC + WebSocket) over the transport layer.

use std::collections::HashMap;

pub struct Api {}

/// Snapshot of the ER's Prometheus `/metrics` endpoint.
pub struct Metrics(pub HashMap<String, f64>);

impl Metrics {
    pub fn get(&self, name: &str) -> Option<f64> {
        self.0.get(name).copied()
    }
}
