//! Transport layer. Currently only the minimal HTTP shim; redline's pooled
//! transports (HTTP/WS pools, rate limiting, confirmations) replace it in
//! the engine port.

pub mod http;
