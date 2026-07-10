//! Transport layer. The HTTP shim and rate pacer are in; redline's pooled
//! HTTP/WS transports and confirmation tracking land next.

pub mod http;
pub mod rate;
pub mod subpool;
pub mod ws;
pub mod wsraw;
