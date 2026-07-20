//! Topology harness: spawn the base L1 and the ER on dynamically allocated
//! ports, with a readiness gate covering the ER's startup checks (identity
//! funding, fee-vault init, mdp registration).
//!
//! One shared boot-once stack serves all scenarios (see `stack`); private
//! per-scenario topologies come back with the restart/ledger-restore family.

mod stack;

pub use stack::{
    current_state, down, er_bin_path, private_er, redline_alias_ids, shared,
    stack_dir, status, workspace_root, ErOptions, PrivateEr, RestartConfig,
    RestartTiming, StackState, COMMITTOR_ID, DLP_ID, MDP_ID,
};
