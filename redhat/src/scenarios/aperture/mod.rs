use std::{rc::Rc, result, time::Duration};

use anyhow::{ensure, Context};
use hash::Hash;
use instruction::{AccountMeta, Instruction};
use keypair::Keypair;
use pubkey::Pubkey;
use serde_json::{json, Value};
use signature::Signature;
use signer::Signer;
use solana_instruction_error::InstructionError;

use redsuite_core::{
    api::{transaction_meta, RpcError},
    netfault::Selector,
    programs::{CellHeader, REDSHIFT},
    rpc_types::{
        config::RpcSendTransactionConfig, request::RpcRequest, response::TransactionError,
    },
    signed::{audit, audit_signatures, decode_state, subscribe, Signed},
    transaction_types::{UiTransactionEncoding, UiTransactionError},
    BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use crate::PAYER_LAMPORTS;

const TIMEOUT: Duration = Duration::from_secs(30);

mod authority;
mod hostile;

pub use authority::TransactionAuthority;
pub use hostile::{TransactionAdmission, TransactionStructure};
