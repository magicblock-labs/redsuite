use std::time::{Duration, Instant};

use account::Account;
use anyhow::{ensure, Context};
use instruction::Instruction;
use pubkey::Pubkey;
use signature::Signature;

use crate::{
    programs::{layout, CellHeader, HashState, REDLINE},
    ChainCtx, Result, TxSender,
};

pub const STEPS: u64 = 3;
pub const CU_LIMIT: u32 = 1_400_000;
pub const HASH_INIT: Pubkey = Pubkey::new_from_array([7u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Step {
    X,
    Y,
    Z,
}

impl Step {
    pub const ALL: [Step; 3] = [Step::X, Step::Y, Step::Z];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn label(self) -> &'static str {
        match self {
            Step::X => "X(A)",
            Step::Y => "Y(A,B)",
            Step::Z => "Z(B)",
        }
    }
    pub fn accounts(self, a: Pubkey, b: Pubkey) -> Vec<Pubkey> {
        match self {
            Step::X => vec![a],
            Step::Y => vec![a, b],
            Step::Z => vec![b],
        }
    }
}

pub struct CausalPair {
    a: Pubkey,
    b: Pubkey,
    model: PairModel,
    pub last_signature: Option<Signature>,
    senders: Vec<TxSender>,
}

impl CausalPair {
    pub fn new(keys: [Pubkey; 2], senders: Vec<TxSender>) -> Self {
        Self {
            a: keys[0],
            b: keys[1],
            senders,
            model: PairModel::default(),
            last_signature: None,
        }
    }

    pub async fn send(&mut self, step: Step, id: u64, iters: u32) -> Result<(Signature, Duration)> {
        let ixs = chain_ixs(id, iters, &step.accounts(self.a, self.b));
        let sender = &self.senders[step.index()];
        let tx = sender.prepare(&ixs).await?;
        let started = Instant::now();
        let signature = sender
            .submit_prepared(&tx)
            .await
            .with_context(|| format!("step {step:?}, id {id}"))?;
        let elapsed = started.elapsed();
        self.last_signature = Some(signature);
        self.model.apply(step, id, iters);
        Ok((signature, elapsed))
    }

    pub async fn read(&self, ctx: &impl ChainCtx) -> Result<[Option<Account>; 2]> {
        Ok([ctx.account(&self.a).await?, ctx.account(&self.b).await?])
    }

    pub fn compare(&self, accounts: &[Option<Account>; 2], scope: &str) -> Result<()> {
        self.model.compare(accounts, scope)
    }

    pub async fn verify(&self, ctx: &impl ChainCtx, scope: &str) -> Result<()> {
        self.compare(&self.read(ctx).await?, scope)
    }
}
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct PairModel {
    pub a: [u8; 32],
    pub b: [u8; 32],
    pub a_id: u64,
    pub b_id: u64,
}

impl PairModel {
    pub fn apply(&mut self, step: Step, id: u64, iters: u32) {
        match step {
            Step::X => {
                self.a = HashState::fold(id, &[self.a], iters);
                self.a_id = id;
            }
            Step::Y => {
                let merged = HashState::fold(id, &[self.a, self.b], iters);
                self.a = merged;
                self.b = merged;
                self.a_id = id;
                self.b_id = id;
            }
            Step::Z => {
                self.b = HashState::fold(id, &[self.b], iters);
                self.b_id = id;
            }
        }
    }

    pub fn compare(&self, accounts: &[Option<Account>; 2], scope: &str) -> Result<()> {
        for (label, account, expected_hash, expected_id) in
            [("A", &accounts[0], self.a, self.a_id), ("B", &accounts[1], self.b, self.b_id)]
        {
            let account =
                account.as_ref().with_context(|| format!("{scope}: account {label} missing"))?;
            let id = CellHeader::try_decode(&account.data)?.value;
            let hash = CellHeader::payload(&account.data)?
                .first_chunk::<{ layout::HASH_SIZE }>()
                .with_context(|| format!("{scope}: account {label} has no hash"))?;
            ensure!((hash, id) == (&expected_hash, expected_id));
        }
        Ok(())
    }
}

pub fn compute_unit_limit(limit: u32) -> Instruction {
    let mut data = vec![2u8];
    data.extend_from_slice(&limit.to_le_bytes());
    Instruction {
        program_id: sdk_ids::compute_budget::ID,
        accounts: Vec::new(),
        data,
    }
}
pub fn chain_ixs(id: u64, iters: u32, accounts: &[Pubkey]) -> Vec<Instruction> {
    let fold = REDLINE.fold_hashes(id, iters, accounts);
    if iters > 0 {
        vec![compute_unit_limit(CU_LIMIT), fold]
    } else {
        vec![fold]
    }
}
pub fn independent_ixs(id: u64, account: Pubkey, iters: u32) -> Vec<Instruction> {
    vec![
        compute_unit_limit(CU_LIMIT),
        REDLINE.hash(id, HASH_INIT.to_bytes(), iters, &[account]),
    ]
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
