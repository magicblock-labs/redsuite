use instruction::Instruction;
use pubkey::Pubkey;
use redline_interface::{instruction::build, layout, utils::fold_hash};

use crate::{ChainCtx, CheckError, Result};

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
                self.a = fold_hash(id, &[self.a], iters);
                self.a_id = id;
            }
            Step::Y => {
                let merged = fold_hash(id, &[self.a, self.b], iters);
                self.a = merged;
                self.b = merged;
                self.a_id = id;
                self.b_id = id;
            }
            Step::Z => {
                self.b = fold_hash(id, &[self.b], iters);
                self.b_id = id;
            }
        }
    }

    pub async fn verify(
        &self,
        ctx: &impl ChainCtx,
        keys: [Pubkey; 2],
        scope: &str,
    ) -> Result<()> {
        for (label, address, expected_hash, expected_id) in [
            ("A", keys[0], self.a, self.a_id),
            ("B", keys[1], self.b, self.b_id),
        ] {
            let account = ctx.account(&address).await?.ok_or_else(|| {
                format!("{scope}: account {label} {address} is missing")
            })?;
            let hash = account
                .data
                .get(
                    layout::HASH_OFFSET
                        ..layout::HASH_OFFSET + layout::HASH_SIZE,
                )
                .ok_or_else(|| {
                    format!(
                        "{scope}: account {label} holds {} bytes, fewer than \
                         the {} the layout needs",
                        account.data.len(),
                        layout::HASH_OFFSET + layout::HASH_SIZE
                    )
                })?;
            let id =
                super::written_id(&account.data).expect("hash follows the id");
            if hash != expected_hash || id != expected_id {
                return Err(CheckError::new(format!(
                    "{scope}: account {label} must hold the fold of its \
                     accepted X/Y/Z history"
                ))
                .expected(format!(
                    "id {expected_id}, hash {}",
                    hex(&expected_hash)
                ))
                .actual(format!("id {id}, hash {}", hex(hash)))
                .into());
            }
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
    let fold = build::hash_fold(id, iters, accounts);
    if iters > 0 {
        vec![compute_unit_limit(CU_LIMIT), fold]
    } else {
        vec![fold]
    }
}
pub fn independent_ixs(
    id: u64,
    account: Pubkey,
    iters: u32,
) -> Vec<Instruction> {
    vec![
        compute_unit_limit(CU_LIMIT),
        build::expensive_hash_compute(id, HASH_INIT, iters, &[account]),
    ]
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
