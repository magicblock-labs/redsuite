use std::rc::Rc;

use anyhow::{anyhow, ensure, Context};
use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    programs::{CellHeader, Redline, REDLINE},
    topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use crate::{ACCOUNT_SPACE, PAYER_LAMPORTS};

const WRITES: [u64; 2] = [61, 62];

pub struct MultiProgramClone {
    er: ErCtx,
    payer: Keypair,
    accounts: [(Pubkey, Pubkey); 2],
}

impl Scenario for MultiProgramClone {
    async fn setup(base: Rc<BaseCtx>, er: ErCtx, _: ()) -> Result<Self> {
        let [(first_program, _), (second_program, _)] = topology::redline_loader_v3_pair();

        let payer = base.funded_payer(PAYER_LAMPORTS).await?;
        let first_pda = base
            .cells(REDLINE, &payer)
            .at(first_program)
            .create_delegated(ACCOUNT_SPACE, er.identity(), 0)
            .await?;
        let second_pda = base
            .cells(REDLINE, &payer)
            .at(second_program)
            .create_delegated(ACCOUNT_SPACE, er.identity(), 1)
            .await?;

        Ok(Self {
            er,
            payer,
            accounts: [(first_program, first_pda), (second_program, second_pda)],
        })
    }

    async fn run(self) -> Result<()> {
        let Self { er, payer, accounts } = self;
        let writes: [Instruction; 2] = std::array::from_fn(|index| {
            let (program, account) = accounts[index];
            Redline::at(program).write(WRITES[index], &[account])
        });
        er.submit_and_confirm(&payer, &writes).await?;

        for ((program, _), label) in accounts.iter().zip(["first", "second"]) {
            let clone = er
                .account(program)
                .await?
                .ok_or_else(|| anyhow!("{label} program not cloned after the transaction"))?;
            ensure!(
                clone.executable,
                "the {label} cloned program must be executable on the ER"
            );
        }
        for ((_, key), expected) in accounts.iter().zip(WRITES) {
            let clone = er
                .account(key)
                .await?
                .with_context(|| format!("account {key} missing on the ER"))?;
            ensure!(CellHeader::written_id(&clone.data) == Some(expected));
        }

        Ok(())
    }
}
