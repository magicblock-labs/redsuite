use std::rc::Rc;

use instruction::Instruction;
use keypair::Keypair;
use pubkey::Pubkey;
use redsuite_core::{
    check, check::CheckError, check_eq, topology, BaseCtx, ChainCtx, ErCtx, Result, Scenario,
};

use crate::{program::instruction::build, ACCOUNT_SPACE, PAYER_LAMPORTS};

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
            .redline(&payer)
            .at(first_program)
            .init_delegated(ACCOUNT_SPACE, er.identity(), 0)
            .await?;
        let second_pda = base
            .redline(&payer)
            .at(second_program)
            .init_delegated(ACCOUNT_SPACE, er.identity(), 1)
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
            build::simple_byte_set_at(program, WRITES[index], &[account])
        });
        er.submit_and_confirm(&payer, &writes).await?;

        for ((program, _), label) in accounts.iter().zip(["first", "second"]) {
            let clone = er.account(program).await?.ok_or_else(|| {
                CheckError::new(format!(
                    "the {label} program must be cloned to the ER after the tx"
                ))
            })?;
            check!(
                clone.executable,
                "the {label} cloned program must be executable on the ER"
            )?;
        }
        for ((_, key), expected) in accounts.iter().zip(WRITES) {
            let clone = er
                .account(key)
                .await?
                .ok_or_else(|| format!("account {key} missing on the ER"))?;
            check_eq!(
                crate::written_id(&clone.data),
                Some(expected),
                "program write to {key}"
            )?;
        }

        Ok(())
    }
}
