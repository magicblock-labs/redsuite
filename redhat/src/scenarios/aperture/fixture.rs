use crate::PAYER_LAMPORTS;
use account::Account;
use anyhow::{ensure, Context};
use instruction::AccountMeta;
use keypair::Keypair;
use message::{Message, VersionedMessage};
use pubkey::Pubkey;
use redsuite_core::{
    programs::{CellHeader, REDSHIFT},
    random::{self, Rng},
    signed::{audit, audit_signatures, decode_state, Signed},
    BaseCtx, ChainCtx, ErCtx, Result,
};
use signature::Signature;
use signer::Signer;
use std::{
    rc::Rc,
    time::{Duration, Instant},
};

const OBSERVE: Duration = Duration::from_millis(250);

pub(super) struct Snapshot {
    pub(super) keys: Vec<Pubkey>,
    pub(super) local: Vec<Option<Account>>,
    pub(super) base: Vec<Option<Account>>,
}

pub(super) struct Fixture {
    pub(super) base: Rc<BaseCtx>,
    pub(super) er: ErCtx,
    pub(super) payer: Keypair,
    pub(super) keys: [Pubkey; 6],
    pub(super) base_images: Vec<Option<Account>>,
    pub(super) expected: Vec<Option<Account>>,
}

pub(super) async fn base_accounts(base: &BaseCtx, keys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
    let mut images = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(100) {
        images.extend(base.accounts(chunk).await?);
    }
    Ok(images)
}

pub(super) fn vacant(account: &Account) -> bool {
    account.lamports == 0
        && account.data.is_empty()
        && account.owner == sdk_ids::system_program::ID
        && !account.executable
}

impl Fixture {
    pub(super) async fn new(base: Rc<BaseCtx>, er: ErCtx, rng: &mut impl Rng) -> Result<Self> {
        let owner = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        let cell = base.cells(REDSHIFT, &owner).create_primary(er.identity(), u32::MAX).await?;
        let payer = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        base.delegate_payer(&owner, &payer, er.identity()).await?;
        er.api()
            .await_accounts(
                &[payer.pubkey(), cell],
                "hostile transaction fixtures cloned",
                decode_state,
            )
            .await?;
        let warm = base.seeded_payer(rng, 1_000_000).await?.pubkey();
        let cold = base.seeded_payer(rng, 1_000_000).await?.pubkey();
        let absent = random::pubkey(rng);
        er.account(&warm).await?.context("warm control missing")?;
        let foreign_owner = base.seeded_payer(rng, PAYER_LAMPORTS).await?;
        let foreign = base
            .cells(REDSHIFT, &foreign_owner)
            .create_primary(random::pubkey(rng), u32::MAX)
            .await?;
        let foreign_image =
            er.account(&foreign).await?.context("foreign readonly control missing")?;
        let keys = [payer.pubkey(), cell, warm, cold, absent, foreign];
        let mut expected = er.local_images(&keys).await?;
        ensure!(expected[..2].iter().all(Option::is_some));
        ensure!(expected[3].is_none() && expected[4].is_none());
        expected[5] = Some(foreign_image);
        let base_images = base_accounts(&base, &keys).await?;
        Ok(Self {
            base,
            er,
            payer,
            keys,
            base_images,
            expected,
        })
    }

    pub(super) async fn snapshot(&self, extra: &[Pubkey]) -> Result<Snapshot> {
        let mut keys = self.keys.to_vec();
        for key in extra {
            if !keys.contains(key) {
                keys.push(*key);
            }
        }
        let local = self.er.local_images(&keys).await?;
        let base = base_accounts(&self.base, &keys).await?;
        Ok(Snapshot { keys, local, base })
    }

    pub(super) async fn unchanged(&self, before: &Snapshot) -> Result<()> {
        let until = Instant::now() + OBSERVE;
        loop {
            ensure!(
                self.er.local_images(&before.keys).await? == before.local,
                "protected local accounts changed: {:?}",
                before.keys
            );
            ensure!(
                base_accounts(&self.base, &before.keys).await? == before.base,
                "protected base accounts changed: {:?}",
                before.keys
            );
            if Instant::now() >= until {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn message(
        &self,
        nonce: u64,
        amount: i64,
        readonly: &[Pubkey],
    ) -> Result<Message> {
        let mut instruction = REDSHIFT.batch().nonce(nonce).add(&self.keys[1..2], amount).build();
        instruction
            .accounts
            .extend(readonly.iter().map(|key| AccountMeta::new_readonly(*key, false)));
        Ok(Message::new_with_blockhash(
            &[instruction],
            Some(&self.payer.pubkey()),
            &self.er.api().get_latest_blockhash().await?,
        ))
    }

    pub(super) async fn check_control(
        &mut self,
        message: VersionedMessage,
        amount: u64,
        revisions: u64,
        skip_preflight: bool,
    ) -> Result<Signature> {
        let tx = Signed::from_versioned(&self.er, &self.payer, message, None).await?;
        ensure!(tx.valid(&self.er).await?);
        let start = self.er.api().get_slot().await?;
        tx.send_config(&self.er, skip_preflight).await??;
        let slot = tx.outcome(&self.er).await?;
        let status = self
            .er
            .api()
            .get_signature_status(&tx.signature)
            .await?
            .context("control terminal status missing")?;
        ensure!(status.slot == slot && status.err.is_none());
        audit(&self.er, start, &[(&tx, 1)]).await?;
        self.expected[0].as_mut().unwrap().lamports -= tx.fee;
        let cell = self.expected[1].as_mut().unwrap();
        let mut header = CellHeader::try_decode(&cell.data)?;
        header.value += amount;
        header.revision += revisions;
        header.write(&mut cell.data)?;
        let actual = self.er.local_images(&self.keys).await?;
        ensure!(actual[..2] == self.expected[..2]);
        for (index, account) in actual.iter().enumerate().skip(2) {
            let expected = if index == 5 {
                self.expected[5].as_ref()
            } else {
                self.base_images[index].as_ref()
            };
            let held = match (account, expected) {
                (None, _) => true,
                (Some(account), None) => vacant(account),
                (Some(account), Some(expected)) => account == expected,
            };
            ensure!(
                held,
                "readonly input {} diverged locally: {account:?} against {expected:?}",
                self.keys[index]
            );
        }
        ensure!(base_accounts(&self.base, &self.keys).await? == self.base_images);
        Ok(tx.signature)
    }

    pub(super) async fn not_admitted(
        &self,
        signature: &Signature,
        start: u64,
        control: Option<&Signature>,
    ) -> Result<()> {
        let mut expected = vec![(signature, 0)];
        expected.extend(control.map(|control| (control, 1)));
        audit_signatures(&self.er, start, &expected).await?;
        ensure!(self.er.api().get_transaction(signature).await?.is_none());
        ensure!(self.er.api().get_signature_status(signature).await?.is_none());
        Ok(())
    }
}
