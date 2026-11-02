use super::*;
use committor_interface::{
    instruction_builder::{
        close_buffer::{create_close_ix, CreateCloseIxArgs},
        init_buffer::{create_init_ix, CreateInitIxArgs},
        write_buffer::{create_write_ix, CreateWriteIxArgs},
    },
    Chunks,
};
use dlp_api::{args::CommitStateFromBufferArgs, error::DlpError, instruction_builder};
use keypair::Keypair;
use rand::seq::SliceRandom;
use redsuite_core::{
    programs::CellHeader,
    random::{self, Rng},
    rpc_types::response::TransactionError,
    topology,
};
use solana_instruction_error::InstructionError;
use std::cell::Cell;
use transaction::Transaction;

const CHUNK: usize = 256;

mod buffers;

/// Seeded successor-nonce probes shared by inline and buffered commit paths.
pub(super) struct Plan {
    markers: Vec<u64>,
    cases: [u8; 6],
    authority: Keypair,
    foreign: Keypair,
    buffers: buffers::Plan,
}

pub(super) fn plan(seed: u64) -> Result<Plan> {
    let mut rng = random::rng(seed, &["redhat", "commit_order"], 0);
    let markers = (0..16).map(|_| rng.gen_range(1..u64::MAX)).collect();
    let mut cases = [0u8, 1, 2, 3, 4, 5];
    cases.shuffle(&mut rng);
    Ok(Plan {
        markers,
        cases,
        authority: topology::identity_for_label("redhat-commit-order-authority")?,
        foreign: topology::identity_for_label("redhat-commit-order-foreign")?,
        buffers: buffers::plan(seed)?,
    })
}

/// A base account delegated to a known signing authority, independent of ER settlement delivery.
struct CommitFixture<'a> {
    base: &'a BaseCtx,
    er: &'a ErCtx,
    payer: &'a Keypair,
    authority: &'a Keypair,
    account: Pubkey,
    sequence: &'a Cell<u64>,
}

impl SettlementAuthorization {
    /// Rejects stale, skipped, and unauthorized successors before accepting the exact next nonce.
    pub(super) async fn check_commit_ordering(&self) -> Result<()> {
        let plan = &self.commit_plan;
        let markers = &plan.markers;
        let cases = &plan.cases;
        let mut fixtures = random::rng(
            self.base.config().seed,
            &["redhat", "commit_order", "fixtures"],
            0,
        );
        let payer = self.base.seeded_payer(&mut fixtures, PAYER_LAMPORTS).await?;
        let authority = &plan.authority;
        let foreign = &plan.foreign;
        self.base.airdrop(&authority.pubkey(), PAYER_LAMPORTS).await?;
        self.base.airdrop(&foreign.pubkey(), PAYER_LAMPORTS).await?;
        let sequence = Cell::new(0);
        for buffered in [false, true] {
            let owner = self.base.seeded_payer(&mut fixtures, PAYER_LAMPORTS).await?;
            let account = self
                .base
                .cells(REDSHIFT, &owner)
                .create_primary(authority.pubkey(), u32::MAX)
                .await?;
            let fixture = CommitFixture {
                base: &self.base,
                er: &self.er,
                payer: &payer,
                authority,
                account,
                sequence: &sequence,
            };
            let mut nonce = 0;
            for marker in &markers[..2] {
                nonce += 1;
                fixture.commit(nonce, *marker, buffered).await?;
            }
            for (index, case) in cases.iter().enumerate() {
                let attempted = match case {
                    0 => 0,
                    1 => nonce - 1,
                    2 => nonce,
                    3 => nonce + 2,
                    _ => nonce + 1,
                };
                let data = fixture.marked_data(markers[index + 2]).await?;
                let buffer =
                    if buffered { Some(fixture.buffer(attempted, &data).await?) } else { None };
                let signer = if *case == 4 { foreign } else { authority };
                let mut instruction =
                    fixture.instruction(signer.pubkey(), attempted, data, buffer).await?;
                if *case == 5 {
                    instruction.accounts[0].is_signer = false;
                }
                let expected = match case {
                    4 => InstructionError::Custom(DlpError::InvalidAuthority as u32),
                    5 => InstructionError::MissingRequiredSignature,
                    _ => InstructionError::Custom(DlpError::NonceOutOfOrder as u32),
                };
                let signers = if *case == 5 { vec![] } else { vec![signer] };
                tracing::debug!(case, buffered, attempted, nonce, "commit refusal");
                fixture.rejected(&[instruction], &signers, expected).await?;
                if buffered {
                    fixture.close_buffer(attempted).await?;
                }
                nonce += 1;
                fixture.commit(nonce, markers[index + 8], buffered).await?;
            }
        }
        Ok(())
    }
}

impl CommitFixture<'_> {
    /// Makes otherwise identical base transactions distinct without changing the commit nonce.
    fn tag(&self) -> Instruction {
        let nonce = self.sequence.get() + 1;
        self.sequence.set(nonce);
        REDSHIFT.batch().nonce(nonce).build()
    }

    async fn marked_data(&self, marker: u64) -> Result<Vec<u8>> {
        let mut data =
            self.base.account(&self.account).await?.context("commit fixture missing")?.data;
        let mut header = CellHeader::try_decode(&data)?;
        header.value = marker;
        header.revision += 1;
        header.write(&mut data)?;
        Ok(data)
    }

    /// Builds equivalent inline or buffered updates while preserving the account's lamports.
    async fn instruction(
        &self,
        authority: Pubkey,
        nonce: u64,
        data: Vec<u8>,
        buffer: Option<Pubkey>,
    ) -> Result<Instruction> {
        let lamports = self
            .base
            .account(&self.account)
            .await?
            .context("commit fixture missing")?
            .lamports;
        Ok(match buffer {
            Some(buffer) => instruction_builder::commit_state_from_buffer(
                authority,
                self.account,
                REDSHIFT.id(),
                buffer,
                CommitStateFromBufferArgs {
                    nonce,
                    lamports,
                    allow_undelegation: false,
                },
            ),
            None => dlp::commit_state(
                &authority,
                &self.account,
                &REDSHIFT.id(),
                dlp::CommitStateArgs {
                    nonce,
                    lamports,
                    allow_undelegation: false,
                    data,
                },
            ),
        })
    }

    /// Initializes and fills a nonce-scoped buffer, checking the assembled payload before use.
    async fn buffer(&self, nonce: u64, data: &[u8]) -> Result<Pubkey> {
        let (_, buffer) = self.initialize_buffer(nonce, data.len()).await?;
        for (index, chunk) in data.chunks(CHUNK).enumerate() {
            self.base
                .submit_and_confirm_with(
                    self.payer,
                    &[self.authority],
                    &[
                        create_write_ix(CreateWriteIxArgs {
                            authority: self.authority.pubkey(),
                            pubkey: self.account,
                            offset: (index * CHUNK) as u32,
                            data_chunk: chunk.to_vec(),
                            commit_id: nonce,
                        }),
                        self.tag(),
                    ],
                )
                .await?;
        }
        ensure!(self.base.account(&buffer).await?.context("buffer missing")?.data == data);
        Ok(buffer)
    }

    /// Creates the chunk bitmap and payload account for a specific commit successor.
    async fn initialize_buffer(&self, nonce: u64, size: usize) -> Result<(Pubkey, Pubkey)> {
        let count = size.div_ceil(CHUNK);
        let (init, chunks, buffer) = create_init_ix(CreateInitIxArgs {
            authority: self.authority.pubkey(),
            pubkey: self.account,
            chunks_account_size: Chunks::struct_size(count) as u64,
            buffer_account_size: size as u64,
            commit_id: nonce,
            chunk_count: count,
            chunk_size: CHUNK as u16,
        });
        let signature = self
            .base
            .submit_and_confirm_with(self.payer, &[self.authority], &[init, self.tag()])
            .await?;
        let initialized = self.base.api().await_transaction(&signature).await?;
        let images = self.base.accounts(&[chunks, buffer]).await?;
        ensure!(
            images.iter().all(Option::is_some),
            "buffer init {signature}: accounts {chunks}, {buffer}; images {images:?}; logs {:?}",
            transaction_logs(transaction_meta(&initialized)?)
        );
        Ok((chunks, buffer))
    }

    async fn close_buffer(&self, nonce: u64) -> Result<()> {
        self.base
            .submit_and_confirm_with(
                self.payer,
                &[self.authority],
                &[
                    create_close_ix(CreateCloseIxArgs {
                        authority: self.authority.pubkey(),
                        pubkey: self.account,
                        commit_id: nonce,
                    }),
                    self.tag(),
                ],
            )
            .await?;
        Ok(())
    }

    /// Accepts a successor and checks both its marked data and persisted last-commit ID.
    async fn commit(&self, nonce: u64, marker: u64, buffered: bool) -> Result<()> {
        let data = self.marked_data(marker).await?;
        let buffer = if buffered { Some(self.buffer(nonce, &data).await?) } else { None };
        let instructions = [
            self.instruction(self.authority.pubkey(), nonce, data.clone(), buffer).await?,
            dlp::finalize(&self.authority.pubkey(), &self.account),
        ];
        self.base
            .submit_and_confirm_with(self.payer, &[self.authority], &instructions)
            .await?;
        ensure!(
            self.base
                .account(&self.account)
                .await?
                .context("committed fixture missing")?
                .data
                == data
        );
        let metadata = self
            .base
            .account(&dlp::delegation_metadata_pda(&self.account))
            .await?
            .context("metadata missing")?;
        ensure!(dlp::last_commit_id(&metadata.data)? == nonce);
        if buffered {
            self.close_buffer(nonce).await?;
        }
        Ok(())
    }

    async fn rejected(
        &self,
        instructions: &[Instruction],
        signers: &[&Keypair],
        error: InstructionError,
    ) -> Result<()> {
        self.rejected_at(instructions, signers, 0, error).await
    }

    /// Requires the exact failing instruction and rollback of all non-payer inputs on both chains.
    async fn rejected_at(
        &self,
        instructions: &[Instruction],
        signers: &[&Keypair],
        index: u8,
        error: InstructionError,
    ) -> Result<()> {
        let mut keys = Vec::new();
        for instruction in instructions {
            for meta in &instruction.accounts {
                if meta.pubkey != self.payer.pubkey() && !keys.contains(&meta.pubkey) {
                    keys.push(meta.pubkey);
                }
            }
        }
        let before = self.base.accounts(&keys).await?;
        let local = self.er.local_images(&keys).await?;
        let mut signing = vec![self.payer];
        signing.extend_from_slice(signers);
        let tx = Transaction::new_signed_with_payer(
            instructions,
            Some(&self.payer.pubkey()),
            &signing,
            self.base.api().get_latest_blockhash().await?,
        );
        let signature = self.base.api().send_transaction(&tx).await?;
        let tx = self.base.api().await_transaction(&signature).await?;
        ensure!(
            transaction_meta(&tx)?.err
                == Some(TransactionError::InstructionError(index, error.clone()).into()),
            "expected instruction {index}: {error:?}; observed {:?}; logs {:?}",
            transaction_meta(&tx)?.err,
            transaction_logs(transaction_meta(&tx)?)
        );
        ensure!(self.base.accounts(&keys).await? == before);
        ensure!(self.er.local_images(&keys).await? == local);
        Ok(())
    }
}
