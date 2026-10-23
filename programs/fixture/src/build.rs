use instruction::{AccountMeta, Instruction as SolanaInstruction};
use pubkey::Pubkey;
use sdk::{
    consts::{MAGIC_CONTEXT_ID, MAGIC_PROGRAM_ID},
    delegate_args::{DelegateAccountMetas, DelegateAccounts},
};
use sdk_ids::system_program;

use crate::{
    Action, ActionGuard, CellId, IndexedMeta, Instruction, Intent, Operation, ResultCallback,
    Settlement, SettlementMode, UndelegationOutcome,
};

#[derive(Clone, Copy)]
pub struct Program {
    id: Pubkey,
}

impl Program {
    pub const fn at(id: Pubkey) -> Self {
        Self { id }
    }
    pub const fn id(self) -> Pubkey {
        self.id
    }
    pub fn pda(self, id: CellId) -> (Pubkey, u8) {
        id.pda(&self.id)
    }
    pub fn primary(self, authority: Pubkey) -> Pubkey {
        self.pda(CellId::new(authority, 0)).0
    }
    pub fn primaries(self, authorities: &[Pubkey]) -> Vec<Pubkey> {
        authorities.iter().map(|&authority| self.primary(authority)).collect()
    }

    fn encode(self, instruction: Instruction, accounts: Vec<AccountMeta>) -> SolanaInstruction {
        SolanaInstruction {
            program_id: self.id,
            accounts,
            data: instruction.into_data(),
        }
    }

    pub fn create(self, payer: Pubkey, id: CellId, space: u32) -> (SolanaInstruction, Pubkey) {
        let (pda, _) = self.pda(id);
        (
            self.encode(
                Instruction::Create { id, space },
                vec![
                    AccountMeta::new(payer, true),
                    AccountMeta::new_readonly(id.authority, true),
                    AccountMeta::new(pda, false),
                    AccountMeta::new_readonly(system_program::ID, false),
                ],
            ),
            pda,
        )
    }

    pub fn delegate(
        self,
        payer: Pubkey,
        id: CellId,
        validator: Option<Pubkey>,
        commit_frequency_ms: u32,
    ) -> SolanaInstruction {
        let metas = DelegateAccountMetas::from(DelegateAccounts::new(self.pda(id).0, self.id));
        self.encode(
            Instruction::Delegate {
                id,
                validator,
                commit_frequency_ms,
            },
            vec![
                AccountMeta::new(payer, true),
                metas.delegated_account,
                metas.owner_program,
                metas.delegate_buffer,
                metas.delegation_record,
                metas.delegation_metadata,
                metas.delegation_program,
                metas.system_program,
                AccountMeta::new_readonly(id.authority, true),
            ],
        )
    }

    pub fn resize(self, payer: Pubkey, cell: Pubkey, space: u32) -> SolanaInstruction {
        self.encode(
            Instruction::Resize { space },
            vec![
                AccountMeta::new(payer, true),
                AccountMeta::new(cell, false),
                AccountMeta::new_readonly(system_program::ID, false),
            ],
        )
    }

    pub fn request_undelegation(self, payer: Pubkey, id: CellId) -> SolanaInstruction {
        let native =
            sdk::dlp_api::instruction_builder::request_undelegation(payer, self.pda(id).0, self.id);
        self.batch().invoke_signed(native, &[id]).build()
    }

    pub fn batch(self) -> Batch {
        Batch {
            program: self,
            accounts: Vec::new(),
            operations: Vec::new(),
            nonce: 0,
        }
    }

    pub fn write(self, value: u64, cells: &[Pubkey]) -> SolanaInstruction {
        self.batch().write(cells, value).build()
    }
    pub fn hash(
        self,
        value: u64,
        initial: [u8; 32],
        rounds: u32,
        cells: &[Pubkey],
    ) -> SolanaInstruction {
        self.batch().set(cells, value).hash(cells, initial, rounds).build()
    }
    pub fn fold_hashes(self, value: u64, rounds: u32, cells: &[Pubkey]) -> SolanaInstruction {
        self.batch().set(cells, value).fold_hashes(cells, value, rounds).build()
    }
    pub fn copy(
        self,
        value: u64,
        sources: &[Pubkey],
        destinations: &[Pubkey],
    ) -> SolanaInstruction {
        self.batch().set(destinations, value).copy(sources, destinations).build()
    }
    pub fn copy_three(self, pool: &[Pubkey], value: u64) -> (SolanaInstruction, [Pubkey; 2]) {
        let len = pool.len() as u64;
        let first = value.wrapping_sub(1).wrapping_mul(3) % len;
        let source = pool[first as usize];
        let destinations = [pool[((first + 1) % len) as usize], pool[((first + 2) % len) as usize]];
        (self.copy(value, &[source], &destinations), destinations)
    }
    pub fn sum_sizes(self, value: u64, output: Pubkey, inputs: &[Pubkey]) -> SolanaInstruction {
        self.batch().set(&[output], value).sum_sizes(output, inputs).build()
    }
    pub fn inspect(self, accounts: &[Pubkey]) -> SolanaInstruction {
        self.batch().inspect(accounts).build()
    }
    pub fn log(self, message: &str) -> SolanaInstruction {
        self.batch().log(message).build()
    }
    pub fn settle(
        self,
        payer: Pubkey,
        cells: &[Pubkey],
        mode: SettlementMode,
    ) -> SolanaInstruction {
        self.batch().settle(payer, cells, mode).build()
    }
}

/// A composable invocation. Account privileges are merged only for the outer call;
/// nested invocations retain the privileges explicitly requested by their caller.
pub struct Batch {
    program: Program,
    accounts: Vec<AccountMeta>,
    operations: Vec<Operation>,
    nonce: u64,
}

pub type Commit = Intent<Pubkey>;

impl Commit {
    pub fn new(mode: SettlementMode, cells: &[Pubkey]) -> Self {
        Self {
            mode,
            cells: cells.to_vec(),
            after_commit: Vec::new(),
            after_undelegate: Vec::new(),
        }
    }
}

pub struct IntentBundle {
    pub payer: Pubkey,
    pub validator: Option<Pubkey>,
    pub commits: Vec<Commit>,
    pub actions: Vec<Action>,
}

impl IntentBundle {
    pub fn new(payer: Pubkey) -> Self {
        Self {
            payer,
            validator: None,
            commits: Vec::new(),
            actions: Vec::new(),
        }
    }
}

impl Action {
    pub fn new(authority: Pubkey, instruction: SolanaInstruction, compute_units: u32) -> Self {
        Self {
            authority,
            instruction,
            compute_units,
            escrow_index: 1,
            callback: None,
        }
    }
    pub fn then(mut self, callback: ResultCallback) -> Self {
        self.callback = Some(callback);
        self
    }
}

impl Batch {
    /// Distinguishes otherwise identical transactions without changing state or logs.
    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = nonce;
        self
    }
    fn account(&mut self, meta: AccountMeta) -> u16 {
        if let Some(index) = self.accounts.iter().position(|entry| entry.pubkey == meta.pubkey) {
            self.accounts[index].is_signer |= meta.is_signer;
            self.accounts[index].is_writable |= meta.is_writable;
            index as u16
        } else {
            // Solana transactions have at most 256 account keys.
            let index =
                u16::try_from(self.accounts.len()).expect("fixture exceeds Solana account limit");
            self.accounts.push(meta);
            index
        }
    }

    fn cells(&mut self, keys: &[Pubkey], writable: bool) -> Vec<u16> {
        keys.iter()
            .map(|&pubkey| {
                self.account(AccountMeta {
                    pubkey,
                    is_signer: false,
                    is_writable: writable,
                })
            })
            .collect()
    }

    pub fn set(mut self, cells: &[Pubkey], value: u64) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::Set { cells, value });
        self
    }
    pub fn add(mut self, cells: &[Pubkey], change: i64) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::Add { cells, change });
        self
    }
    pub fn write(self, cells: &[Pubkey], value: u64) -> Self {
        self.set(cells, value).fill(cells, value, 0)
    }
    pub fn fill(mut self, cells: &[Pubkey], seed: u64, step: u64) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::Fill { cells, seed, step });
        self
    }
    pub fn hash(mut self, cells: &[Pubkey], initial: [u8; 32], rounds: u32) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::Hash { cells, initial, rounds });
        self
    }
    pub fn fold_hashes(mut self, cells: &[Pubkey], salt: u64, rounds: u32) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::FoldHashes { cells, salt, rounds });
        self
    }
    pub fn copy(mut self, sources: &[Pubkey], destinations: &[Pubkey]) -> Self {
        let sources = self.cells(sources, false);
        let destinations = self.cells(destinations, true);
        self.operations.push(Operation::Copy { sources, destinations });
        self
    }
    pub fn sum_sizes(mut self, output: Pubkey, inputs: &[Pubkey]) -> Self {
        let output = self.account(AccountMeta::new(output, false));
        let inputs = self.cells(inputs, false);
        self.operations.push(Operation::SumSizes { output, inputs });
        self
    }
    pub fn inspect(self, accounts: &[Pubkey]) -> Self {
        self.observations(accounts, false)
    }
    pub fn observe(self, accounts: &[Pubkey]) -> Self {
        self.observations(accounts, true)
    }
    fn observations(mut self, accounts: &[Pubkey], hash: bool) -> Self {
        let accounts = self.cells(accounts, false);
        self.operations.push(Operation::Inspect { accounts, hash });
        self
    }
    pub fn fill_bytes(mut self, key: Pubkey, byte: u8) -> Self {
        let account = self.account(AccountMeta::new(key, false));
        self.operations.push(Operation::FillBytes { account, byte });
        self
    }
    pub fn undelegation(mut self, cells: &[Pubkey], outcome: UndelegationOutcome) -> Self {
        let cells = self.cells(cells, true);
        self.operations.push(Operation::Undelegation { cells, outcome });
        self
    }
    pub fn log(mut self, message: &str) -> Self {
        self.operations.push(Operation::Log(message.to_owned()));
        self
    }
    pub fn fail(mut self, code: u32) -> Self {
        self.operations.push(Operation::Fail(code));
        self
    }
    pub fn require_signer(mut self, key: Pubkey) -> Self {
        self.account(AccountMeta::new_readonly(key, true));
        self
    }
    pub fn readonly(mut self, keys: &[Pubkey]) -> Self {
        for account in &mut self.accounts {
            if keys.contains(&account.pubkey) {
                account.is_writable = false;
            }
        }
        self
    }
    pub fn writable(mut self, keys: &[Pubkey]) -> Self {
        self.cells(keys, true);
        self
    }

    pub fn invoke(self, instruction: SolanaInstruction) -> Self {
        self.invoke_signed(instruction, &[])
    }
    pub fn invoke_signed(mut self, instruction: SolanaInstruction, signers: &[CellId]) -> Self {
        let signed: Vec<_> = signers.iter().map(|id| self.program.pda(*id).0).collect();
        let accounts = instruction
            .accounts
            .into_iter()
            .map(|meta| {
                let signer = meta.is_signer;
                let writable = meta.is_writable;
                let index = self.account(AccountMeta {
                    is_signer: signer && !signed.contains(&meta.pubkey),
                    ..meta
                });
                IndexedMeta { index, signer, writable }
            })
            .collect();
        let program = self.account(AccountMeta::new_readonly(instruction.program_id, false));
        self.operations.push(Operation::Invoke {
            program,
            accounts,
            data: instruction.data,
            signers: signers.to_vec(),
        });
        self
    }

    pub fn transfer_from_escrow(mut self, destination: Pubkey, amount: u64) -> Self {
        let destination = self.account(AccountMeta::new(destination, false));
        let system = self.account(AccountMeta::new_readonly(system_program::ID, false));
        self.operations
            .push(Operation::TransferFromEscrow { destination, system, amount });
        self
    }

    pub fn settle(self, payer: Pubkey, cells: &[Pubkey], mode: SettlementMode) -> Self {
        let mut bundle = IntentBundle::new(payer);
        bundle.commits.push(Commit::new(mode, cells));
        self.intents(bundle)
    }

    pub fn intents(mut self, bundle: IntentBundle) -> Self {
        let payer = self.account(AccountMeta::new(bundle.payer, true));
        let context = self.account(AccountMeta::new(MAGIC_CONTEXT_ID, false));
        let magic = self.account(AccountMeta::new_readonly(MAGIC_PROGRAM_ID, false));
        let vault = bundle.validator.map(|validator| {
            self.account(AccountMeta::new(
                sdk::pda::magic_fee_vault_pda_from_validator(&validator),
                false,
            ))
        });
        let mut intents = Vec::new();
        for commit in bundle.commits {
            let cells = self.cells(&commit.cells, true);
            for action in commit.after_commit.iter().chain(&commit.after_undelegate) {
                self.account(AccountMeta::new_readonly(action.authority, true));
            }
            intents.push(Intent {
                mode: commit.mode,
                cells,
                after_commit: commit.after_commit,
                after_undelegate: commit.after_undelegate,
            });
        }
        for action in &bundle.actions {
            self.account(AccountMeta::new_readonly(action.authority, true));
        }
        self.operations.push(Operation::Settle(Settlement {
            payer,
            context,
            magic,
            vault,
            intents,
            actions: bundle.actions,
        }));
        self
    }

    pub fn build(self) -> SolanaInstruction {
        self.program.encode(
            Instruction::Execute {
                nonce: self.nonce,
                operations: self.operations,
            },
            self.accounts,
        )
    }
    pub fn action(mut self, cell: Pubkey, owner: Pubkey) -> SolanaInstruction {
        self.account(AccountMeta::new_readonly(cell, false));
        self.program.encode(
            Instruction::Action {
                nonce: self.nonce,
                guard: ActionGuard { cell, owner },
                operations: self.operations,
            },
            self.accounts,
        )
    }
}
