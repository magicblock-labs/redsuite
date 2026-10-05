use borsh::BorshDeserialize;
use pubkey::Pubkey;
use sdk::{
    cpi::{undelegate_account, DelegateAccounts, DelegateConfig},
    utils::create_pda,
};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    msg,
    program_error::ProgramError,
};
use solana_sdk_ids::system_program;

use crate::layout::{
    DATA_OFFSET, HASH_OFFSET, HASH_SIZE, ID_OFFSET, OWNER_PUBKEY_SIZE,
};

fn prepare_buffer(index: &mut usize, target: &mut [u8], data: &[u8]) {
    target[*index..*index + data.len()].copy_from_slice(data);
    *index += data.len();
}

fn require_space(len: usize, needed: usize) -> Result<(), ProgramError> {
    if len < needed {
        msg!("Error: account data {} is smaller than {}", len, needed);
        return Err(ProgramError::AccountDataTooSmall);
    }
    Ok(())
}

fn verify_account_owner(
    signer: &AccountInfo,
    account: &AccountInfo,
) -> Result<Pubkey, ProgramError> {
    if !signer.is_signer {
        msg!("Error: Account owner must be a signer");
        return Err(ProgramError::MissingRequiredSignature);
    }

    let data = account.try_borrow_data()?;
    if data.len() < OWNER_PUBKEY_SIZE {
        msg!("Error: Account data too small to contain owner pubkey");
        return Err(ProgramError::InvalidAccountData);
    }

    let stored_owner = Pubkey::try_from(&data[..OWNER_PUBKEY_SIZE])
        .map_err(|_| ProgramError::InvalidAccountData)?;

    if stored_owner != *signer.key {
        msg!(
            "Error: Signer {} does not match stored owner {}",
            signer.key,
            stored_owner
        );
        return Err(ProgramError::InvalidAccountData);
    }

    Ok(stored_owner)
}

pub fn init_account(
    program_id: &Pubkey,
    iter: &mut std::slice::Iter<AccountInfo>,
    space: u32,
    seed: u8,
    bump: u8,
    authority: Pubkey,
) -> ProgramResult {
    let payer = next_account_info(iter)?;
    let pda = next_account_info(iter)?;
    let base = next_account_info(iter)?;
    require_space(space as usize, OWNER_PUBKEY_SIZE)?;
    let mut seeds = space.to_le_bytes().to_vec();
    seeds.push(seed);
    seeds.extend_from_slice(&authority.as_ref()[..16]);
    let seeds = [base.key.as_ref(), &seeds, &[bump]];

    create_pda(
        pda,
        program_id,
        space as usize,
        &[&seeds],
        next_account_info(iter)?,
        payer,
        true,
    )?;

    let mut data = pda.try_borrow_mut_data()?;
    data[..OWNER_PUBKEY_SIZE].copy_from_slice(payer.key.as_ref());

    msg!("initialized PDA: {} with owner: {}", pda.key, payer.key);
    Ok(())
}

pub fn delegate_account(
    accs: &[AccountInfo],
    seed: u8,
    authority: Pubkey,
) -> ProgramResult {
    let [payer, pda, owner_program, buffer, delegation_record, delegation_metadata, delegation_program, system_program, base] =
        accs
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };
    let accounts = DelegateAccounts {
        payer,
        pda,
        owner_program,
        buffer,
        delegation_record,
        delegation_metadata,
        delegation_program,
        system_program,
    };

    verify_account_owner(payer, accounts.pda)?;
    let mut seeds = (accounts.pda.data_len() as u32).to_le_bytes().to_vec();
    seeds.push(seed);
    seeds.extend_from_slice(&authority.as_ref()[..16]);
    let seeds = [base.key.as_ref(), &seeds];
    let pda = *accounts.pda.key;
    let config = DelegateConfig {
        commit_frequency_ms: u32::MAX,
        validator: Some(authority),
    };
    sdk::cpi::delegate_account(accounts, &seeds, config)?;
    msg!("delegated PDA: {} to {}", pda, authority);
    Ok(())
}

pub fn simple_byte_set(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
) -> ProgramResult {
    let mut count = 0;
    let mut total_bytes = 0;

    while let Ok(pda) = next_account_info(iter) {
        if pda.lamports() == 0 {
            continue; // Skip uninitialized accounts
        }
        let mut data = pda.try_borrow_mut_data()?;
        let buffer = id.to_le_bytes();
        let mut index = DATA_OFFSET;

        while index + buffer.len() <= data.len() {
            prepare_buffer(&mut index, &mut data, &buffer);
        }

        total_bytes += index - DATA_OFFSET;
        count += 1;
    }

    msg!(
        "filled {} accounts with id {}, using {} total bytes",
        count,
        id,
        total_bytes
    );
    Ok(())
}

pub fn multi_account_read(
    iter: &mut std::slice::Iter<AccountInfo>,
    accounts: &[AccountInfo],
    id: u64,
) -> ProgramResult {
    let pda = next_account_info(iter)?;
    if pda.lamports() == 0 {
        return Err(ProgramError::UninitializedAccount);
    }
    let mut data = pda.try_borrow_mut_data()?;
    require_space(data.len(), DATA_OFFSET + 16)?;
    let sum = iter.clone().map(|a| a.data_len() as u64).sum::<u64>();
    let buffer = id.to_le_bytes();
    let buffer_sum = sum.to_le_bytes();
    let mut index = DATA_OFFSET;

    prepare_buffer(&mut index, &mut data, &buffer);
    prepare_buffer(&mut index, &mut data, &buffer_sum);

    msg!(
        "computed sum of {} accounts' data: {}, txn: {}",
        accounts.len() - 1,
        sum,
        id
    );
    Ok(())
}

pub fn expensive_hash_compute(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
    hash: [u8; 32],
    iters: u32,
) -> ProgramResult {
    msg!("Starting compute-intensive operation...");

    let hash = crate::utils::hash_chain(hash, iters);

    let mut count = 0;
    while let Ok(pda) = next_account_info(iter) {
        if pda.lamports() == 0 {
            continue; // Skip uninitialized accounts
        }
        let mut data = pda.try_borrow_mut_data()?;
        require_space(data.len(), DATA_OFFSET + 8 + hash.len())?;
        let buffer = id.to_le_bytes();
        let mut index = DATA_OFFSET;

        prepare_buffer(&mut index, &mut data, &buffer);
        prepare_buffer(&mut index, &mut data, &hash);
        count += 1;
    }

    msg!(
        "computed SHA-256 hash {} times, wrote to {} accounts, txn: {}",
        iters,
        count,
        id
    );
    Ok(())
}

pub fn account_data_copy(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
) -> ProgramResult {
    let accounts: Vec<_> = iter.collect();
    if accounts.is_empty() {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let split = accounts.len() / 2;
    let split = if split == 0 { 1 } else { split };

    let sources = &accounts[..split];
    let destinations = &accounts[split..];

    let mut total_bytes = 0;
    for (dest_idx, dest) in destinations.iter().enumerate() {
        if dest.lamports() == 0 {
            continue;
        }

        let src = sources[dest_idx % sources.len()];
        if src.lamports() == 0 {
            continue;
        }

        let src_data = src.try_borrow_data()?;
        let mut dst_data = dest.try_borrow_mut_data()?;
        require_space(src_data.len(), DATA_OFFSET)?;
        require_space(dst_data.len(), DATA_OFFSET + 8)?;

        let buffer = id.to_le_bytes();
        let mut index = DATA_OFFSET;
        prepare_buffer(&mut index, &mut dst_data, &buffer);

        let copy_len =
            (src_data.len() - DATA_OFFSET).min(dst_data.len() - index);
        if copy_len > 0 {
            dst_data[index..index + copy_len].copy_from_slice(
                &src_data[DATA_OFFSET..DATA_OFFSET + copy_len],
            );
            total_bytes += copy_len;
        }
    }

    msg!(
        "copied data from {} sources to {} destinations, {} total bytes, txn: {}",
        sources.len(),
        destinations.len(),
        total_bytes,
        id
    );
    Ok(())
}

pub fn read_accounts_data(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
) -> ProgramResult {
    while let Ok(account) = next_account_info(iter) {
        msg!(
            "account {} has {} space, txn: {}",
            account.key,
            account.data_len(),
            id
        );
    }
    Ok(())
}

pub fn hash_fold(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
    iters: u32,
) -> ProgramResult {
    let accounts: Vec<_> = iter.collect();
    if accounts.is_empty() {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let mut hashes = Vec::with_capacity(accounts.len());
    for account in &accounts {
        if account.lamports() == 0 {
            return Err(ProgramError::UninitializedAccount);
        }
        let data = account.try_borrow_data()?;
        require_space(data.len(), HASH_OFFSET + HASH_SIZE)?;
        let mut hash = [0u8; HASH_SIZE];
        hash.copy_from_slice(&data[HASH_OFFSET..HASH_OFFSET + HASH_SIZE]);
        hashes.push(hash);
    }

    let digest = crate::utils::fold_hash(id, &hashes, iters);
    for account in &accounts {
        let mut data = account.try_borrow_mut_data()?;
        let mut index = ID_OFFSET;
        prepare_buffer(&mut index, &mut data, &id.to_le_bytes());
        prepare_buffer(&mut index, &mut data, &digest);
    }

    msg!(
        "folded {} accounts over {} rounds, txn: {}",
        accounts.len(),
        iters,
        id
    );
    Ok(())
}

pub fn commit_accounts(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
) -> ProgramResult {
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let accounts: Vec<_> = iter.collect();
    let count = accounts.len();
    sdk::ephem::commit_accounts(
        payer,
        accounts,
        magic_context,
        magic_program,
        None,
    )?;
    msg!("committed {} accounts to chain txn: {}", count, id);
    Ok(())
}

pub fn commit_undelegate_accounts(
    iter: &mut std::slice::Iter<AccountInfo>,
    id: u64,
) -> ProgramResult {
    let payer = next_account_info(iter)?;
    let magic_context = next_account_info(iter)?;
    let magic_program = next_account_info(iter)?;
    let accounts: Vec<_> = iter.collect();
    let count = accounts.len();
    sdk::ephem::commit_and_undelegate_accounts(
        payer,
        accounts,
        magic_context,
        magic_program,
        None,
    )?;
    msg!("commit-undelegated {} accounts to chain txn: {}", count, id);
    Ok(())
}

pub fn close_account(
    iter: &mut std::slice::Iter<AccountInfo>,
) -> ProgramResult {
    let owner = next_account_info(iter)?;
    let account_to_close = next_account_info(iter)?;

    verify_account_owner(owner, account_to_close)?;

    let lamports_to_transfer = account_to_close.lamports();
    **account_to_close.lamports.borrow_mut() = 0;
    **owner.lamports.borrow_mut() = owner
        .lamports()
        .checked_add(lamports_to_transfer)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    account_to_close.assign(&system_program::ID);
    account_to_close.resize(0)?;

    msg!(
        "closed account {} and refunded rent to {}",
        account_to_close.key,
        owner.key
    );
    Ok(())
}

pub fn undelegate(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let iter = &mut accounts.iter();
    let delegated_account = next_account_info(iter)?;
    let buffer = next_account_info(iter)?;
    let payer = next_account_info(iter)?;
    let system_program = next_account_info(iter)?;

    let account_seeds =
        <Vec<Vec<u8>>>::try_from_slice(data).map_err(|err| {
            msg!("ERROR: failed to parse account seeds {:?}", err);
            ProgramError::InvalidArgument
        })?;

    undelegate_account(
        delegated_account,
        program_id,
        buffer,
        payer,
        system_program,
        account_seeds,
    )
}
