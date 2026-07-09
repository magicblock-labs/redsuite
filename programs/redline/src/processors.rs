use borsh::BorshDeserialize;
use pubkey::Pubkey;
use sdk::{
    cpi::{undelegate_account, DelegateAccounts, DelegateConfig},
    utils::create_pda,
};
use sha2::{Digest, Sha256};
use solana_program::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    msg,
    program_error::ProgramError,
};
use solana_sdk_ids::system_program;

use crate::layout::{DATA_OFFSET, OWNER_PUBKEY_SIZE};

fn prepare_buffer(index: &mut usize, target: &mut [u8], data: &[u8]) {
    target[*index..*index + data.len()].copy_from_slice(data);
    *index += data.len();
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

    if stored_owner != *signer.key && stored_owner != Pubkey::default() {
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
    iter: &mut std::slice::Iter<AccountInfo>,
    space: u32,
    seed: u8,
    bump: u8,
    authority: Pubkey,
) -> ProgramResult {
    let payer = next_account_info(iter)?;
    let pda = next_account_info(iter)?;
    let base = next_account_info(iter)?;
    let mut seeds = space.to_le_bytes().to_vec();
    seeds.push(seed);
    seeds.extend_from_slice(&authority.as_ref()[..16]);
    let seeds = [base.key.as_ref(), &seeds, &[bump]];

    create_pda(
        pda,
        &crate::ID,
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
    mut hash: [u8; 32],
    iters: u32,
) -> ProgramResult {
    msg!("Starting compute-intensive operation...");

    for i in 0..iters {
        let mut hasher = Sha256::new();
        hasher.update(hash);
        hasher.update(i.to_le_bytes());
        hash.copy_from_slice(&hasher.finalize());
    }

    let mut count = 0;
    while let Ok(pda) = next_account_info(iter) {
        if pda.lamports() == 0 {
            continue; // Skip uninitialized accounts
        }
        let mut data = pda.try_borrow_mut_data()?;
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

pub fn undelegate(accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
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
        &crate::ID,
        buffer,
        payer,
        system_program,
        account_seeds,
    )
}

#[cfg(test)]
mod tests {
    use solana_program::pubkey::Pubkey as SolPubkey;

    use super::*;
    use crate::layout::{ID_OFFSET, ID_SIZE};

    fn account_info<'a>(
        key: &'a SolPubkey,
        lamports: &'a mut u64,
        data: &'a mut [u8],
        owner: &'a SolPubkey,
        signer: bool,
    ) -> AccountInfo<'a> {
        AccountInfo::new(key, signer, true, lamports, data, owner, false)
    }

    #[test]
    fn simple_byte_set_writes_id_at_layout_offset() {
        let key = SolPubkey::new_unique();
        let program = crate::ID;
        let stored_owner = SolPubkey::new_unique();
        let mut lamports = 1_000_000u64;
        let mut data = vec![0u8; OWNER_PUBKEY_SIZE + 3 * ID_SIZE + 5];
        data[..OWNER_PUBKEY_SIZE].copy_from_slice(stored_owner.as_ref());

        let accounts = vec![account_info(
            &key,
            &mut lamports,
            &mut data,
            &program,
            false,
        )];
        let id: u64 = 0xDEAD_BEEF_CAFE_F00D;
        simple_byte_set(&mut accounts.iter(), id).unwrap();
        drop(accounts);

        assert_eq!(&data[..OWNER_PUBKEY_SIZE], stored_owner.as_ref());
        for i in 0..3 {
            let at = ID_OFFSET + i * ID_SIZE;
            assert_eq!(data[at..at + ID_SIZE], id.to_le_bytes());
        }
        // The sub-8-byte tail stays untouched.
        assert_eq!(&data[OWNER_PUBKEY_SIZE + 3 * ID_SIZE..], &[0u8; 5]);
    }

    #[test]
    fn owner_verification_gates_by_stored_header() {
        let program = crate::ID;
        let owner_key = SolPubkey::new_unique();
        let intruder_key = SolPubkey::new_unique();

        let mut owner_lamports = 1u64;
        let mut owner_data = [0u8; 0];
        let mut intruder_lamports = 1u64;
        let mut intruder_data = [0u8; 0];
        let mut pda_lamports = 1u64;
        let mut pda_data = vec![0u8; 64];
        pda_data[..OWNER_PUBKEY_SIZE].copy_from_slice(owner_key.as_ref());
        let pda_key = SolPubkey::new_unique();

        let pda = account_info(
            &pda_key,
            &mut pda_lamports,
            &mut pda_data,
            &program,
            false,
        );

        let owner = account_info(
            &owner_key,
            &mut owner_lamports,
            &mut owner_data,
            &program,
            true,
        );
        assert_eq!(verify_account_owner(&owner, &pda).unwrap(), owner_key);

        let intruder = account_info(
            &intruder_key,
            &mut intruder_lamports,
            &mut intruder_data,
            &program,
            true,
        );
        assert!(verify_account_owner(&intruder, &pda).is_err());

        let unsigned_owner = account_info(
            &owner_key,
            &mut owner_lamports,
            &mut owner_data,
            &program,
            false,
        );
        assert!(verify_account_owner(&unsigned_owner, &pda).is_err());
    }
}
