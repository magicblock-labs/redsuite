set positional-arguments := true

validator := justfile_directory() / ".." / "magicblock-validator"
dlp := justfile_directory() / ".." / "delegation-program"
export MAGICBLOCK_VALIDATOR_BIN := validator / "target/debug/magicblock-validator"
export MAGICBLOCK_VERIFIER_BIN := validator / "target/debug/magicblock-verifier"
export REDSUITE_FIXTURES_DIR := justfile_directory() / "target/deploy"
export REDSUITE_DLP_SO := dlp / "target/deploy/delegation_program.so"
export REDSUITE_ROOT := env("REDSUITE_ROOT", justfile_directory())

unexport REDSUITE_ACCOUNTSDB_DIR

[private]
default:
    @just --list --unsorted
    @echo 'After building, start, test, and discard the DLP ledger with `just base-start dlp-lifecycle base-stop`.'
    @just status

# Show both bases, their RPC addresses and PIDs, and any leftover rollups.
status:
    @just _suite dlp stack status
    @just _suite shared stack status

# Build the local validator, verifier, committor, DLP, and fixtures.
dlp-build:
    cd {{ quote(validator) }} && cargo build --locked --target-dir target --bin magicblock-validator --bin magicblock-verifier
    ./scripts/with-runtime cargo xtask programs
    ./scripts/with-runtime cargo build-sbf --manifest-path {{ quote(validator / "programs/committor-program/Cargo.toml") }} --sbf-out-dir {{ quote(validator / "target/deploy") }} -- --locked
    ./scripts/with-runtime cargo build-sbf --manifest-path {{ quote(dlp / "program/Cargo.toml") }} --sbf-out-dir {{ quote(dlp / "target/deploy") }} -- --locked

# Start or reuse the DLP test base and print its RPC address.
base-start:
    @just _suite dlp stack up

# Restart the DLP test base with a fresh ledger and the current program builds.
base-restart:
    @just _suite dlp stack restart

# Stop the DLP test base, delete its test ledger, and keep the logs.
base-stop:
    @just _suite dlp stack down

alias dlp-down := base-stop

# Build the programs and discard the DLP base. The next base-start loads them.
dlp-build-and-reset: dlp-build base-stop

# Run delegate, commit, and undelegate on the running DLP base.
dlp-lifecycle:
    @just _test dlp_v2_lifecycle

# Test buffered commits and undelegations with 8 KiB accounts on the DLP base.
dlp-buffered:
    @just _test dlp_v2_buffered_settlement

# Commit and undelegate legacy and v2 accounts together on the DLP base.
dlp-mixed:
    @just _test dlp_mixed_settlement

# Run all three scenarios on the DLP base. Each run starts and stops its own rollup.
dlp-all:
    @just _test dlp_

# Start or reuse the separate shared RedSuite base and print its RPC address.
shared-base-start:
    @just _suite shared stack up

# Restart the shared RedSuite base with a fresh test ledger.
shared-base-restart:
    @just _suite shared stack restart

# Stop the shared RedSuite base, delete its test ledger, and keep the logs.
shared-base-stop:
    @just _suite shared stack down

# Stop both bases and delete both test ledgers.
stop-all: base-stop shared-base-stop

[private]
_test selection:
    @echo 'Tests use the DLP base. Start it with `just base-start`.'
    @just _suite dlp redshift run --require-running-base --serial -- "$1"

[private]
_suite base +args:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$1" in
        dlp)    export REDSUITE_STACK_DIR="$REDSUITE_ROOT/target/dlp-v2-stack" ;;
        shared) export REDSUITE_STACK_DIR="$REDSUITE_ROOT/target/redsuite-stack" ;;
        *) printf 'Unknown base "%s". Choose dlp or shared.\n' "$1" >&2; exit 2 ;;
    esac
    printf '\n[%s base]\n' "$1"
    shift
    cargo build --quiet --locked --target-dir target -p redsuite
    if [[ $1 == stack && ( $2 == status || $2 == down ) ]]; then
        exec ./target/debug/redsuite "$@"
    fi
    if [[ ! -x $MAGICBLOCK_VALIDATOR_BIN || ! -f $REDSUITE_DLP_SO ||
          ! -f $REDSUITE_FIXTURES_DIR/redshift_program.so ||
          ! -f {{ quote(validator / "target/deploy/magicblock_committor_program.so") }} ]]; then
        echo 'Build the local programs first with `just dlp-build`.' >&2
        exit 1
    fi
    exec ./scripts/with-runtime ./target/debug/redsuite "$@"
