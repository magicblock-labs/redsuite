# Native bundle assembly

The xtask commands prepare archives for Apple Silicon macOS and x86-64 GNU/Linux.
They do not create tags or publish releases.

Pushing a version tag (`N.N.N`) runs the [bundle workflow](../.github/workflows/bundles.yml).
It builds both native runtimes and runners, builds shared fixtures once, and
inspects the extracted bundles. After both platforms pass, it attaches the runtime,
fixture, and full bundle archives with their checksum sidecars to the tag's GitHub
Release, creating a draft if needed. Reruns replace assets with matching names.
Manual workflow runs only upload Actions artifacts; they do not change releases.

`shared/components.toml` defines the single Agave/Yellowstone pair used by this
RedSuite version, including its source revisions. Cache identities use pinned component revisions and one recipe
version in xtask; bump it when archive layout or build instructions change.
Archive names use versions and the platform or recipe, not source SHAs.
Fixture names include the workspace version and Agave recipe, not Yellowstone.
The workspace version also selects the release used by `with-runtime`.
Archive manifests record exact source revisions.

## Assemble locally

Install the checkout's pinned Rust toolchain, curl, tar, and native build tools.
macOS assembly builds Yellowstone from its exact upstream source using that
source's toolchain; Linux reuses the upstream plugin. Both include the complete
Agave distribution. SBF platform-tools download only when rebuilding fixtures.

```bash
cargo xtask package runtime target/assets
git worktree add --detach /tmp/redsuite-fixture-source HEAD
REDSUITE_ASSETS_DIR="$PWD/target/assets" \
  cargo xtask package fixtures /tmp/redsuite-fixture-source target/assets
cargo build --release --locked -p redsuite
cargo xtask package bundle target/assets/redsuite-runtime-*.tar.gz \
  target/assets/redsuite-fixtures-*.tar.gz target/release/redsuite target/bundles
```

Fixture assembly builds a fresh copy of a clean source checkout, not existing
`target/deploy` files. A bundle requires fixtures from the runner's source
commit. Manifests record source revisions and component build inputs. Assembly alone is not release qualification.

Archives have SHA-256 sidecars and include license notices. The runtime also
includes the corresponding Yellowstone source. Preserve these when redistributing.

To check a downloaded archive manually, place its `.sha256` sidecar from the
same release beside it and run:

```bash
bundle=redsuite-VERSION-PLATFORM.tar.gz
shasum -a 256 -c "$bundle.sha256"
```

For local development before the version's release assets are published, assemble
local assets or explicitly select local fixtures:

```bash
REDSUITE_ASSETS_DIR="$PWD/target/assets" REDSUITE_FIXTURES_DIR="$PWD/target/deploy" \
  scripts/with-runtime cargo run --locked -p redsuite -- redshift list
```

`with-runtime` verifies archives before atomically installing cache entries.
`REDSUITE_CACHE_DIR` overrides the dependency cache, which defaults to
`${XDG_CACHE_HOME:-$HOME/.cache}/redsuite`. Complete entries work offline.
To build the MagicBlock committor with pinned SBF tools, use
`cargo xtask runtime exec --runtime-only -- COMMAND ...`. It prepares Agave and
Yellowstone without downloading RedSuite fixtures.
`REDSUITE_BUILD_CACHE` separately controls assembly downloads and Yellowstone
build outputs. Workload state, logs, and reports stay outside both caches.

## Qualification before release

Extract the bundle outside the checkout, including into a path containing spaces:

```bash
mkdir -p '/tmp/relocated redsuite'
tar -xzf target/bundles/redsuite-*.tar.gz -C '/tmp/relocated redsuite'
cargo xtask package verify '/tmp/relocated redsuite'/redsuite-*
MAGICBLOCK_VALIDATOR_BIN=/absolute/path/to/target/release/magicblock-validator \
  cargo xtask package verify '/tmp/relocated redsuite'/redsuite-* --live
```

Live verification runs `cache_transport_recovery`, which observes gRPC refresh
while HTTP fetch and WebSocket update paths are withheld. Supply a compatible
MagicBlock build using the [operator setup](../README.md#prepare-the-validator-under-test),
including the committor and host prerequisites. Cold startup needs RPC access.
Replication scenarios additionally need a verifier.
Static CLI and dependency inspection does not establish a live pass.

Also exercise a modified local runner, the [fixture rebuild workflow](../README.md#rebuild-fixture-programs),
and cache reuse with network access disabled. Establish minimum macOS,
glibc, and CPU requirements on target hosts. CI's macOS 15 and Ubuntu 22.04 images
are build environments, not portability guarantees. Keep bundles unqualified
until the required checks pass with a compatible supplied MagicBlock build.

The workflow does not publish its draft. Complete live qualification and review
the release notes before approving publication; wait for all assets to finish
uploading. The development wrapper never selects `latest` or substitutes another
release. Preparing a RedSuite release does not update consumer pins in other
projects.
