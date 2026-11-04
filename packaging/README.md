# Component releases

RedSuite runners, Agave/Yellowstone runtimes, and fixtures are separate archives.
A runner release does not rebuild or upload runtime or fixture archives.
There are no self-contained bundles.

`shared/components.toml` selects runtime and fixture releases explicitly. Their
pins stay unchanged across runner releases until a maintainer updates them.
The initial pins reuse assets already published with RedSuite 3.4.0.

## Release a runner

Pushing a version tag (`N.N.N`) runs the [runner workflow](../.github/workflows/bundles.yml).
It builds only the macOS Apple Silicon and Linux x86-64 runners and attaches
`redsuite-runner-VERSION-PLATFORM.tar.gz` and checksum sidecars to the tag's
GitHub Release, creating a draft if needed. Each archive includes the CLI,
component pins, license, and source revision. Reruns replace matching assets.
Manual workflow runs only upload Actions artifacts.

## Update shared components

Build new components only when explicitly changing them:

- For Agave/Yellowstone changes, update the pins and bump `runtime.version`.
  Bump that version for build-recipe changes too. The [runtime workflow](../.github/workflows/runtimes.yml)
  builds both platforms for a `runtime-VERSION` tag.
- For fixture changes, bump `fixtures.version`. The
  [fixture workflow](../.github/workflows/fixtures.yml) builds shared SBF programs
  for a `fixtures-VERSION` tag. Its runtime must already be published.

Both workflows also support manual builds without release writes. Tag runs attach
archives to a draft, without replacing existing assets. Publish the component
release before selecting it in `runtime.release` or `fixtures.release` for the
next runner release. Never overwrite an existing component version.

Fixture archives contain compiled programs, a license, and build provenance, not
the source snapshot. They need not match the runner's commit. Maintainers must
bump the fixture version when programs or their host interfaces change together.

## Assemble locally

```bash
cargo build --release --locked -p redsuite
cargo xtask package runner target/release/redsuite target/assets
```

For a new runtime, `cargo xtask package runtime target/assets` assembles Agave and
Yellowstone. macOS builds the pinned Yellowstone source; Linux uses the upstream
plugin. `REDSUITE_BUILD_CACHE` controls downloads and Yellowstone build outputs.

To package fixtures, build from a clean checkout:

```bash
git worktree add --detach /tmp/redsuite-fixture-source HEAD
cargo xtask package fixtures /tmp/redsuite-fixture-source target/assets
```

This uses the pinned published runtime. To use locally assembled archives instead,
set `REDSUITE_ASSETS_DIR` to their directory.

## Run and reuse

`./scripts/redsuite` acquires the published runner for the checkout's workspace
version and its pinned runtime/fixtures, then executes the CLI. For a modified
source runner, use `./scripts/with-runtime cargo run --locked -p redsuite --`.
Both wrappers leave MagicBlock binaries and the committor user-supplied.

`REDSUITE_CACHE_DIR` overrides the component cache, which defaults to
`${XDG_CACHE_HOME:-$HOME/.cache}/redsuite`. Existing component directories are
reused without verification or network access. Remove a component's directory
if it needs to be downloaded again. Missing published assets fail; there is no
fallback release or automatic build.

`REDSUITE_FIXTURES_DIR` selects locally rebuilt fixtures instead of a download.
`cargo xtask runtime exec --runtime-only -- COMMAND ...` prepares only Agave and
Yellowstone, for example when building the MagicBlock committor.

Release builds do not run live qualification. CI's macOS 15 and Ubuntu 22.04
images are build environments, not minimum OS or CPU guarantees.
