# alwith-runtime — rules for agents

## Verify locally before pushing

This repo has its own `target/`; clippy and the test suite take seconds. Before any push, run
`cargo clippy --locked --workspace --all-targets -- -D warnings` and `cargo test --locked --workspace`
here and make them green. CI is a second look, never the first: pushing to find out whether it
compiles wastes five minutes per round. Grep the whole workspace (`apps/`, every crate, test modules
included) for every call site before changing a signature, so one push carries the whole change.

Origin: user instruction (2026-10-19), after one change went through nine pushes, five of them
compile errors CI could have been spared.

## Never bump a minor or major version on your own

Agents may bump the patch version (x.y.Z) only. Minor and major bumps are the user's call; "breaking
change" or "semver convention" is not a reason to do it alone. Ask first.

Origin: user instruction (2026-10-19).

## Commit messages: English only, one commit per change

- Write commit messages in English only. No Chinese in subjects or bodies, in any repo.
- Squash churn before pushing: one commit per logical change. Never leave an add-then-remove sequence (a feature commit followed by its revert, or a dependency pinned and re-pinned) in history; rewrite it into the net result.
- Origin: user instruction (2026-10-20).

## Unified ALwith package versions

All published `@alwith/*` packages, including Runtime and its platform packages, use the `0.5.x` release line. Consumers must follow that line. The transition to `0.5.0` is explicitly authorized; subsequent releases increment only the patch version. Third-party packages keep their own versions. Origin: user instruction (2026-10-30).

## Rust formatting: stable only, never nightly

- Format Rust with stable `cargo fmt` only. `rustfmt.toml` keeps options that only nightly applies; on stable they are ignored with a warning, which is known and accepted.
- Never run `cargo +nightly`, `rustfmt +nightly` or `rustup toolchain install nightly`, and never install a nightly toolchain to make those warnings go away.
- Origin: user instruction (2026-07-25 in alwith-desktop; repeated here 2026-11-25 after a session auto-installed nightly through `cargo +nightly fmt`).
