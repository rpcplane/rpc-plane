# Contributing to RPC Plane

Thanks for your interest in improving RPC Plane. This is a small, focused
codebase — contributions that keep it that way are the most welcome.

## Before you start

- **Bugs and features:** open an issue using the
  [bug report](https://github.com/rpcplane/rpc-plane/issues/new?template=bug_report.yml)
  or [feature request](https://github.com/rpcplane/rpc-plane/issues/new?template=feature_request.yml)
  template. For anything non-trivial, please discuss it in an issue before
  opening a PR so we can agree on the approach.
- **Questions / usage help:** use
  [Discussions](https://github.com/rpcplane/rpc-plane/discussions), not issues.
- **Security vulnerabilities:** do **not** open a public issue. Follow
  [SECURITY.md](SECURITY.md).

## Development setup

The toolchain is pinned in `rust-toolchain.toml` (currently Rust **1.96.1**);
`rustup` will install it automatically when you build.

```bash
git clone https://github.com/rpcplane/rpc-plane
cd rpc-plane
cargo build
cargo run -p rpc-plane -- --help
```

`tools/dummy-rpc` is a stub upstream you can point providers at for local
testing, and `tools/load-test` drives synthetic traffic.

## Before you open a PR

CI runs these three checks and treats clippy warnings as errors. Run them
locally first — they're exactly what the `Lint & Test` job runs:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

## Pull requests

- Keep PRs focused; one logical change per PR.
- Add or update tests for behavior changes. Most proxy logic is covered by
  unit and integration tests alongside the binary source.
- Update `README.md`, `config.example.toml`, and the relevant `examples/`
  configs when you add or change a config option.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `perf:`, `chore:`, `docs:`, …). Release notes are generated
  from commit history, so clear messages matter.
- Rebase on `main` and make sure the branch is green before requesting review.

## Contributor License Agreement

You'll sign the [CLA](CLA.md) on your first pull request — once, via the CLA
Assistant bot. You keep ownership of your contributions; RPC Plane stays under
the [Elastic License 2.0](LICENSE).
