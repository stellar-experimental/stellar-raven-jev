# Contributing

Read [AGENTS.md](AGENTS.md) first. Its "General purpose only" rules apply to every change.
The tool must answer any Stellar question. Do not add code, tests, or docs for one question or one topic.

## Setup

Install Rust with [rustup](https://rustup.rs). [`rust-toolchain.toml`](rust-toolchain.toml) selects the
toolchain, with `rustfmt` and `clippy`.

```sh
git clone https://github.com/stellar-experimental/stellar-raven-jev
cd stellar-raven-jev
cargo build --locked
```

You do not need credentials to build or test. To make live calls, see [docs/configuration.md](docs/configuration.md).

## Checks

Run these three checks before you open a pull request. CI runs them too, and it also runs `cargo deny check`.

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

## Tests

- Tests never call live sources or Jev. They use `--fixture` runs and local HTTP servers.
- `search`, `more`, and `check` without `--fixture` spend money and use shared rate limits.
  Do not run them from a test.
- Tests use made-up names, questions, and facts. Do not copy evaluation questions into tests, fixtures, or docs.
- Command tests set their own settings. They do not depend on variables from your shell or a local `.env` file.

## Code map

| Path | Role |
|---|---|
| `src/main.rs` | Command-line options, `--help` text, and the commands |
| `src/pipeline.rs` | One run: routing, retrieval, admission, scoring, original pages, currentness, and sessions (`more`) |
| `src/jev.rs` | Jev requests, the provider chain, budgets, reservations, and hedging |
| `src/http.rs` | The recording HTTP client, source rate-limit gates, source hedging, and the original-page client |
| `src/governor.rs` | Host-wide state shared between processes: admission slots, Jev send budgets, source windows, and fetch slots |
| `src/connectors/` | One file per source family (`algolia.rs`, `lumenloop.rs`, `stellarlight.rs`), plus `original.rs` for original pages and `mod.rs` for the source registry |
| `src/extract.rs` | Shared HTML and Markdown text extraction, used by the Algolia connector and the original-page reader |
| `src/query.rs` | The query planner: keyword and semantic queries from the words of the question |
| `src/rank.rs` | Dates, authority tiers, and the final ranking |
| `src/search.rs` | The report, the compact output, and the evidence bundle |
| `src/session.rs` | Session records, pools, and `check` |
| `src/maintenance.rs` | `prune` and `usage` |
| `src/types.rs` | Shared types |
| `src/*/tests.rs` | Unit tests of large modules, in their own files |
| `tests/` | Command tests, multi-process tests, and fixtures |
| `skills/stellar-raven-jev/` | The agent skill |
| `docs/` | Configuration and operating reference |

## Documentation

- `--help` is the reference for commands, flags, and output fields. Change it with the code.
- [README.md](README.md) holds what a new user needs first.
- [docs/configuration.md](docs/configuration.md) and [docs/operating.md](docs/operating.md) hold settings and mechanisms.
- [`.env.example`](.env.example) lists each variable with one short comment.
- The skill holds guidance for agents. It points to `--help` for fields.
- Write current facts only. Do not add dates of past runs, test results, or change history. Git keeps the history.

## Security

Report security problems as [SECURITY.md](SECURITY.md) describes. Do not open a public issue for them.
