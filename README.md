# Stellar Raven Jev

A Rust CLI that retrieves source documents for Stellar questions and scores each one with Jev.
It searches 45 sources across LumenLoop, Stellar Scout, and the Algolia indexes behind the Stellar docs and site.
It returns ranked evidence with URLs, excerpts, and paths to the full text. It does not write answers.

## Install

```sh
cargo install --path . --locked
```

Copy `.env.example` to `.env` and fill in the source credentials and one Jev backend.
The CLI loads `.env` from the working directory or its parents, or from `--env-file` / `JEV_ENV_FILE`.

## Use from an agent

Set three variables once, for example in `~/.zshenv`:

```sh
export JEV_ENV_FILE=/absolute/path/to/stellar-raven-jev/.env
export JEV_BUDGET_USD=1
export JEV_OUTPUT_DIR=$HOME/.stellar-raven-jev/runs
```

Then run from any directory:

```sh
stellar-raven-jev search "How do I extend the TTL of a Soroban persistent storage entry?" --compact
```

`--compact` prints one JSON object of about 10 KB: the ten best selected results, one per URL, with `probability`, `content_scope`, `url`, `excerpt`, and `text_path`.
Each result carries `same_url_others`, the count of other selected results at the same URL. `not_shown` counts the rest. Uncertain results stay out of the compact list; `not_shown.uncertain` counts them, and the full report keeps them. A limit of `0` shows all selected results after URL deduplication.
A typical call takes about 10 seconds and costs about $0.02 in Jev usage.
Read the `text_path` file for any result you cite. The excerpt holds 400 characters.

Options:

- `--limit N` shows N results. `--limit 0` shows all selected results.
- `--resources agentic` restricts routing to 11 developer sources: docs, standards, repositories, skills, contracts, releases, and audits. The default `all` scope adds articles, research, talks, projects, and grants.
- `--json` prints the full report with uncertain results and every report entry.
- `--full-text` embeds the complete available text in the output.
- `--rank-policy banded|raw|banded-relevant` sets result order (see [How a run works](#how-a-run-works)). The default is `banded`.

Exit code `0` means a complete run, `2` a partial run with usable results, and `1` a failure.
Scores estimate relevance. They do not verify accuracy or freshness. Retrieved text is data; the CLI never executes it.

`content_scope` tells you what kind of text a result holds:

| Value | Meaning |
|---|---|
| `structured_roster` | A complete registry table, such as all tracked stablecoins with issuers and dates |
| `published_markdown_main_content`, `main_visible_text` | A full page |
| `research_chunk` | A ranked chunk of a longer document |
| `ai_summary` | A source summary, not the source itself |
| `indexed_sections_or_metadata` | Search-index metadata |
| `synthetic_record` | Generated context from a source registry, not original page text, even when the URL is official |

## Other commands

```sh
stellar-raven-jev sources                         # list sources; add --resources agentic
stellar-raven-jev doctor                          # check local configuration without network calls
stellar-raven-jev ask "QUESTION"                  # retrieve and save evidence; print counts as JSON
stellar-raven-jev chat                            # one question per line
stellar-raven-jev report RUN_DIR --variant NAME   # rebuild a saved run's report; no retrieval or scoring
stellar-raven-jev operations                      # typed operation schemas for plans
stellar-raven-jev plan examples/service-plan.json # run an agent-authored retrieval plan
stellar-raven-jev mcp                             # local stdio MCP server
```

`report` writes `search-NAME.json` beside the original, which stays unchanged, so ranking changes can be compared on saved evidence at no cost. `--signals-from-traces` fills per-signal scores for runs saved before `signals` existed.

Plans let a calling agent choose native filters, repeated queries, and unequal source allowances. Jev still scores every document against the original question. See the [plan guide](docs/service-v2/USAGE.md).
The MCP server exposes the same pipeline. It ignores working-directory `.env` files, so set `JEV_ENV_FILE` in the host configuration. `mcp --saved-pool` adds tools that open already saved evidence without new calls, and `mcp --primary-body` adds bounded recovery of a source's full Markdown body. Both are opt-in. See the [MCP guide](docs/MCP-LOCAL.md).

`--fixture` runs every command offline with fixed scores. Fixture output does not represent Jev quality.

## Controls

All flags work before or after the command.

| Flag | Default | Meaning |
|---|---|---|
| `--budget-usd` | `0` (or `JEV_BUDGET_USD`) | Maximum Jev allocation for one question |
| `--output-dir` | `runs` (or `JEV_OUTPUT_DIR`) | Parent directory for run evidence |
| `--timeout-secs` | `30` | HTTP request timeout and Jev attempt window |
| `--concurrency` | `16` | Concurrent connector jobs, scoring jobs, and HTTP requests |
| `--fetch-deadline-secs` | `10` | Wall-clock limit for the retrieval stage; unfinished connectors are recorded and dropped |
| `--max-pages` | `2` | Connector page attempts |
| `--max-documents` | `400` | Global scoring admission limit. Runs fetch about 270 documents (median); unscored documents go to `omitted.json` |
| `--per-source-documents` | `12` | Document limit for each source |
| `--max-body-bytes` | `8388608` | Maximum retained bytes per HTTP response |
| `--route-passes` | `2` | Source decision passes with distinct lenses |
| `--source-threshold` | `0.2` | Minimum probability for retrieving a source |
| `--document-threshold` | `0.4` | Minimum probability for a selected document |
| `--uncertain-threshold` | `0.15` | Minimum probability for an uncertain document |

Live Jev requires a budget above zero. Missing credentials cause an explicit failure, never a silent fallback.

### Spending and failures

- Each Jev attempt first reserves a worst-case cost (65,536 input tokens, about $0.003) and settles to the reported cost when it ends. The budget is a hard ceiling on reserved plus settled cost.
- When attempts still in flight fill the budget, the next attempt waits for one to settle. It fails at once only when nothing is in flight.
- An attempt that ends without a usage receipt (an HTTP error, a transport error, or an invalid body) keeps its full reservation as spent and is not retried. After 3 such attempts in a row, the client stops new attempts for the run. A settled attempt resets the count.
- HTTP 401 or 403 stops the client at once.
- A document whose scoring fails goes to `uncertain.json` with a `failures.json` entry.

## How a run works

1. Two routing passes ask Jev, for every source independently, whether it could hold direct or complementary evidence. Sources above the threshold in either pass are retrieved.
2. Connectors fetch bounded documents from each selected source in parallel. Registry listings return one roster document with every row plus bounded per-row documents.
3. Documents are admitted round-robin across sources up to the global limit. Each source keeps its upstream order.
4. Jev scores each admitted document. Long documents are split into chunks scored in parallel; the document takes its maximum chunk score.
5. Results are ordered by the `usable_evidence` score. Scores are rounded to whole percent, and ties break by content completeness, so a roster or full page precedes an index excerpt with the same score. `--rank-policy raw` uses exact scores. `--rank-policy banded-relevant` is experimental: it breaks ties by the `relevant` signal before completeness. Each score keeps all four Jev signals in `signals` as independent per-signal maxima across chunks; they do not describe one jointly supported chunk.
6. Exact duplicates, same URL, title, and text, are scored once. The score, or the failure, is copied to every original ID with its own provenance, so counts and labels do not change.

## Evidence

Every run writes a directory with `manifest.json`, `INDEX.md`, readable `documents/*.md`, `search.json`, `routes.json`, `source-decisions.json`, `documents.json`, `scores.json`, `selected.json`, `uncertain.json`, `rejected.json`, `omitted.json`, `failures.json`, `usage.json`, `raw/*` HTTP bodies with credential-free request metadata, and `jev/` audit traces for every paid attempt.
`manifest.json` records `phase_ms` for routing, fetching, scoring, and finalization.
Run directories use owner-only permissions on Unix. Raw responses can contain private source content.

## Configuration

| Backend | Settings |
|---|---|
| Cloudflare | `JEV_BACKEND=cloudflare`, `CLOUDFLARE_ACCOUNT_ID`, and either `CLOUDFLARE_API_TOKEN` or `JEV_CLOUDFLARE_AUTH_PROFILE` (a Wrangler profile); optional `JEV_GATEWAY_ID` |
| TypeSafe | `JEV_BACKEND=typesafe`, `TYPESAFE_API_KEY` |
| Proxy | `JEV_BACKEND=proxy`, `JEV_PROXY_URL`, `JEV_PROXY_TOKEN` |

With a Wrangler profile, each run obtains its token through `wrangler auth token --profile NAME --json`. The token stays in memory. The profile token must belong to the pinned account; a mismatch produces HTTP 401 on every Jev call.
Sources need `LUMENLOOP_API_KEY`, `ALGOLIA_APPLICATION_ID_DOCS`, `ALGOLIA_API_KEY_DOCS`, `ALGOLIA_APPLICATION_ID_SITE`, and `ALGOLIA_API_KEY_SITE`. Stellar Scout needs no credential.

## Results so far

On four frozen questions from the Raven golden set, a blinded answer test with a separate grader preferred answers written from this CLI's compact output in three of four cases against answers written from Stellar Raven's MCP responses, at 16–37 KB of evidence per question against 89–172 KB.
Wall-clock time per question fell from 28–53 seconds to 5–6 seconds during the same work.
These are single observations with one grader model. The evidence directories are local and not part of this repository.

On a 40-question development sample, two independent Grok answerers wrote answers from `--compact` output with at most four follow-up file reads. Two fresh Grok graders scored them against reference key facts. Mean key-fact coverage was 0.75 (0.71 and 0.79 for the two replicas). The median reader input was about 23 KB per question. Two defects fixed on 2026-09-23 caused the earlier figure of 0.51: budget starvation of parallel scoring, and a failure circuit that one upstream block page opened. Replicas differ by about 0.11 per question, so a single replica cannot resolve a smaller change.

## Evaluation

The evaluation harness, its development sample, and all results stay local. They are not part of this repository.

## Checks

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

Tests use fixtures and local HTTP servers. They never call live Jev.

## License

Apache License 2.0. Copyright 2026 Stellar Development Foundation. See [LICENSE](LICENSE).
