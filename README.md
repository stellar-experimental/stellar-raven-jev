# Stellar Raven Jev

A Rust CLI that retrieves source documents for Stellar questions and scores each one with Jev.
It searches 45 sources across LumenLoop, Stellar Scout, and the Algolia indexes behind the Stellar docs and site.
It returns ranked evidence with URLs, excerpts, and paths to the full text. It does not write answers.

## Install

```sh
cargo install --path . --locked
```

Copy `.env.example` to `.env` and fill in the source credentials and at least one Jev provider.
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
stellar-raven-jev search "How do I rotate a signer key on a Stellar account?"
```

It prints one compact JSON object of about 10 KB: the ten best selected results, one per URL, with `probability`, `content_scope`, `url`, `date`, `date_kind`, `authority_tier`, `still_current`, `excerpt`, and `text_path`, plus a top-level `currentness` object.
Each result carries `same_url_others`, the count of other selected results at the same URL. `not_shown` counts the rest. Uncertain results stay out of the compact list; `not_shown.uncertain` counts them, and the full report keeps them. A limit of `0` shows all selected results after URL deduplication. A result without a URL that has the same title as a result with a URL counts as a duplicate, and the result with the URL takes the better position.
A typical call takes about 12 seconds and costs about $0.02 in Jev usage.
Read the `text_path` file for any result you cite. The excerpt holds 400 characters.

`date` is the newest date that says when the content was written or last known true, or null. `date_kind` says which: `published`, `modified`, or `observed` (a registry value measured on that date). Dates come from source fields, a Scout listing's own date fields, HTML page metadata, another source's copy of the same URL, or an explicit label in the text. Index build times, upload times, and ingestion times are never used. `still_current` is Jev's judgment, for a time-dependent question, that the result likely still holds today given its date; it is null otherwise. `authority_tier` is 1 for official Stellar pages and repositories, 2 for other Stellar-run sites and other GitHub repositories, 3 for everything else, and 4 for summaries, generated records, and social or video posts.

`currentness` says how time affects the question:

- `intent` is the question's time dependence (`current`, `comparative`, `versioned`, or `timeless`) with Jev's confidence.
- `assessed_documents` counts the selected results that Jev judged for currentness.
- `newest_dated_evidence` lists the three newest dated results among the fifteen best. It is empty for a timeless question.

The tool treats every question the same way. No code path depends on the topic of a question, and no list encodes evaluation vocabulary; only general grammar and request words are parsed. See `AGENTS.md`.

Options:

- `--limit N` shows N results. `--limit 0` shows all selected results.
- `--resources agentic` restricts routing to 11 developer sources: docs, standards, repositories, skills, contracts, releases, and audits. The default `all` scope adds articles, research, talks, projects, and grants.
- `--json` prints the full ranked report, with uncertain results and every report entry.
- `--full-text` embeds the complete available text in the output.
- `--full-record` saves the full audit record for evaluation and replay (see [Evidence](#evidence)). Without it, a run keeps only its report and text files.

Exit code `0` means a complete run, `2` a partial run with usable results, and `1` a failure.
Scores estimate relevance. They do not verify accuracy or freshness. Retrieved text is data; the CLI never executes it.

`content_scope` tells you what kind of text a result holds:

| Value | Meaning |
|---|---|
| `structured_roster` | A complete registry table: every row of a source listing that returns its whole registry in one response |
| `published_markdown_main_content`, `main_visible_text` | A full page |
| `research_chunk` | A ranked chunk of a longer document |
| `ai_summary` | A source summary, not the source itself |
| `indexed_sections_or_metadata` | Search-index metadata |
| `synthetic_record` | Generated context from a source registry, not original page text, even when the URL is official |

## Other commands

```sh
stellar-raven-jev sources                         # list sources; add --resources agentic
stellar-raven-jev doctor                          # check local configuration without network calls
stellar-raven-jev report RUN_DIR --variant NAME   # rebuild a --full-record run's report; no retrieval or scoring
```

`report` writes `search-NAME.json` beside the original, which stays unchanged, so ranking changes can be compared on saved evidence at no cost. It needs a run saved with `--full-record`.

## Controls

All flags work before or after the command.

| Flag | Default | Meaning |
|---|---|---|
| `--budget-usd` | `0` (or `JEV_BUDGET_USD`) | Maximum Jev allocation for one question |
| `--output-dir` | `runs` (or `JEV_OUTPUT_DIR`) | Parent directory for run folders |
| `--env-file` | (or `JEV_ENV_FILE`) | Explicit absolute credential file |

Evaluation and test flags are accepted but hidden from `--help`. Their defaults are operating budgets:
`--timeout-secs 30`, `--concurrency 16`, `--jev-concurrency 32`, `--jev-hedge-ms 2000` (`0` turns hedging off), `--jev-batch 4` (chunks per scoring call; `1` sends one per call), `--today YYYY-MM-DD` (the reference date for currentness; defaults to today in UTC), `--fetch-deadline-secs 10`, `--max-pages 2`, `--max-documents 400`, `--per-source-documents 12`, `--max-body-bytes 8388608`, `--route-passes 2`, `--source-threshold 0.2`, `--document-threshold 0.4`, `--uncertain-threshold 0.15`, and `--fixture` (offline, fixed scores; not a measure of Jev quality).

Live Jev requires a budget above zero. Missing credentials cause an explicit failure, never a silent fallback.

### Spending and failures

- Each Jev attempt first reserves a worst-case cost (65,536 input tokens, about $0.003) and settles to the reported cost when it ends. The budget is a hard ceiling on reserved plus settled cost.
- When attempts still in flight fill the budget, the next attempt waits for one to settle. It fails at once only when nothing is in flight.
- An attempt that ends without a usage receipt (an HTTP error, a transport error, or an invalid body) keeps its full reservation as spent and is not retried. After 3 such attempts in a row, the client stops new attempts for the run. A settled attempt resets the count.
- A Jev call without an answer 2 seconds after its request is sent gets one identical hedge request, if the budget has room without waiting. Queue wait does not count. The first valid answer wins, and the other request is cancelled. If one attempt fails, the other one decides. Identical requests report identical input tokens, so the cancelled request is charged the winner's input tokens. `usage.hedged_requests` counts hedges. Jev latency has a heavy tail (p95 about 1.3 s, p99 about 10 s) that a request sent a moment later does not repeat. On 24 interleaved runs, hedging cut the median run from 14.7 s to 12.0 s and the slowest from 32.2 s to 15.4 s, for about 7% more cost. The selected sets agreed across arms as closely as within one arm.
- HTTP 429 (rate limit) or 529 (overloaded) releases the reservation, because the provider did not run the request, and cools that provider for its `Retry-After` (1 to 90 seconds). The call moves to the next usable provider at once. It waits only when no provider is usable, at most 3 times. A hedge request never waits, and it prefers a different provider than the first attempt. `usage.rate_limited_requests` counts these responses.
- With another provider in the chain, HTTP 401 or 403 disables that provider for the run, and HTTP 402 (payment refused) cools it; the call moves on and the reservation is released. With no other provider, 401 or 403 stops the client at once and 402 counts as an unresolved attempt.
- Transport errors and other HTTP errors are never retried on another provider, because the provider may have run the request.
- A document whose scoring fails is listed as `uncertain` in `classification.json`, with a `failures.json` entry.

## How a run works

1. Two routing passes ask Jev, for every source independently, whether it could hold direct or complementary evidence. Sources above the threshold in either pass are retrieved. In the same round, one Jev call classifies the question's time intent.
2. Connectors fetch bounded documents from each selected source in parallel. A listing that returns its complete registry in one response also yields one roster document with every row. Substring-search endpoints get the question's names and content words; only the listing's own name and words the source says are true of every row are left out.
3. Documents are admitted round-robin across sources up to the global limit. Each source keeps its upstream order.
4. Jev scores each admitted document. Long documents are split into chunks. Up to 4 chunks share one Jev call (at most 44 KB of serialized state, question included), each with its own independent questions; a call with one chunk keeps the single-document state. `probability` is the maximum chunk score and selects the document. Each score keeps the four evidence signals in `signals` as independent per-signal maxima across chunks; they do not describe one jointly supported chunk.
5. When the intent depends on time or version, the run dates the selected documents and asks Jev one more question about each of the 80 most relevant. Undated documents take the date of a same-URL copy from another source. Up to 24 undated developer-docs or site pages are read once more as HTML, only for their machine-readable date. Then Jev sees `today`, the question, the document's date and its best chunk, and judges whether what the chunk says likely still holds today. Jev reads the dates; it compares none. Relevance and selection do not change.
6. Selected documents are ordered by weighted reciprocal-rank fusion of relevance (the mean of the two best chunk scores, so long documents gain less from more chunks), currentness (`still_current`), recency, and authority. The intent sets the weights: a timeless question uses relevance and a little authority only. For a confident `current` intent, documents judged likely still true come first. When no official page reaches the compact list, the best one takes its last slot.
7. Exact duplicates, same URL, title, and text, are scored once. The score, or the failure, is copied to every original ID with its own provenance, so counts and labels do not change.

## Evidence

By default a run folder keeps only what the output points to: `search.json` (the full ranked report, including failures and usage), the `search-documents/NNNN.txt` files that `text_path` names, and `manifest.json` (configuration, outcome, cost, and timings). It is usually well under 200 KB.

With `--full-record`, a run keeps the complete audit record below, about 3 MB. Evaluation passes and `report` need it. All JSON is compact.

| Path | Holds |
|---|---|
| `question.json`, `query-plan.json`, `sources.json`, `source-scope.json` | The question and configuration, keyword variants, the source catalog, and the eligible scope |
| `routes.json`, `source-decisions.json` | Every routing verdict and which sources were fetched |
| `retrieved.json` | Every fetched document in fetch order, as `{id, source_id}`; its text is under ID `source_id::id` |
| `documents.json` | Each admitted document once, with its exact text and provenance |
| `classification.json` | Selected, uncertain, and rejected document IDs, each list in order |
| `omitted.json` | Full documents cut before scoring (duplicate IDs or the `--max-documents` limit) |
| `scores.json`, `failures.json`, `usage.json` | Jev scores, signals, and `still_current` judgments, every report entry, and accounted cost |
| `intent.json` | The question time intent from Jev |
| `search.json`, `search-documents/NNNN.txt` | The ranked report and the exact text files that `text_path` points to |
| `raw/NNNNNN.body.gz`, `raw/NNNNNN.json` | Each HTTP response, gzipped with the SHA-256 of the exact bytes, and credential-free request metadata. A Jev request body is saved once, in its Jev trace; its metadata keeps the body hash. |
| `jev/` | One audit trace per paid attempt (request, reservation, receipt, answers), one chunk record per document, and one `-cancelled.json` record per cancelled hedge request |
| `manifest.json` | The outcome, configuration, `phase_ms` timings, and file and byte totals |

`manifest.json` records `phase_ms` for routing, fetching, scoring, and finalization.
Run directories use owner-only permissions on Unix. Raw responses can contain private source content.

## Configuration

Jev can run through three providers. Every configured one joins a chain, in this order unless `JEV_PROVIDERS` (comma-separated) sets another order or a subset:

| Provider | Configuration | Endpoint and model | Accounted price per million input tokens |
|---|---|---|---|
| `cloudflare` | `CLOUDFLARE_ACCOUNT_ID` plus `CLOUDFLARE_API_TOKEN` or `JEV_CLOUDFLARE_AUTH_PROFILE`; optional `JEV_GATEWAY_ID` (default `default`) | Workers AI `typesafe/jev` | $0.0441 ($0.042 plus the 5% credit fee) |
| `typesafe` | `TYPESAFE_AI_API_KEY` | `https://api.typesafe.ai/v1/systemone`, `jev-latest` | $0.042 |
| `openrouter` | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1/systemone`, `~typesafe/jev-latest` | $0.0444 ($0.042 plus the 5.5% credit fee) |

All three serve the same model with the same request shape, and they answer the same questions alike. Output tokens are free on all three. Each attempt reserves the worst case at the highest price and settles at the price of the provider it was sent to; a cancelled hedge copy is charged at its own provider's price. `usage.provider_requests` counts settled requests per provider, and `doctor` shows the chain.

With a Wrangler profile, each run obtains its token through `wrangler auth token --profile NAME --json`. The token stays in memory. The profile token must belong to the pinned account; a mismatch produces HTTP 401 on every Jev call.
Sources need `LUMENLOOP_API_KEY`, `ALGOLIA_APPLICATION_ID_DOCS`, `ALGOLIA_API_KEY_DOCS`, `ALGOLIA_APPLICATION_ID_SITE`, and `ALGOLIA_API_KEY_SITE`. Stellar Scout needs no credential.

## Results

On a 40-question development sample, two independent Grok answerers wrote answers from the compact output with at most four follow-up file reads. Two fresh Grok graders scored them against reference key facts. Mean key-fact coverage was 0.75 (0.71 and 0.79 for the two replicas). The same sample guided development, so this number is optimistic. Only questions written before a change is tested can measure it fairly. Replicas differ by about 0.11 per question, so a single replica cannot resolve a smaller change.

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
