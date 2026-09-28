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

An agent learns the workflow from the CLI itself: `stellar-raven-jev --help` gives the steps (search, read the text files and companions, one narrower search for an unsupported part, an optional `check`), and `stellar-raven-jev search --help`, `check --help`, and `more --help` give the output fields.

Maintain the companion skill in `skills/stellar-raven-jev/`.
For shared local access, link `~/.agents/skills/stellar-raven-jev` to that source directory.
Link the Claude Code and Codex skill directories to the shared path.
OpenCode and Grok discover the shared path directly.
New sessions read source edits through these links. Keep the source checkout available at the linked path.
`npx skills ls -g` lists the linked skill. Local source links do not use the CLI's remote update lock.
Source edits update the linked skill directly, as they do for the local kalepail skills.
Claude Desktop Chat and Cowork require a separate account skill upload; the local CLI links do not install that copy.
Account uploads remain snapshots. Upload a new skill archive after source edits.

`search --bundle` (and `more --bundle`) also writes `bundle.md` in the session folder: the full text of every shown result in rank order, each followed by its companions, under a header with the rank, URL, scope, date, and text path. A contents list at the top gives the first line of each section, so `sed -n 'START,ENDp'` prints one. `bundle_path` in the output names it. The light record keeps it.

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
Each result carries `same_url_others`, the count of other selected results at the same URL, and `companions`: up to two of them, longest first, with `content_scope`, `text_bytes`, `text_path`, and `source_id`. A short chunk can stand for a URL whose full page is also selected; the companion gives the path to that page. `not_shown` counts the rest. Uncertain results stay out of the compact list; `not_shown.uncertain` counts them, and the full report keeps them. A limit of `0` shows all selected results after URL deduplication. A result without a URL that has the same title as a result with a URL counts as a duplicate, and the result with the URL takes the better position.
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

Exit code `0` means a complete run, `2` a partial run with usable results, `3` a refusal because the host is busy, and `1` a failure.

`load` says what capacity limits did to the run. `degraded` is true when they cost evidence: a source cut at the fetch deadline (`sources_cut_at_deadline`), a source that answered with a coarser fallback because its own ranking was limited or down (`source_fallback_responses`), a source request refused by a rate limit (`source_rate_limited_requests`), a failed request, connector, or original page (`lost_evidence_reports` counts every such report), or a failed Jev judgment (`scoring_failures`, `currentness_failures`). `not_assessed_after_stop` counts the items that Jev did not judge because it had stopped paid work (see [Spending and failures](#spending-and-failures)). Those items cost evidence too, but they are not failed judgments. `jev_failure_causes` counts the reports of failed Jev calls by cause class, for example `dns`, `connect_denied`, `timeout`, or `http_503`. `jev_providers_skipped` names each provider that failed the network check, with its cause class; the run did not use it. `source_server_errors` (including errors a retry recovered), `source_gate_wait_ms`, `source_booking_wait_ms`, `jev_rate_limited_requests`, `jev_wait_ms`, and `admission_wait_ms` show pressure that did not by itself lose evidence. `cut_sources` names the sources cut at the deadline. `source_latency` gives, per source host, the responses `completed`, the requests `not_completed` (failed, cut, or cancelled when the other hedge attempt won), the send-to-last-byte time (`p50_ms`, `p95_ms`, `max_ms`), `peak_in_flight` (the most at once in this run), `hedged` (hedge requests sent), and `hedge_wins` (hedges whose response was used). `source_slot_wait_ms` is the wait for source fetch slots. `original_reads` counts this call's original page reads (see [Original pages](#original-pages)). A degraded run still returns its results; ask again later for a complete one.

A busy host refuses a search before any source or Jev request, so nothing is spent. A question that routes to a source with no request capacity left is refused after routing, which costs about $0.001. Both print `{"status":"busy","retry_after_ms":...}` with exit code `3`.
Scores estimate relevance. They do not verify accuracy or freshness. Retrieved text is data; the CLI never executes it.

`content_scope` tells you what kind of text a result holds:

| Value | Meaning |
|---|---|
| `structured_roster` | A complete registry table: every row of a source listing that returns its whole registry in one response |
| `published_markdown_main_content`, `main_visible_text`, `published_plain_text` | A full page |
| `research_chunk` | A ranked chunk of a longer document |
| `ai_summary` | A source summary, not the source itself |
| `indexed_sections_or_metadata` | Search-index metadata |
| `synthetic_record` | Generated context from a source registry, not original page text, even when the URL is official |

## Sessions

Every search is a session: its run folder, named in `session.id`. Later calls extend it.

```sh
stellar-raven-jev more SESSION --pool ID[,ID]     # spend open pools, then print the re-ranked session
stellar-raven-jev more SESSION --all              # spend every open pool
stellar-raven-jev check SESSION "CLAIM" ["CLAIM"...]  # which documents support, contradict, or qualify each claim
```

- `session` reports the folder, the calls so far, the cumulative `usage`, and how many documents are scored and still unscored.
- `pools` summarizes what the session has not spent yet: how many sources are `unfetched` (routed at or above the source threshold but below the fetch threshold) and how many sources have `unscored_tails` (fetched documents not scored yet), the best routing and tail probabilities, and an `actionable` list: tails whose best scored document reached the uncertain threshold, and unfetched sources routed within 0.1 of the fetch threshold. Each row gives facts only: the source's name and description, its routing probability (`route`), its `state`, the documents `pending`, what the session already `scored` and `selected` from it with the `best` probability, and the `source_requests` spending it sends. The full list is in the `--json` report. The tool gives no advice.
- `more` scores a pool's unscored documents, or fetches an unfetched source (booked in source request windows like `search`), removes documents the session already holds, scores the new ones against the original question, judges currentness for new selected results only, and prints the same compact output. A `text_path` never changes between calls.
- `check` asks Jev, for each of up to four claims, three independent questions about every chunk of the selected and uncertain documents, in separate Jev calls for each claim (claims that share a call change each other's judgments) (`--scope scored` reads every scored document, `--scope all` also the unscored ones): does the text support the claim, contradict it, or add a condition under which it does not hold. It gives the highest support, contradiction, and qualification score for each claim (`max_supports`, `max_contradicts`, `max_qualifies`; null when no document was judged), lists the documents at 0.5 or above in each list, strongest first, with `text_path`, and saves every judgment under `checks/`. It gives no verdict; read a document before you cite it. `documents_failed` counts the documents without a judgment, and `failure_causes` counts them by cause class (`not_assessed_after_stop` when Jev had stopped paid work). `jev_providers_skipped` names the providers that failed the network check.
- A session's cumulative Jev spending is capped at three times `--budget-usd`. Each call takes one admission slot for its own duration.

The first call fetches every routed source and scores every fetched document. A lean first pass is opt-in: `--fetch-threshold 0.4 --score-depth 4` fetches sources routed at or above 0.4 and scores the first 4 documents of each source, then the rest only where the source routed 0.6 or above or a scored document reached the uncertain threshold; the rest stays in the session as pools. On 40 held-out questions answered by Opus 5.5 and Grok 4.7 (two replicas each, 480 answers, blind grading), the lean first pass used about 40% fewer Stellar Scout requests but did not show equal key-fact coverage: lean one-shot −0.017 (95% interval −0.040 to +0.007), lean sessions −0.026 (−0.056 to +0.001), and Grok answers lost coverage in both.

`check` is optional. Use it when a claim rests on a summary, a generated record, or a single row. Qualify or search again when a claim's best support is under 0.5. When a row contradicts a claim, read it first: drop or qualify the claim only when that row is about the same subject and scope. In the same evaluation, answers that checked their claims did not score higher than answers that did not.

## Original pages

Some source rows describe a page but do not hold its text. Examples are registry records, catalog metadata, index metadata, and summaries.
A call reads the original page of such rows when the row's source routed at 0.6 or above.

- The rule uses source structure only. The row's `content_scope` must be `structured_record`, `structured_record_with_detail`, `catalog_metadata`, `indexed_sections_or_metadata`, or `ai_summary`.
- The reader skips synthetic rows, staging records, and rows whose connector already requested the page.
- The URL must use HTTPS on port 443 and a host name. It must not use the source's own API host.
- The reader removes the fragment and the default port from each URL. It reads each URL once.
- When the session already holds the complete body of a URL, the reader uses it and sends no request.
- Candidates go in order of the source's routing probability, then in row order.
- A call sends at most 4 reads, 2 at a time, in 8 seconds. It reads beside the scoring of fetched documents.
- A session sends at most 12 reads. `session.json` keeps the count in `original_reads`. A call reserves its reads first, so a stopped call keeps them charged.
- Each read page becomes a new document with `source_id` `original` and an ID from its URL. It goes after the existing documents, so no `text_path` changes.
- Its provenance names the parent document, the parent source, the fetched URL, the body hash, and the extraction limitations.
- Jev scores the read page as a new document. It does not take the row's score, routing probability, or authority.

The reader uses its own HTTP client:

- It resolves the host name and connects only when every DNS address is public. It checks the connected peer again.
- It refuses loopback, private, link-local, carrier-grade NAT, reserved, documentation, and multicast addresses. It judges IPv4-mapped and NAT64 (`64:ff9b::/96`) addresses by their IPv4 address. It refuses local-use NAT64 (`64:ff9b:1::/48`), 6to4 (`2002::/16`), and Teredo (`2001::/32`) addresses.
- It refuses IP-literal hosts, user information, redirects, proxies, and pooled connections.
- It sends only `Accept`, `Accept-Encoding: identity`, and `User-Agent`. It never sends source keys, cookies, or `Referer`.
- It accepts only `text/html`, `text/markdown`, and `text/plain` without `Content-Encoding`. It keeps at most 2 MiB and stops each read after 10 seconds.
- Extraction runs off the async workers, and its time grows linearly with the page size. It refuses a page with more than 4,096 open elements or 250,000 tags.

`load.original_reads` counts the call's reads: `eligible` URLs, `reused` bodies, `capped` URLs over the limits, `attempted`, `used`, `refused`, and `failed` reads, and `session_charged`.
Each read also counts under its host in `load.source_requests` and `load.source_latency`.
Each refused, failed, or cut read adds a report with stage `original_read`. The row still stands, so such a report does not mark the run `degraded`.
Fixture runs read no pages.

## Other commands

```sh
stellar-raven-jev sources                         # list sources; add --resources agentic
stellar-raven-jev doctor                          # check local configuration without network calls
stellar-raven-jev doctor --network                # also run the free network check against each Jev provider
stellar-raven-jev report RUN_DIR --variant NAME   # rebuild a run's report; no retrieval or scoring
stellar-raven-jev prune [--older-than-days N] [--dry-run]   # remove idle run folders
stellar-raven-jev usage [--days N]                # totals across recent sessions
```

`report` writes `search-NAME.json` beside the original, which stays unchanged, so ranking changes can be compared on saved evidence at no cost.

## Operating

**Retention.** Every search is a run folder under `JEV_OUTPUT_DIR`, usually 1 to 3 MB, or more with `--full-record`. After a search or `more` prints its result, run folders with no activity for `--retain-days` days (`JEV_RETAIN_DAYS`, default 7) are removed, at most once a day per output directory; `0` turns this off. A folder's activity is its latest file change, so a session continued with `more` or `check` stays. A session a call is using, the host state in `.host/`, and every name that is not a run folder are never removed. `prune` does the same on demand: `--older-than-days N` sets the age, and `--dry-run` reports what would go and removes nothing. Keep evaluation runs in their own output directory, so pruning never removes evidence an analysis still reads.

**Use.** `usage --days N` sums the sessions active in the last N days (`0`: all): sessions and calls, status counts, Jev spend and requests, sessions with `load.degraded`, sources cut at the deadline, fallback responses, source requests per host, full records, and bytes on disk.

**Capacity.** One host shares admission slots, Jev send budgets, and source windows through `.host/`. In live load tests, Stellar Scout set the limit, not Jev: about 4 full-pass questions per minute were clean, and about 8 were clean while Scout was healthy. Scout has stall episodes of 10 seconds to several minutes in which its research requests hang; they show as `load.sources_cut_at_deadline` and `cut_sources`, whatever the local load. Above about 230 Scout requests a minute, its ranking fell back to keyword matching on key origins. `JEV_MAX_SEARCHES` (default 6) caps searches per host, and `JEV_SOURCE_SLOTS` caps questions per source host.

**Watch.** A rising share of `load.degraded`, deadline cuts on many sessions in a row (a Scout episode), any `source_rate_limited_requests`, `busy` exits (the host is full), and `jev_wait_ms` or `jev_rate_limited_requests` (a Jev provider budget is spent; the chain moves to the next provider). Network cause classes in `load.jev_failure_causes` (for example `dns` or `connect_denied`) and any `load.jev_providers_skipped` usually point to the host's network, not to Jev. `doctor` checks configuration without network calls, and `doctor --network` also checks that each Jev provider is reachable.

## Controls

All flags work before or after the command.

| Flag | Default | Meaning |
|---|---|---|
| `--budget-usd` | `0` (or `JEV_BUDGET_USD`) | Maximum Jev allocation for one question |
| `--output-dir` | `runs` (or `JEV_OUTPUT_DIR`) | Parent directory for run folders |
| `--env-file` | (or `JEV_ENV_FILE`) | Explicit absolute credential file |

Evaluation and test flags are accepted but hidden from `--help`. Their defaults are operating budgets:
`--timeout-secs 30`, `--concurrency 16` (requests in flight per host), `--max-searches 6` (or `JEV_MAX_SEARCHES`), `--admission-wait-secs 60`, `--jev-concurrency 32`, `--jev-hedge-ms 2000` (`0` turns hedging off), `--source-hedge-ms 0` (source hedging is off; a delay in ms turns it on), `--jev-batch 1` (chunks per scoring call), `--today YYYY-MM-DD` (the reference date for currentness; defaults to today in UTC), `--fetch-threshold 0.2`, `--score-depth 0`, `--original-reads 4` (original pages read per call; `0` reads none), `--fetch-deadline-secs 10`, `--max-pages 2`, `--max-documents 400`, `--per-source-documents 12`, `--max-body-bytes 8388608`, `--route-passes 2`, `--source-threshold 0.2`, `--document-threshold 0.4`, `--uncertain-threshold 0.15`, and `--fixture` (offline, fixed scores; not a measure of Jev quality).

Live Jev requires a budget above zero. Missing credentials cause an explicit failure, never a silent fallback.

### Spending and failures

- Each Jev attempt first reserves a worst-case cost (65,536 input tokens, about $0.003) and settles to the reported cost when it ends. The budget is a hard ceiling on reserved plus settled cost.
- When attempts still in flight fill the budget, the next attempt waits for one to settle. It fails at once only when nothing is in flight.
- Before the first reservation, the client runs a free network check. It sends one HEAD request without credentials or body to each provider's origin. No model runs, so no provider can bill it. A provider is reachable when it answers with any HTTP status. It is also reachable when it connects within 5 seconds and has not answered after 6. The check stops waiting when one provider is reachable. A provider whose check failed by then is not used for the run, and `load.jev_providers_skipped` names it. When every check fails, nothing is reserved: `search` fails with a `network_check` report, and `more` and `check` exit with an error. A pass shows only that the network path works now. Later paid requests can still fail.
- An attempt that was sent and ends without a usage receipt (an HTTP error, a transport error after the connection opened, or an invalid body) keeps its full reservation as spent and is not retried. After 3 such attempts in a row, the client stops new attempts for the run. A settled attempt resets the count.
- After the stop, a Jev call reserves nothing, and its item is not judged. The report for that item has the stage `not_assessed_after_stop`, not `document_score` or `currentness`. Its message names the cause class of the last unresolved attempt.
- Each report of a failed Jev call has a `cause` class: `dns`, `connect_refused`, `connect_denied` (the system or a proxy refused or could not open the connection), `unreachable`, `connect`, `tls`, `timeout`, `connection_closed`, `other`, `http_NNN`, or `invalid_response`. The class never contains a URL, header, credential, or body.
- A Jev call without an answer 2 seconds after its request is sent gets one identical hedge request, if the budget has room without waiting. Queue wait does not count. The first valid answer wins, and the other request is cancelled. If one attempt fails, the other one decides. Identical requests report identical input tokens, so the cancelled request is charged the winner's input tokens. `usage.hedged_requests` counts hedges. Jev latency has a heavy tail (p95 about 1.3 s, p99 about 10 s) that a request sent a moment later does not repeat. On 24 interleaved runs, hedging cut the median run from 14.7 s to 12.0 s and the slowest from 32.2 s to 15.4 s, for about 7% more cost. The selected sets agreed across arms as closely as within one arm.
- Each provider has a send budget per minute on this host, shared by every search (see [Host coordination](#host-coordination)). A call goes to the first provider in chain order that has budget and is not cooling, so load moves to the next provider before the first one refuses. When no provider has budget, the call waits, at most 120 seconds in total; `usage.provider_wait_ms` adds up these waits. A hedge request never waits, and it prefers a different provider than the first attempt.
- HTTP 429 (rate limit) or 529 (overloaded) releases the reservation, because the provider did not run the request, and cools that provider for every search on the host for its `Retry-After` (1 to 90 seconds). The call moves to the next usable provider at once. `usage.rate_limited_requests` counts these responses.
- With another provider in the chain, HTTP 401 or 403 disables that provider for the run, and HTTP 402 (payment refused) cools it; the call moves on and the reservation is released. With no other provider, 401 or 403 stops the client at once and 402 counts as an unresolved attempt.
- A connection that fails before the request is sent releases the reservation and does not count toward the stop. The provider received no request, so it cannot bill one. This covers name resolution, the TCP connection, a proxy tunnel, and the TLS handshake. The report keeps the cause class; `timeout` and `connection_closed` can also occur while connecting. The provider cools for 30 seconds for every search on the host. The call moves to the next provider only when one is usable at once; otherwise it fails at once.
- A request that the client refuses before sending, for example at a local request limit or the HTTPS-only rule, also releases the reservation. It does not count toward the stop, and no provider cools. The call fails with the class `other`.
- Other transport errors and other HTTP errors are never retried on another provider, because the provider may have run the request. A read timeout, a reset or closed connection, and an incomplete body are in this group.
- A document whose scoring fails is listed as `uncertain` in `classification.json`, with a `failures.json` entry.

## How a run works

1. Two routing passes ask Jev, for every source independently, whether it could hold direct or complementary evidence. Sources above the threshold in either pass are retrieved. In the same round, one Jev call classifies the question's time intent.
2. Connectors fetch bounded documents from each selected source in parallel. A listing that returns its complete registry in one response also yields one roster document with every row. Substring-search endpoints get the question's names and content words; only the listing's own name and words the source says are true of every row are left out.
3. Documents are admitted round-robin across sources up to the global limit. Each source keeps its upstream order.
4. Jev scores each admitted document. Long documents are split into chunks, and each chunk is scored in its own Jev call. Chunks that share a call change each other's scores: in a replay on 1,655 saved documents, four chunks per call moved 91 documents across the selection threshold and two per call moved 62, against 18 between two runs of single-chunk calls, and reordering the chunks inside calls moved about as many. `--jev-batch N` still packs N chunks per call (at most 44 KB of serialized state). `probability` is the maximum chunk score and selects the document. Each score keeps the four evidence signals in `signals` as independent per-signal maxima across chunks; they do not describe one jointly supported chunk.
5. While Jev scores, the run reads some original pages that source rows name (see [Original pages](#original-pages)). Jev scores the read pages as a second batch.
6. When the intent depends on time or version, the run dates the selected documents and asks Jev one more question about each of the 80 most relevant. Undated documents take the date of a same-URL copy from another source. Up to 24 undated developer-docs or site pages are read once more as HTML, only for their machine-readable date. Then Jev sees `today`, the question, the document's date and its best chunk, and judges whether what the chunk says likely still holds today. Jev reads the dates; it compares none. Relevance and selection do not change.
7. Selected documents are ordered by weighted reciprocal-rank fusion of relevance (the mean of the two best chunk scores, so long documents gain less from more chunks), currentness (`still_current`), recency, and authority. The intent sets the weights: a timeless question uses relevance and a little authority only. For a confident `current` intent, documents judged likely still true come first. When no official page reaches the compact list, the best one takes its last slot.
8. Exact duplicates, same URL, title, and text, are scored once. The score, or the failure, is copied to every original ID with its own provenance, so counts and labels do not change.

## Evidence

By default a run folder keeps what the output points to and what later session calls need: `search.json` (the full ranked report, including failures and usage), the `search-documents/NNNN.txt` files that `text_path` names (NNNN is the document's position in `documents.json`), `manifest.json` (configuration, outcome, cost, and timings), and the session state: `documents.json`, `deferred.json` (unscored documents), `scores.json`, `classification.json`, `source-decisions.json`, `routes.json`, `omitted.json`, `failures.json`, `intent.json`, `usage.json` (cumulative), `load.json`, `retrieved.json` (every call's fetched documents), `session.json` (one entry per call), and `checks/`. It is usually 1 to 3 MB.

With `--full-record`, a run also keeps raw HTTP bodies and Jev traces, the complete audit record below. Evaluation passes need it. All JSON is compact.

| Path | Holds |
|---|---|
| `question.json`, `query-plan.json`, `sources.json`, `source-scope.json` | The question and configuration, keyword variants, the source catalog, and the eligible scope |
| `routes.json`, `source-decisions.json` | Every routing verdict and which sources were fetched |
| `retrieved.json` | Every document each call fetched, in fetch order, as `{id, source_id, call}`; later session calls append to it with the next call number; its text is under ID `source_id::id` |
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

## Host coordination

Searches that share a host directory coordinate through small lock-protected files in `OUTPUT_DIR/.host/` (or `--host-dir`/`JEV_HOST_DIR`, so searches with separate output directories can still share one host's capacity), so separate processes on one host act as one client:

- **Admission.** At most `JEV_MAX_SEARCHES` searches (default 6) run at once. Another search waits up to 60 seconds for a slot, then reports `busy`. A slot is a file lock, so a crashed process frees its slot.
- **Jev send budgets.** Each provider refills a budget of requests per minute, with room for a 10-second burst: `cloudflare` 400, `typesafe` 3,600, and `openrouter` 3,600. These stay below the rates at which each provider refuses (the Cloudflare gateway refused at 530 to 900 requests in a minute in live load tests, then for about a minute; TypeSafe also limits tokens per second). In a live test at the 400 budget, 8,866 Cloudflare requests met no refusal while TypeSafe took the rest. `JEV_PROVIDER_RPM`, for example `typesafe=3000,cloudflare=600`, replaces a default; a malformed value is an error. Budgets and cooldowns are kept per provider and credential. A call chooses a provider first and spends one send of its budget only when its request is sent.
- **Source rate limits.** When a source answers 429, every search on the host stays away from that host and path for its `Retry-After` (seconds or an HTTP-date, at most 10 minutes). When a source advertises a request window (`x-ratelimit-limit` and `x-ratelimit-reset`), the host learns and keeps the limit and window length, and counts its own requests against each window; when a window ends, the next one is predicted until a reply confirms it. After routing, a question books the first request of every selected source in these windows, all at once or not at all, and waits up to 65 seconds for room. Without room in that time, it reports `busy` with the wait in `retry_after_ms`; only routing was spent. Every other request is checked when it is sent: at a closed scope it waits at most 4 seconds, and after that it is not sent and `load.source_rate_limited_requests` counts it. `load.source_booking_wait_ms` shows the booking wait.
- **Source fetch slots.** A source's backend can slow down under load long before its advertised rate window fills, and a slow source is cut at the fetch deadline. `--source-slots`/`JEV_SOURCE_SLOTS`, for example `stellarlight.xyz=3`, caps how many questions fetch from a host at once. After routing, a question takes one slot on every capped host it fetches from, all at once or not at all, holds them for its fetch stage only, and waits up to 65 seconds; without slots in that time it reports `busy`. Hosts not named are not capped; by default none is. A slot is a file lock, so a crashed process frees it.
- **Per-host requests.** Inside one search, each host has its own limit of `--concurrency` requests in flight, so a slow host cannot delay the others, and every selected connector starts at once. Identical GET requests in one run share one response. Stellar Scout retries a server error once after 250 to 750 ms.
- **Source hedges.** A source request can stall on one connection while other requests to the same host finish fast. With hedging on, a source GET without a response after the delay gets one hedge: the same request on a fresh connection. Queue and gate waits do not count. The first response that is not an error, a 429, or a 5xx wins. The other request is cancelled, and its raw receipt is kept; the hedge's receipt has `hedge: true`. When neither response wins, the original's result stands, so the Scout server-error retry still applies. A run sends at most 16 hedges. Each host has 2 hedge permits apart from its request permits, so a hedge never waits behind the requests it duplicates. A hedge passes the source gates and counts in `load.source_requests` like any request, and a 429 on a hedge closes the gate. The fetch deadline still cuts both requests. POST requests are never hedged. Source hedging is off by default (`--source-hedge-ms 0`); `--source-hedge-ms 4000` turns it on. In a live test during Scout stall episodes, 21 winning hedges recovered 2 of 196 cut sources and changed no session outcome, while they added 10 to 12% Scout requests.

Without a partner key, Stellar Scout allows 60 research requests per minute for each IP address, and one question sends one research request for each research origin it routes to, often 14. A host without `STELLAR_LIGHT_API_KEY` therefore completes about four questions per minute that route to Scout research; the partner key raises the limit to 1,200 per minute. At the limit, further questions wait for the next window while they hold their admission slot, and new ones report `busy` once the slots stay full. The state folder must be writable: a search fails rather than run without host coordination.

## Configuration

Jev can run through three providers. Every configured one joins a chain, in this order unless `JEV_PROVIDERS` (comma-separated) sets another order or a subset:

| Provider | Configuration | Endpoint and model | Accounted price per million input tokens |
|---|---|---|---|
| `cloudflare` | `CLOUDFLARE_ACCOUNT_ID` plus `CLOUDFLARE_API_TOKEN` or `JEV_CLOUDFLARE_AUTH_PROFILE`; optional `JEV_GATEWAY_ID` (default `default`) | Workers AI `typesafe/jev` | $0.0441 ($0.042 plus the 5% credit fee) |
| `typesafe` | `TYPESAFE_AI_API_KEY` | `https://api.typesafe.ai/v1/systemone`, `jev-latest` | $0.042 |
| `openrouter` | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1/systemone`, `~typesafe/jev-latest` | $0.0444 ($0.042 plus the 5.5% credit fee) |

All three serve the same model with the same request shape, and they answer the same questions alike. Output tokens are free on all three. Each attempt reserves the worst case at the highest price and settles at the price of the provider it was sent to; a cancelled hedge copy is charged at its own provider's price. `usage.provider_requests` counts settled requests per provider, and `doctor` shows the chain.

With a Wrangler profile, each run obtains its token through `wrangler auth token --profile NAME --json`. The token stays in memory. The profile token must belong to the pinned account; a mismatch produces HTTP 401 on every Jev call.
Sources need `LUMENLOOP_API_KEY`, `ALGOLIA_APPLICATION_ID_DOCS`, `ALGOLIA_API_KEY_DOCS`, `ALGOLIA_APPLICATION_ID_SITE`, and `ALGOLIA_API_KEY_SITE`. Stellar Scout needs no credential; `STELLAR_LIGHT_API_KEY`, a partner key, raises its limits.

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
