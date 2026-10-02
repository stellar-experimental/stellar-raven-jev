# Operating

This file explains how a run works and how sessions grow.
It also explains how searches on one host share capacity, and what a run folder holds.
[configuration.md](configuration.md) lists the settings and the spending rules.

## How a run works

1. Routing. Two passes ask Jev, for each source independently, whether it could hold direct or
   complementary evidence. A source above the source threshold in either pass is selected.
   In the same round, one Jev call classifies the time intent of the question.
2. Retrieval. Connectors fetch bounded documents from each selected source in parallel.
   A listing that returns its complete registry in one response also gives one roster document with every row.
   Substring-search endpoints get the names and content words of the question.
   They do not get the listing's own name or words that the source says are true of every row.
3. Admission. Documents are admitted round-robin across sources, up to `--max-documents`.
   Each source keeps its own order.
4. Scoring. Jev scores each admitted document. A long document is split into chunks.
   Each chunk is scored in its own Jev call, because chunks that share a call change each other's scores.
   `probability` is the highest chunk score, and it selects the document.
   `signals` holds the highest value of each of the four evidence signals across chunks.
   These maxima can come from different chunks.
5. Original pages. While Jev scores, the run reads some original pages that source rows name
   (see [Original pages](#original-pages)). Jev scores these pages as a second batch.
6. Currentness. When the intent depends on time or version, the run dates the selected documents.
   Jev then judges the 80 most relevant: does the best chunk likely still hold today, given its date?
   An undated document takes the date of a copy of the same URL from another source.
   Up to 24 undated developer-docs or site pages are read again as HTML, for their machine-readable date only.
   The code gives Jev today's date and the document's date. Jev judges the age; it does no date arithmetic.
   Relevance and selection do not change.
7. Ranking. Selected documents are ordered by weighted reciprocal-rank fusion of relevance, currentness,
   recency, and authority. Relevance is the mean of the two best chunk scores, so a long document gains less
   from many chunks. The intent sets the weights. A timeless question uses relevance and a little authority.
   When no official page reaches the compact list, the best official page takes its last slot.
8. Duplicates. Documents with the same URL, title, and text are scored once.
   The score, or the failure, is copied to each ID with its own provenance.

The tool asks the same general questions of every question and every document.
No code path depends on the topic of a question. See [AGENTS.md](../AGENTS.md).

## Output fields

`<command> --help` lists the output fields. This section gives the detail.

### Dates and currentness

- `date` is the newest date that says when the text was written or last known true, or null.
- `date_kind` is `published`, `modified`, or `observed`. `observed` is a registry value measured on that date.
- Dates come from source fields, a listing's own date fields, or HTML page metadata.
  They also come from another source's copy of the same URL, or from an explicit label in the text.
  Index build, upload, and ingestion times are never used.
- `still_current` is Jev's judgment, for a time-dependent question, that the result likely still holds today.
  It is null for other questions.
- `currentness.intent` is the time dependence of the question (`current`, `comparative`, `versioned`, or
  `timeless`), with Jev's confidence.
- `currentness.assessed_documents` counts the selected results that Jev judged for currentness.
- `currentness.newest_dated_evidence` lists the three newest dated results among the fifteen best.
  It is empty for a timeless question.

### Authority

| `authority_tier` | Documents |
|---|---|
| 1 | `developers.stellar.org`, `stellar.org`, and repositories under `github.com/stellar/` |
| 2 | `skills.stellar.org`, `communityfund.stellar.org`, and other `github.com` repositories |
| 3 | Other sites |
| 4 | `ai_summary` and `synthetic_record` documents on any host, and every page on `x.com`, `twitter.com`, `youtube.com`, `youtu.be`, and `medium.com` |

The `content_scope` check comes first. The host check then applies to the page URL.
A page on a tier 4 host is tier 4 even when it holds ordinary text.

### Content scope

| `content_scope` | Meaning |
|---|---|
| `structured_roster` | A complete registry table: every row of a listing that returns its whole registry in one response |
| `published_markdown_main_content`, `main_visible_text`, `article_visible_text`, `body_visible_text`, `published_plain_text`, `stored_editorial_body`, `skill_markdown_entrypoint` | Full text of a page or file |
| `research_chunk` | A ranked chunk of a longer document |
| `structured_record`, `structured_record_with_detail`, `catalog_metadata` | A registry or catalog row: metadata, not page text |
| `indexed_sections_or_metadata` | Search-index metadata |
| `ai_summary` | A summary from the source, not the source itself |
| `synthetic_record` | Context that the source generated, not page text, even when the URL is official |

### Results and duplicates

- `companions` lists up to two other selected documents at the same URL, longest first.
  It is present only when there are some. A short chunk can stand for a URL whose full page is also selected.
- `same_url_others` counts the other selected results at the same URL.
- `not_shown` counts what the compact list leaves out: duplicate URLs, results beyond `--limit`, and
  uncertain results. `--json` prints the full report. `--limit 0` shows all selected results.
- A result without a URL that has the same title as a result with a URL counts as a duplicate.

### Load

`load` says what capacity limits and failures did to the call. `degraded` is true when evidence was lost:

- A source was cut, refused, or fell back to coarser matching.
- Documents went over the scoring limit.
- A request, a Jev judgment, or host coordination failed.

| Field | Meaning |
|---|---|
| `degraded` | True when `lost_evidence_reports`, `source_fallback_responses`, or `source_rate_limited_requests` is not zero |
| `lost_evidence_reports` | Reports of lost evidence: deadline cuts, documents over the scoring limit, failed requests, connectors, Jev judgments, or host coordination, and items not judged after a stop |
| `sources_cut_at_deadline`, `cut_sources` | Sources cut at the fetch deadline |
| `source_fallback_responses` | Sources that answered with coarser matching because their own ranking was limited or down |
| `source_rate_limited_requests` | Source requests not sent because of a rate limit |
| `scoring_failures`, `currentness_failures` | Failed Jev judgments |
| `not_assessed_after_stop` | Items not judged because Jev stopped paid work. They cost evidence, but they are not failed judgments. |
| `jev_failure_causes` | Failed Jev calls by cause class, for example `dns`, `connect_denied`, `timeout`, `http_503` |
| `jev_providers_skipped` | Providers that failed the network check, with the cause class |
| `source_server_errors` | Source server errors, including errors that a retry recovered |
| `source_gate_wait_ms`, `source_booking_wait_ms`, `source_slot_wait_ms` | Waits for source rate limits and fetch slots |
| `jev_rate_limited_requests`, `jev_wait_ms`, `admission_wait_ms` | Jev refusals and waits, and the wait for a search slot |
| `source_requests` | Requests per source host |
| `source_latency` | Per source host: `completed`, `not_completed`, `cancelled_while_queued`, `queue_p95_ms`, `queue_max_ms`, `p50_ms`, `p95_ms`, `max_ms`, `peak_in_flight`, `hedged`, `hedge_wins` |
| `original_reads` | Original page reads (see [Original pages](#original-pages)) |

A degraded call still returns its results. Ask again later for a complete one.
The call status is `partial` (exit 2) only when `lost_evidence_reports` or `source_rate_limited_requests` is not zero.
A coarser fallback alone sets `degraded`, but the status stays `complete`.

## Sessions and pools

A session is a run folder. `search` starts it, and `more` and `check` extend it.

- `session` gives the folder, the calls so far, the cumulative `usage`, and the counts of scored and unscored documents.
- A session can spend at most 3 times `--budget-usd` across its calls.
- Each call holds the session. A second call on the same session prints `busy` and exits 3.
- A `text_path` never changes between calls.

A default search fetches every selected source and scores every fetched document, so it leaves no pools.
The lean first pass, `--fetch-threshold 0.4 --score-depth 4`, does less at first:

- It fetches only sources routed at or above 0.4. The other selected sources become `unfetched` pools.
- It scores the first 4 documents of each source. It scores the rest only when the source routed at 0.6
  or above, or a scored document reached the uncertain threshold. The rest become `unscored_tails` pools.

`pools` summarizes what the session has not spent. `pools.actionable` lists tails whose best scored document
reached the uncertain threshold, and unfetched sources routed within 0.1 of the fetch threshold.
Each row gives facts only: the source, its routing score, its state, and the pending documents.
It also gives what the session already scored and selected from that source.

`more` spends pools. It fetches or scores them against the original question, removes documents that the
session already holds, and prints the re-ranked session. Fetches are booked in source request windows as in `search`.

`check` asks Jev three questions about each claim and each chunk of the documents in scope.
Does the text support the claim? Does it contradict the claim? Does it add a condition under which the claim does not hold?

- Each claim is judged in its own Jev calls, because claims that share a call change each other's judgments.
- `--scope selected` (default) reads selected and uncertain documents. `scored` reads every scored document.
  `all` also reads the unscored documents.
- For each claim, the output gives `max_supports`, `max_contradicts`, and `max_qualifies`.
  It lists the documents at 0.5 or above, strongest first. It gives no verdict.
- Each printed list keeps at most `--limit` rows (default 5). `checks/` holds the judgment of every document.
- `documents_failed` and `failure_causes` count the documents without a judgment.
- Every judgment is saved under `checks/`.

## Original pages

Some source rows describe a page but do not hold its text: registry records, catalog metadata,
index metadata, and summaries. A call reads the original page of such a row when the row's source routed
at 0.6 or above.

- The row's `content_scope` must be `structured_record`, `structured_record_with_detail`, `catalog_metadata`,
  `indexed_sections_or_metadata`, or `ai_summary`. The rule uses source structure only.
- The reader skips synthetic rows, staging records, and rows whose connector already requested the page.
- The URL must use HTTPS on port 443 and a host name. It must not use the source's own API host.
- The reader removes the fragment and the default port, and it reads each URL once.
  When the session already holds the complete body of a URL, the reader uses it and sends no request.
- Candidates go in order of the source's routing score, then in row order.
- A call reads at most 4 pages, 2 at a time, in 8 seconds. A session reads at most 12 pages.
  A call reserves its reads first, so a stopped call keeps them charged.
- Each page becomes a new document with `source_id` `original`. It goes after the existing documents.
  Its provenance names the parent document and source, the URL, the body hash, and the extraction limits.
- Jev scores the page as a new document. It does not take the row's score, routing score, or authority.

The reader uses its own HTTP client:

- It connects only when every DNS address of the host is public, and it checks the connected peer again.
- It refuses loopback, private, link-local, carrier-grade NAT, reserved, documentation, and multicast addresses.
  It judges IPv4-mapped and NAT64 (`64:ff9b::/96`) addresses by their IPv4 address.
  It refuses local-use NAT64 (`64:ff9b:1::/48`), 6to4 (`2002::/16`), and Teredo (`2001::/32`) addresses.
- It refuses IP-literal hosts, user information, redirects, proxies, and pooled connections.
- It sends only `Accept`, `Accept-Encoding: identity`, and `User-Agent`. It never sends source keys, cookies, or `Referer`.
- It accepts only `text/html`, `text/markdown`, and `text/plain` without `Content-Encoding`.
  It keeps at most 2 MiB, and it stops each read after 10 seconds.
- It refuses a page with more than 4,096 open elements or 250,000 tags.
- It skips a host that has a fetch-slot cap.

`load.original_reads` counts `eligible` URLs, `reused` bodies, `capped` URLs, `slot_host_skipped` URLs,
and `attempted`, `used`, `refused`, and `failed` reads, and `session_charged`.
A refused, failed, or cut read adds a report with the stage `original_read`.
The row still stands, so such a report does not mark the call `degraded`. Fixture runs read no pages.

## Host coordination

Searches that share a host folder act as one client. The folder is `OUTPUT_DIR/.host/`,
or `--host-dir` / `JEV_HOST_DIR`. It holds small lock-protected files.
The folder must be writable. A search fails rather than run without host coordination.

### Admission

At most `JEV_MAX_SEARCHES` searches (default 6) run at once. Another search waits up to 60 seconds
for a slot, then prints `busy` and exits 3. It spends nothing. A slot is a file lock, so a crashed process frees it.

### Jev send budgets

Each provider has a budget of requests per minute on the host, with room for a 10-second burst.
The defaults are `cloudflare` 400, `typesafe` 3,600, and `openrouter` 3,600. `JEV_PROVIDER_RPM` replaces a default.
These defaults stay below the rates at which the providers refuse requests.

- A call spends one send only when its request is sent.
- Budgets and cooldowns are kept per provider and credential.
- When one provider has no budget, the call goes to the next provider before the first one refuses.

### Source rate limits

- When a source answers HTTP 429, every search on the host stays away from that host and path.
  The wait is the `Retry-After` value (seconds or an HTTP date), at most 10 minutes.
- A source can advertise a request window with `x-ratelimit-limit` and `x-ratelimit-reset`.
  The host then keeps the limit and window length, and it counts its own requests against each window.
  A window marked `x-ratelimit-scope: instance` describes one serving instance, not the host, so it is not kept.
- When a question routes to two or more Scout research sources, they share one research request (`source=a,b,c&perSource=N`).
  Scout reads each source as in a single-source call and groups the rows by source. A source that Scout cannot read fails alone.
  The question books one research request for all of them.
- The tool keeps each Scout host at or under 32 requests in flight and 600 research requests per minute.
  An advertised window never raises this limit.
- After routing, a question books the first request of every selected source, all at once or not at all.
  It waits up to 65 seconds for room. Without room, it prints `busy` with `retry_after_ms`. Only routing was spent.
- Every other request is checked when it is sent. At a closed window it waits at most 4 seconds.
  After that it is not sent, and `load.source_rate_limited_requests` counts it.

### Source fetch slots

A source can slow down under load before its rate window fills. A slow source is cut at the fetch deadline.
`--source-slots` / `JEV_SOURCE_SLOTS` caps how many questions fetch from a host at once, as `HOST=N[,HOST=N]`.

- After routing, a question takes one slot on each capped host that it fetches from. It takes all or none.
  It holds them for its fetch stage only. It waits up to 65 seconds, then prints `busy`.
- A host with an in-flight limit has a default cap.
  The cap is that limit divided by the requests that one question may have in flight there. For `stellarlight.xyz` this is 32 / 16 = 2 slots, or 1 slot with source hedging on.
- A named host replaces its default. For a host with an in-flight limit, the tool refuses settings where slots times requests per question exceed that limit. Other hosts are not capped.
- A slot is a file lock, so a crashed process frees it.

### Requests in one search

- Each host has its own limit of `--concurrency` requests in flight, so a slow host does not delay the others.
- Identical GET requests in one run share one response.
- Stellar Scout retries HTTP 500, 502, or 503 once. HTTP 504 is Scout's function time cap and is treated as a timeout.
  It also retries a request once when it fails before a complete response, unless the cause is a timeout. It waits for Scout's `Retry-After` plus up to 500 ms, or 250 to 750 ms without one.
  A `Retry-After` above 4 seconds, or a retry that would end within 1 second of the fetch deadline, is not retried.
- A Scout HTTP failure report names Scout's `Retry-After` and the `error` field of its JSON body, when Scout sent them.
- Scout failure and fallback reports end with Scout's request ID, `Server-Timing`, and match mode.

### Source hedging

Source hedging is off by default. `--source-hedge-ms N` turns it on with a delay of N ms.

- A source GET with no response after the delay gets one hedge: the same request on a new connection.
  Queue and gate waits do not count.
- The first response that is not an error, a 429, or a 5xx wins. The other request is cancelled.
  Its raw receipt is kept, and the hedge's receipt has `hedge: true`.
- When no response wins, the original's result stands.
- A run sends at most 16 hedges. Each host has 2 hedge permits in addition to its request permits.
- A hedge passes the source gates and counts in `load.source_requests`. A 429 on a hedge closes the gate.
- The fetch deadline cuts both requests. POST requests are never hedged.

### Signals to watch

- A rising share of `load.degraded`.
- Deadline cuts on many sessions in a row. Stellar Scout usually sets the throughput limit of a host.
  Latency counts from send. `cancelled_while_queued` counts requests that a cut stopped before they left this client, and `queue_p95_ms` and `queue_max_ms` show how long sent requests waited here first. That time is not the source's.
- Any `source_rate_limited_requests`, and `busy` exits.
- `jev_wait_ms` or `jev_rate_limited_requests`: a provider budget is spent, and calls move to the next provider.
- Network cause classes in `load.jev_failure_causes` (for example `dns` or `connect_denied`) and
  `load.jev_providers_skipped` usually point to the host's network, not to Jev. `doctor --network` checks it.

## Retention and use

Each search makes a run folder under the output folder, named `<unix seconds>-<uuid>`.

- After a `search` or `more` prints its result, run folders with no activity for `--retain-days` days
  (default 7) are removed. This happens at most once a day per output folder.
  `--retain-days 0` turns this automatic removal off.
- A folder's activity is its latest file change, so a session that `more` or `check` continued stays.
- Pruning never removes a session that a call holds or the host folder `.host/`.
  It also never removes a name that is not a run folder.
- `prune` removes idle run folders on demand. `--older-than-days N` sets the age, and the default is `--retain-days`.
  The age must be at least 1, so with `--retain-days 0`, give `--older-than-days`. `--dry-run` removes nothing.
- Keep evaluation runs in their own output folder, so that pruning does not remove evidence that you still need.

`usage --days N` (default 7, `0` for all) sums the sessions that were active in the last N days.
It gives sessions and calls, status counts, Jev spend and requests, degraded sessions, and deadline cuts.
It also gives fallback responses, source requests per host, full records, and bytes on disk.

## Run folder

By default a run folder keeps what the output points to and what later calls need, usually 1 to 3 MB:

- `search.json`: the full ranked report, with failures and usage.
- `search-documents/NNNN.txt`: the text files that `text_path` names. NNNN is the position in `documents.json`.
- `manifest.json`: configuration, outcome, cost, and `phase_ms` timings for routing, fetching, scoring, and finalization.
- The session state: `question.json`, `source-scope.json`, `routes.json`, `source-decisions.json`,
  `documents.json`, `deferred.json`, `scores.json`, `classification.json`, `omitted.json`, `failures.json`,
  `intent.json`, `usage.json`, `load.json`, `retrieved.json`, and `session.json`.
- `checks/` after a `check`, and `bundle.md` after `--bundle`.
- `search-NAME.json` and `search-documents-NAME/` after `report`. NAME is the `--variant` value (default `replay`).

`--bundle` writes `bundle.md`: the full text of each shown result in rank order, each followed by its
companions. A contents list at the top gives the first line of each section. `bundle_path` names the file.

`report RUN_DIR [--variant NAME]` rebuilds the report from the saved state, without retrieval or scoring.
NAME defaults to `replay`.
The original `search.json` does not change, so you can compare ranking changes on saved evidence at no cost.

With `--full-record`, a run also keeps the complete audit record:

| Path | Holds |
|---|---|
| `query-plan.json`, `sources.json` | Keyword variants and the source catalog |
| `raw/NNNNNN.body.gz`, `raw/NNNNNN.json` | Each HTTP response, gzipped, with the SHA-256 of the exact bytes, and request metadata without credentials. A Jev request body is saved once, in its Jev trace. |
| `jev/` | One trace per paid attempt (request, reservation, receipt, answers), one chunk record per document, and one `-cancelled.json` record per cancelled hedge |

- `retrieved.json` lists every document that each call fetched, in fetch order, as `{id, source_id, call}`.
- `omitted.json` lists documents cut before scoring: duplicate IDs or documents over `--max-documents`.
- All JSON files are compact.
- Run folders use owner-only permissions on Unix. Raw responses can contain private source content.
