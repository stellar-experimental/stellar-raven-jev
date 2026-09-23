# Local MCP transport

This server serves one local user through standard input and standard output.
It calls the existing retrieval pipeline or an explicit typed operation plan.
It returns source evidence and run records.
It does not generate answers.

The server supports MCP `2025-11-25`.
Clients send `initialize`, then `notifications/initialized`.
The server returns `2025-11-25` when the client requests another version.
Clients must check the returned version.
The `2026-07-28` discovery protocol is outside this implementation.
That revision removes the initialization handshake.
See the official [lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) and [revision changes](https://modelcontextprotocol.io/specification/2026-07-28/changelog).

## Start a session

The CLI starts the server with the `mcp` command.
Use fixture mode for an offline check:

```sh
cargo run -- mcp --fixture --budget-usd 0 --output-dir runs
```

A host can start the compiled binary:

```json
{
  "mcpServers": {
    "stellar-raven-jev": {
      "command": "/absolute/path/to/stellar-raven-jev",
      "args": ["mcp", "--fixture", "--budget-usd", "0", "--output-dir", "/absolute/path/to/runs"]
    }
  }
}
```

A live session requires configured Jev credentials and an explicit positive budget.
Supply credentials through the host environment.
MCP does not load `.env` from the working directory.
Set `JEV_ENV_FILE` to an absolute trusted file path when an explicit environment file is needed.
The server uses the same retrieval limits as the CLI.
Do not put credentials in tool arguments.
The server processes one call at a time.
EOF ends the session after the current call completes.
Cancellation notifications do not cancel a running call.
A repeated identical question reuses its earlier result without another paid retrieval.
Changing only the call budget does not refresh that result.
The response sets `reused: true` and includes the current remaining budget.
The server also reuses failed results.
Reword the question only when a new retrieval is intentional.
A session permits at most 64 distinct cached tool calls.
This includes retrieval questions, plans, and enabled experimental calls.
`execute_plan` caches the complete validated plan. Changed queries or allowances create a distinct run.
The server does not send progress notifications.
The pipeline applies its configured request timeouts.

## Protocol contract

Messages use UTF-8 JSON-RPC with one message per line.
Standard output contains protocol messages only.
The server rejects malformed JSON and oversized lines.
It skips blank lines.
Invalid requests echo a valid request identifier.
The server accepts additional top-level JSON-RPC fields.
It drains an oversized line before it reads the next message.
The input limit is 524,288 bytes, including the newline.
Batch requests are unsupported.
Notifications receive no responses.
A tool notification never starts retrieval or spends money.

Supported requests:

- `initialize`
- `ping`
- `tools/list`
- `tools/call`
- `resources/list`
- `resources/read`
- `resources/templates/list`, which returns an empty list

Unknown methods return `-32601` after initialization.
Invalid request parameters return `-32602`.
Unknown resources return `-32002`.
Tool execution failures return a tool result with `isError: true`.

The transport follows the official [stdio](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), and [resources](https://modelcontextprotocol.io/specification/2025-11-25/server/resources) contracts.

## Retrieval tool

`retrieve_sources` accepts these arguments:

```json
{"question":"How do Stellar smart contracts work?","budget_usd":0.5}
```

`question` is required and cannot contain only whitespace.
Its UTF-8 size cannot exceed 16,384 bytes.
`budget_usd` is optional.
No other arguments are accepted.
A typical live call settles at about $0.02 and scores up to 400 documents.
Each direct attempt reserves about $0.0029 until it settles; the 16 parallel scoring jobs therefore hold up to about $0.046 at once.
With a smaller call budget, scoring waits for reservations to settle instead of failing, so the call runs slower but completes.
One proxy request reserves about $0.0145 because the proxy can hide retries.
A call budget of $0.10 or more avoids waits for direct backends.
The caller cannot change the output directory or increase retrieval limits.

The result includes counts, selected evidence, uncertain evidence, gaps, failures, usage, and the remaining session budget.
Three resource links identify the manifest and the selected and uncertain evidence indexes.
Each index contains document resource links or links to index pages.
Follow every page link to obtain all document links.
Large documents use numbered part resources.
Each part is a complete JSON object with `part` and `parts` fields.
Join the `text` fields in part order to obtain the full document text.
The `run_id` matches the local run directory name.
Failure summaries include the stage and source identifier.
The complete failure messages remain in the local artifacts.
The result includes at most 32 failure summaries.

Document resources contain source identifiers, titles, public URLs, source text, and evidence status.
URL credentials and sensitive query parameters are removed from the resource projection.
Ordinary query parameters and section fragments remain available.
Relative URLs remain text and carry `url_relative: true`.
The sensitive-key filter matches the HTTP recorder: authorization, cookie, secret, token, password, api-key, api_key, apikey, and credential.
Raw HTTP responses, authentication records, and local file paths are not published as resources.
Complete provenance and raw references remain in the local run artifacts.
Source text remains data in resources.
The server never adds source text to its instructions or tool descriptions.

Fixture results contain synthetic evidence.
Fixture success does not prove source relevance or live model quality.

## Session budget

The CLI `--budget-usd` value covers the complete MCP process session.
It is not a fresh allocation for each call.
The session ceiling is $100.

Each call reserves its complete allocation before retrieval starts.
An omitted call budget reserves the remaining session allocation.
An explicit call budget cannot exceed the remaining allocation.
Known, finalized usage releases the unused allocation.
Zero remaining allocation blocks live retrieval.
Fixture calls can use a zero allocation.

The server compares `usage.json` with the returned pipeline usage.
It checks every Jev attempt record before it releases the allocation.
Incomplete attempts, a usage mismatch, and accounting errors block further session spending.
A pipeline error before finalization also blocks further spending.
The reserved allocation remains unavailable when usage is unknown.

With a direct backend (Cloudflare or TypeSafe), a finished attempt without a usage receipt keeps its full reservation.
That reservation covers the whole request, so the session charges it as a known upper bound and continues.
Upstream block pages return HTTP 402 on a small share of calls, so this keeps a session usable.
Within one call, the Jev client stops new attempts after 3 consecutive unresolved attempts.
The proxy hides possible upstream retries.
A proxy call therefore blocks further session spending, even when its response succeeds.
Restarting the process creates a new explicit session allocation.
Operators must account for previous sessions before they restart live work.

The server writes `session.json` at startup, before retrieval, and after usage settlement.
The ledger records the initial budget, remaining budget, pending allocation, block state, and run directory names.
The server synchronizes each ledger file before replacing the previous record.
A reservation write failure prevents retrieval.
A settlement write failure blocks further spending.
The ledger provides an audit record.
It does not restore a budget after restart or enforce a budget across processes.

## Resources and retention

Each process creates a private `mcp-<uuid>` directory below its configured output directory.
On Unix, the directory has mode `0700` at creation.
Each retrieval creates its own run directory below that session directory.
Resources use opaque `raven://` URIs.
A session registers only artifacts from runs that it creates.
A later session cannot read an earlier session's resources.

The server reads a fixed set of artifact names.
It rejects symbolic links and unexpected run directories.
Published resources use immutable memory snapshots.
Resource requests perform exact URI lookups.
They never convert caller input into filesystem paths.
This design assumes a trusted local user and filesystem.
It does not isolate the server from another process owned by the same user.

`resources/list` returns at most 32 resources per page.
Use its `nextCursor` value to request the next page.
Finish pagination before another retrieval changes the resource list.

A standard `resources/read` request with only `uri` returns a complete JSON resource.
Each published resource fits within 16,384 bytes.
Optional extension fields can read a smaller byte slice:

```json
{"uri":"raven://<session>/<resource>","offset":0,"length":16384}
```

Offsets and lengths count UTF-8 bytes.
The offset must be a character boundary.
The length must be between 1 and 16,384 bytes.
The server shortens the final boundary when a character spans the requested limit.
A length that cannot contain one complete character returns `-32602`.
An offset equal to the resource size returns an empty final slice.
An offset beyond the resource size returns `-32602`.

The response includes `_meta.raven` with the returned byte count, total size, and truncation flag.
A truncated response includes a `continuation` object.
Pass that object as the next `resources/read` request parameters.
Join the returned text slices before parsing the complete JSON resource.
Clients without extension support use ordinary resource reads and follow the document part links.

A resource cannot exceed 16 KiB.
The server accepts source artifact files up to 32 MiB.
The session stores at most 64 MiB across 20,000 resources.
Publication can fail when these limits are exceeded.
The completed local run remains available in that case.
The tool still returns the run identifier, counts, usage, and a publication error.
It also registers a reduced manifest when resource capacity permits.
The server does not delete local artifacts when it exits.
Operators control artifact retention through the filesystem.

This implementation does not provide HTTP service, remote authentication, or isolation between multiple users.
It is not a production or remote server release.

## Offline validation

```sh
cargo test --lib mcp::tests
```

Tests cover initialization, notifications, invalid parameters, budget settlement, budget depletion, uncertain usage, and fixture retrieval.
They also cover URI traversal attempts, symbolic links, UTF-8 boundaries, continuation records, and oversized input recovery.
Regression tests also cover repeated questions, ledger records, complete large-document reads, source URLs, and publication failures.
The tests make no paid Jev calls.

## Typed plans

`list_operations` returns operation schemas and provider limits without network activity.
`execute_plan` accepts `{"plan": YOUR_PLAN}` and returns the existing evidence resource format.
Both retrieval tools share the server spending ledger.
Read the [plan guide](service-v2/USAGE.md) and [example plan](../examples/service-plan.json).

## Experimental primary Markdown recovery

Start `mcp --primary-body` to enable two additional tools.
The default tool list stays unchanged.
This option requires evaluation before use as a default policy.

`list_primary_sources` lists eligible selected and uncertain documents already published in this session.
It returns at most eight entries per page and makes no network request.
Each entry preserves its selected or uncertain status.
The list follows stable document identity order, not relevance ranking.
Pass `next_offset` as `offset` to continue.
Finish pagination before another retrieval changes the source list.
The list includes GitHub Markdown URLs with `main`, `master`, or 40-character hexadecimal refs.
It does not verify source availability or resolve a branch to a commit.

`resolve_primary_body` accepts `source_uri`, `git_ref`, and `git_path`.
The URI must identify an original document part published by this session.
The explicit ref and path must match that document's original public GitHub URL.
The tool rejects caller URLs, local paths, redirects, and unsupported URL forms.
Other single-segment refs can work when the caller supplies them explicitly.
The tool preserves the original text, score, and source identity.
It stores the new body and its hash separately.
Recovered bodies remain explicitly unscored.
Their original evidence status describes the original excerpt only.
A mutable branch remains mutable after retrieval.

Each session permits eight recovery attempts.
Each attempt permits one client GET attempt, 1 MiB of retained body, and a 20-second asynchronous operation deadline.
The HTTP client disables redirects, retries, proxies, and content decompression.
DNS results must contain only approved public addresses and are pinned for the request.
An operating-system DNS operation can outlive asynchronous cancellation.
Local filesystem operations also lack a hard wall-clock bound.
Use an outer process deadline when a strict experiment deadline is required.

Recovery uses a separate HTTP allowance from retrieval plans.
It shares the 64-call limit, 64 MiB resource limit, and 20,000-resource limit.
It makes no Jev call and consumes no Jev allocation.
An unresolved session spending record blocks new recovery.
Exact repeats and document-part aliases reuse the same result, including failures.
They do not consume another recovery attempt.

The first result links to a section index and a manifest.
It does not deliver the complete body text.
Section resources preserve exact UTF-8 byte offsets, body hashes, and heading context.
ATX headings outside fenced code blocks provide navigation.
Setext headings do not create section boundaries.
Section parts can split procedures, tables, and lists.
Headings and parts do not certify independent claim support or complete procedures.
Read all required context before using an excerpt.

`document_complete` describes the acquired body.
`publication_status` separately reports successful or failed resource publication.
Local artifacts retain raw response bytes, parsed response headers, and failure records.
An incomplete response never becomes a complete document resource.
Fixture mode returns labeled synthetic text without DNS or HTTP activity.
Fixture text does not reproduce the original source.

## Experimental saved document pool

Enable `mcp --saved-pool` to inspect saved documents from the current session.
This flag is independent of `--primary-body`.
Use both flags to expose both tool pairs.
Without either flag, the default tool list and retrieval publication stay unchanged.
The saved-pool tools make no HTTP or Jev calls.
They can operate when further session spending is blocked.

Call `list_saved_sources` with `run_uri` equal to the retrieval result's `manifest` URI:

```json
{"run_uri":"raven://<session>/<manifest>"}
```

The result contains `sources`, `total_sources`, and `next_cursor`.
Pass `next_cursor` as `cursor` for the next page.
Each page contains at most eight rows and fits the existing 16 KiB resource limit.
Large metadata rows reduce the number of rows per page.
An individual row that cannot fit causes an explicit error.
Cursors bind to the run and the optional URL filter.

Use `same_url_as` to find saved companions of an already published document part:

```json
{"run_uri":"raven://<session>/<manifest>","same_url_as":"raven://<session>/<document-part>"}
```

The server requires a document part from that exact run and session.
It rejects metadata URIs, manifest URIs, empty original URLs, and unknown document URIs.
It compares exact original URLs before the public URL projection.
Different saved texts at the same URL remain separate rows.
The server does not replace an excerpt, change its status, or score another document.

The catalog contains rows from `selected.json`, `uncertain.json`, `rejected.json`, and `omitted.json`.
It does not reconstruct other fetched records or records with no saved body.
Rows use stable document identity order, followed by artifact order and row index.
This order is not a relevance ranking.
Duplicate document IDs retain separate `source_uri` values.
Each row includes its original document ID and source ID.
Titles, public URLs, and content scopes have explicit truncation flags.
Saved text sizes and hashes describe the exact saved UTF-8 text.
List rows contain only identity, title, public URL, scope, saved text size, text hash, and status fields.
Read a row's `source_uri` to inspect its complete audit metadata resource.
That resource also contains artifact, artifact hash, row index, record hash, and provenance hash.
The list states fixture labels, untrusted-content labels, and status explanations once per page.
The audit metadata resource retains these labels and explanations for each source.
Complete provenance and raw references remain local.

`admission_status` preserves the original file group.
`evidence_status` is `unscored` for omitted rows.
Rejected rows preserve the original relevance decision.
Rejection does not mean factual error.
An uncertain row can reflect a scoring failure.
No saved-pool read creates a new score.
Fixture metadata and text resources carry explicit fixture labels.

Call `open_saved_source` with a row's `source_uri`:

```json
{"source_uri":"raven://<session>/<saved-source-metadata>"}
```

The result links an `index` with the existing `documents` and `pages` structure.
Read its text parts in order.
Each part contains exact UTF-8 byte offsets, part numbers, and the original evidence status.
`saved_text_complete: true` means all saved text is published.
`remote_document_complete: null` means remote document completeness is unknown.
An empty saved text remains an empty saved text.
It does not become evidence of a complete remote response.

The server binds each catalog to its session, run directory, and exact artifact bytes.
New listing pages and first text publications validate those bindings.
Changed artifacts or unsafe paths cause an explicit error.
Text publication is atomic under the shared resource limits.
A failed first listing also removes its staged catalog resources and bindings.
Exact repeated calls reuse immutable snapshots, including cached failures.
A cached success remains the original published snapshot after a local artifact changes.
A different uncached request still validates the current artifact bytes.

Metadata, list pages, text parts, and indexes consume the existing session resource storage limits.
Distinct list pages and text opens share the existing 64-call cache limit with retrieval and recovery.
Repeated calls reuse the cache without another allocation.
Resource reads retain the existing per-read limit.
There is no cumulative delivered-byte limit or four-document reader limit.
This feature has no default ranking or answer-quality claim.
