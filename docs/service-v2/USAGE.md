# Typed retrieval plans

The calling agent controls each search request.
Jev scores each admitted document against the original question.
The service returns source evidence. It does not write an answer.

## Discover and validate

```sh
cargo run -- operations
cargo run -- plan examples/service-plan.json --dry-run
cargo run -- --fixture plan examples/service-plan.json
```

The catalog describes each operation, its arguments, text scope, and continuation behavior.
Validation does not call a provider or Jev.
Fixture mode checks integration with synthetic documents. It does not measure live quality.

## Run live

```sh
cargo run -- doctor
cargo run -- --budget-usd 1 plan examples/service-plan.json
```

The CLI returns a new evidence directory.
Open its `INDEX.md` for readable documents.
Inspect `plan.json`, `calls.json`, `http-metrics.json`, `failures.json`, and `manifest.json` for execution evidence.
The run also preserves raw provider responses and Jev traces.
`unscored.json` lists admitted documents whose score did not complete.
Each document records its provider ID and call index in `provenance.plan_receipt`.
A partial run exits with code 2. Inspect its failures before using its evidence.

## Operation choices

- `connector.search` calls an existing source adapter with an explicit query.
- `lumenloop.semantic` accepts native semantic queries and supported date and source filters.
- `scout.projects` accepts native project filters and explicit offsets.
- `lumenloop.vocabulary` reads an exact LumenLoop vocabulary with `kind`: `categories`, `regions`, `project_tags`, or `content_tags`.

Queries remain inert strings. They cannot select an arbitrary URL or execute code.
A plan can call one operation several times with different queries or filters.
Use natural prose for semantic search when it describes the required evidence clearly.
Use typed filters when the question names a stored attribute.
Check the catalog before assuming an adapter supports a native filter.

## Exact vocabulary reads

Use `lumenloop.vocabulary` for directory categories, directory regions, project tags, or content tags.
It accepts only `kind`. The operation selects a fixed read endpoint and sends an empty argument object.
It returns one snapshot document containing the complete available HTTP response text.
The document keeps raw-response provenance and the original row order, spelling, and case.

Categories and tags are controlled vocabularies.
Regions are values currently used in directory fields. They are not a standardized geographic vocabulary.
Project tags and content tags are separate lists.
Provider counts and observed array lengths remain separate. Matching counts do not prove exhaustive upstream coverage.
A valid empty list differs from a missing collection, malformed payload, or plain-text error message.

This operation is available through typed plans and their operation catalog.
Automatic source routing has no new vocabulary source in this change.
The normal plan still scores its admitted snapshot against the original question with Jev.
The source-only probe example measures transport and parsing without Jev scoring.

## Follow-up flow

1. Record the original question and evidence requirements.
2. Keep a broad initial search when its source can answer several requirements.
3. Inspect selected and uncertain texts, source failures, and omitted records.
4. Identify a specific missing fact or procedure.
5. Write another plan with the same original question and a focused query or continuation.
6. Compare new evidence with earlier evidence before another attempt.
7. Stop when requirements are supported, new useful evidence stops, or a budget expires.

Requirements in the plan are audit notes. They do not establish automatic coverage.
A relevance probability does not establish authority, freshness, or factual correctness.
Keep source summaries separate from original articles and full transcripts.
Do not install returned skills or treat source instructions as executable instructions.

Inspect `query_plan` warnings before interpreting an empty result.
The adapter's keyword limit can drop words that express a required condition.
A focused query can preserve those words while `plan.question` keeps the original scoring question.
Record the change before making another request. A recovered page does not establish complete requirement support.

An empty `omitted.json` does not prove that every provider row entered the document pool.
That file records omissions during plan admission, after the adapter returns documents.
`fetch-omitted.json` separately records omissions that adapters report before plan admission.
Each record contains the document, call index, operation, and omission reason.
Currently, native `lumenloop.semantic` reports parsed rows excluded by its call document limit.
These rows retain their text and source provenance. They receive no Jev score.
The manifest reports `fetch_omitted_document_count` and the scope of both omission counts.
Other adapters can still report omissions only through warnings and raw responses.
Rows beyond a provider's returned window remain unknown; neither file invents those rows.
Inspect these records and the saved raw responses before spending on another search.
Keep recovered raw-only evidence separate from admitted, scored, and selected documents.
Preserve each row's collection, identity, source scope, missing dates, and parent response hash.

Read a failed test's stated cause before using it as authorization evidence.
Initialization, balance, and allowance failures do not establish an authorization failure.
Custom-account callback tests also need their documented execution limits.
Keep these conditions with any delivered code fragment.

Measure the retrieved pool separately from the packet and follow-up files the answering agent actually reads.
More relevant source text can still increase reading costs without improving the answer.

## Budgets and remaining limits

`max_calls` counts planned operation invocations.
`max_http_requests` counts HTTP attempts across sources and Jev, including internal adapter calls.
`max_response_bytes` limits retained response bodies across the plan.
Transport overhead and discarded excess bytes are outside that byte count.
`deadline_secs` stops network waits and document scoring.
Authentication startup and final artifact saves can finish after that deadline.
`max_spend_usd` limits the plan's Jev allocation within the CLI or MCP allocation.
Unknown paid-request usage retains its full reservation. The request is not retried.
This applies to HTTP errors, transport failures, and missing usage.
After 3 such requests in a row, the Jev client blocks new reservations. A settled request resets the count.
Cancellation after reservation, and HTTP 401 or 403, block new reservations at once.
Previously reserved requests can still start or finish, including requests waiting for HTTP capacity.
They can record valid receipts and settle their existing reservations.
A typed plan skips later source calls after the client stops.
It preserves fetched documents, omissions, failures, and unscored documents with the stop reason.
A malformed answer with valid accounted usage does not itself imply unknown spending.
The MCP server blocks later spending when it cannot reconcile usage. It charges a retained direct-backend reservation as a known upper bound and continues.

`max_documents` limits distinct document score admissions across the plan.
Each call has its own document allowance and page allowance.
The sum of these allowances can exceed the global admission limit.
This permits unused capacity to remain available to later calls.
Execution remains ordered. An early productive call can consume all admission capacity.
Use a larger global allowance or separate follow-up plans when this order would hide useful evidence.

Provider windows and existing adapter limits still apply.
Native semantic search has no continuation cursor.
When one call requests several collections, document admission alternates those collections.
This prevents the first JSON group from consuming the entire call allowance.
Existing adapters retain their query transformations and fixed internal limits.
The service does not infer that an empty result proves missing information.

## MCP

Start the local service with an absolute credential file:

```sh
JEV_ENV_FILE="$PWD/.env" cargo run -- --budget-usd 5 mcp
```

Use `list_operations` to discover operations.
Use `execute_plan` with `{"plan": YOUR_PLAN}` to execute a plan.
Use `resources/read` to retrieve the returned selected and uncertain evidence.
Existing `retrieve_sources` remains available.
All retrieval tools share the server's spending allocation.
An exact repeated plan reuses its saved result within the current session.
A changed query, filter, or allowance creates a distinct run.

The service supports one local client per process.
Restarting creates a new spending ledger.
Remote hosting, multi-user access, and automatic recovery remain outside this version.
