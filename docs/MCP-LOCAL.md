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
A session permits at most 64 distinct retrieval questions or plans.
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
The `0.5` example supports initial reservations but does not guarantee complete retrieval.
Four concurrent direct requests need about $0.0116 in available reservations.
One proxy request reserves about $0.0145 because the proxy can hide retries.
The actual run can require more budget.
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
Missing receipts, incomplete attempts, and accounting errors block further session spending.
A pipeline error before finalization also blocks further spending.
The reserved allocation remains unavailable when usage is unknown.

The proxy hides possible upstream retries.
A proxy call therefore blocks further session spending, even when its response succeeds.
Direct provider receipts can permit another call within the remaining budget.
A retained error reservation bounds cost but does not prove finalized provider usage.
An HTTP error therefore blocks later spending, even when a subsequent retry succeeds.
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

