# Configuration

This file lists the settings, the Jev providers, the flags, and the spending rules.
[`.env.example`](../.env.example) is a template with every variable.
`stellar-raven-jev --help` and `<command> --help` are the reference for flags and output fields.

## Where settings come from

The CLI loads one env file before it reads its options:

1. The file that `--env-file` names.
2. Else the file that `JEV_ENV_FILE` names.
3. Else a `.env` file in the working directory or a parent directory.

An explicit env file path must be absolute. Variables that the shell sets take precedence over the file.
An empty value in the file keeps the default.
Global flags work before or after the command. Command flags, such as `search --limit`, follow the command.

Some values cannot go in an HTTP header, for example a key with a line break.
Such a key, token, or gateway ID fails at startup. The error names the setting.

## Sources

| Source | Settings |
|---|---|
| LumenLoop | `LUMENLOOP_API_KEY` |
| Algolia, Stellar developer docs | `ALGOLIA_APPLICATION_ID_DOCS`, `ALGOLIA_API_KEY_DOCS` |
| Algolia, stellar.org | `ALGOLIA_APPLICATION_ID_SITE`, `ALGOLIA_API_KEY_SITE` |
| Stellar Scout (`stellarlight.xyz`) | None. `STELLAR_LIGHT_API_KEY` is an optional Stellar Scout partner key, sent as a bearer token. |

`doctor` shows which source settings are present and which are missing. It never prints values.

## Jev providers

Jev runs through one or more providers. Each configured provider joins a chain in this order:

| Provider | Settings | Endpoint and model | Accounted price per million input tokens |
|---|---|---|---|
| `cloudflare` | `CLOUDFLARE_ACCOUNT_ID`, and `CLOUDFLARE_API_TOKEN` or `JEV_CLOUDFLARE_AUTH_PROFILE`; optional `JEV_GATEWAY_ID` (default `default`) | Workers AI `typesafe/jev` | $0.0441 ($0.042 plus a 5% credit fee) |
| `typesafe` | `TYPESAFE_AI_API_KEY` | `https://api.typesafe.ai/v1/systemone`, `jev-latest` | $0.042 |
| `openrouter` | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1/systemone`, `~typesafe/jev-latest` | $0.0444 |

- `JEV_PROVIDERS` (comma-separated) sets another order or a subset. It must name only configured providers.
- All three serve the same model with the same request shape. Output tokens are free.
- `JEV_GATEWAY_ID` names the Cloudflare AI Gateway. The gateway's own settings decide how Cloudflare bills the request.
- `CLOUDFLARE_API_TOKEN` takes precedence over `JEV_CLOUDFLARE_AUTH_PROFILE`.
- With a Wrangler profile, each run gets its token from `wrangler auth token --profile NAME --json`.
  The token stays in memory. It must belong to `CLOUDFLARE_ACCOUNT_ID`. Otherwise every Jev call fails with HTTP 401.
- `doctor` shows the chain. `usage.provider_requests` in the output counts settled requests per provider.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `JEV_ENV_FILE` | none | Absolute path of the env file |
| `JEV_BUDGET_USD` | `0` | Jev spending limit for one call. Live calls need a value above 0 and at most 100. |
| `JEV_OUTPUT_DIR` | `runs` | Parent folder of the run folders |
| `JEV_HOST_DIR` | `OUTPUT_DIR/.host` | Folder of host-wide state. Searches that share it share this host's capacity. |
| `JEV_RETAIN_DAYS` | `7` | Run folders idle this many days are removed after a `search` or `more`. `0` turns this off. |
| `JEV_MAX_SEARCHES` | `6` | Searches that may run at once on this host |
| `JEV_SOURCE_SLOTS` | see [operating.md](operating.md#source-fetch-slots) | Questions that may fetch from one source host at once, as `HOST=N[,HOST=N]` |
| `JEV_PROVIDERS` | all configured, in chain order | Jev provider order or subset |
| `JEV_PROVIDER_RPM` | `cloudflare=400,typesafe=3600,openrouter=3600` | Jev requests per minute per provider on this host. A malformed value is an error. |

The source and provider credentials are in the tables above.

## Flags

Each of these flags works with every command:

| Flag | Variable | Default |
|---|---|---|
| `--env-file` | `JEV_ENV_FILE` | none |
| `--budget-usd` | `JEV_BUDGET_USD` | `0` |
| `--output-dir` | `JEV_OUTPUT_DIR` | `runs` |
| `--host-dir` | `JEV_HOST_DIR` | `OUTPUT_DIR/.host` |
| `--source-slots` | `JEV_SOURCE_SLOTS` | see above |
| `--retain-days` | `JEV_RETAIN_DAYS` | `7` |

Command flags such as `--limit`, `--json`, `--full-text`, `--full-record`, `--bundle`, and `--resources`
are in each command's `--help`.

### Evaluation flags

These flags are hidden from `--help`. Their defaults are the normal operating values.
Change them only to test the tool.

| Flag | Default | Meaning |
|---|---|---|
| `--fixture` | off | Offline run with fixed scores. It does not measure Jev. |
| `--timeout-secs` | `30` | Request time limit |
| `--concurrency` | `16` | Requests in flight per source host in one search |
| `--max-searches` | `6` | Same as `JEV_MAX_SEARCHES` |
| `--admission-wait-secs` | `60` | Wait for a search slot before `busy` |
| `--jev-concurrency` | `32` | Jev calls in flight in one search |
| `--jev-hedge-ms` | `2000` | Delay before a Jev hedge request. `0` turns Jev hedging off. |
| `--source-hedge-ms` | `0` | Delay before a source hedge request. `0` turns source hedging off. |
| `--jev-batch` | `1` | Chunks per scoring call |
| `--today` | today in UTC | Reference date (`YYYY-MM-DD`) for currentness |
| `--route-passes` | `2` | Routing passes |
| `--source-threshold` | `0.2` | Routing score that selects a source |
| `--fetch-threshold` | `0.2` | Routing score that fetches a source. Selected sources below it become pools. |
| `--score-depth` | `0` | Documents scored per source before the rest wait. `0` scores all. |
| `--document-threshold` | `0.4` | Score that selects a document |
| `--uncertain-threshold` | `0.15` | Score that marks a document uncertain |
| `--fetch-deadline-secs` | `10` | Time limit for the whole retrieval stage |
| `--max-pages` | `2` | Result pages requested from a paged source |
| `--max-documents` | `400` | Documents scored in one search |
| `--per-source-documents` | `12` | Most documents taken from one source |
| `--max-body-bytes` | `8388608` | Largest response body |
| `--original-reads` | `4` | Original pages read per call, at most 4. `0` reads none. |

`--fetch-threshold 0.4 --score-depth 4` is the lean first pass. It fetches and scores less at first,
and it leaves pools for `more`. See [operating.md](operating.md#sessions-and-pools).

## Spending

- `--budget-usd` is a hard limit for one call. A session can spend at most 3 times that value across its calls.
- Each Jev attempt reserves a worst case first: 65,536 input tokens at the highest provider price, about $0.003.
  When the attempt ends, the reservation settles to the reported cost.
- When attempts in flight fill the budget, the next attempt waits for one to settle.
  It fails at once only when no attempt is in flight.
- A search makes Jev calls for routing, for the time intent, for each document chunk, and for currentness.
  Routing asks about every source in each pass, in calls grouped by size. Most calls settle far below their reservation.
- `usage` in the output gives the settled cost, the requests, and the hedge requests.

## Provider failures

Before its first reservation, a call runs a free network check.
It sends one HEAD request, with no credentials and no body, to each provider's origin. No model runs.

- A provider is reachable when it answers with any HTTP status.
  It is also reachable when it connects within 5 seconds and has not answered after 6 seconds.
- A provider that fails the check is not used for the run. `load.jev_providers_skipped` names it.
- When every check fails, nothing is reserved. `search` fails with a `network_check` report.
  `more` and `check` exit with an error.

A call goes to the first provider in chain order that is usable. A usable provider is enabled, is not cooling, and has send budget on this host.

| Event | Result |
|---|---|
| HTTP 429 or 529 | Releases the reservation. Cools the provider for every search on the host, for its `Retry-After` (1 to 90 seconds). The call moves to the next provider. |
| HTTP 401 or 403 | With another provider in the chain: disables the provider for the run and releases the reservation. With no other provider: stops the client. |
| HTTP 402 | With another provider: cools the provider and releases the reservation. With no other provider: counts as an unresolved attempt. |
| Connection fails before the request is sent (DNS, TCP, proxy, TLS) | Releases the reservation. Cools the provider for 30 seconds. The call moves to a provider that is usable at once. |
| Request refused locally before the send | Releases the reservation. No provider cools. The call fails with the class `other`. |
| Other HTTP error, read timeout, reset, or incomplete body | Keeps the reservation as spent. Not retried, because the provider may have run the request. |

- An attempt that was sent and ends without a usage receipt is unresolved.
  After 3 unresolved attempts in a row, the client stops paid work for the run. A settled attempt resets the count.
- After the stop, a Jev call reserves nothing, and its item is not judged.
  Its report has the stage `not_assessed_after_stop`.
- When no provider is usable after a connection failure, the client runs the network check again.
  It tries after 1, 2, and 4 seconds, and starts no round after 10 seconds.
  If every round fails, later calls fail at once with the same cause class.
- When no provider has send budget, a call waits at most 120 seconds in total.
  `usage.provider_wait_ms` adds up these waits.
- Each failed Jev call has a `cause` class: `dns`, `connect_refused`, `connect_denied`, `unreachable`, `connect`,
  `tls`, `timeout`, `connection_closed`, `other`, `http_NNN`, or `invalid_response`.
  The class never holds a URL, header, credential, or body.
- A document whose scoring fails is `uncertain` in `classification.json` and has an entry in `failures.json`.

### Jev hedging

A Jev call with no answer 2 seconds after its request is sent gets one identical hedge request.
It does this only if the budget has room without waiting. Queue time does not count.

- The first valid answer wins, and the other request is cancelled.
- If one attempt fails, the other one decides.
- The cancelled request is charged the winner's input tokens at its own provider's price.
- A hedge never waits for a provider, and it prefers another provider than the first attempt.
- `usage.hedged_requests` counts the hedges.
