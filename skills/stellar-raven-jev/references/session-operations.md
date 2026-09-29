# Saved sessions

Read the relevant command's `--help` before use. Take session and pool identifiers from the output.
Keep the same output directory when continuing a session.

## Check a claim

Use `check` when a claim depends on a summary, generated record, or a single row:

```sh
stellar-raven-jev check SESSION "CLAIM"
```

This command judges documents already in the session. It fetches no new sources, but its Jev calls cost money.
Inspect the supporting, contradicting, and qualifying text before changing the answer.
Check that a contradicting document addresses the same subject and scope.
Low or absent support means the claim remains unsupported by these documents.
The scores do not replace source reading or prove that a claim is false.

## Continue pending retrieval

Use `more` only when the session reports pending pools relevant to an observed gap:

```sh
stellar-raven-jev more SESSION --pool POOL_ID
```

It fetches or scores the pending pool against the original question and can spend money.
A default search normally leaves no pools. Do not use `more` as a general retry command.
Use one narrower `search` when the session has no relevant pending evidence.
Read the new text and assess it against the original question before declaring coverage.

## Inspect sources and usage

Use `sources` to discover source families and their retrieval scope.
Use `usage` to inspect recorded calls, cost, capacity signals, and disk use without network requests.
Source exclusions and retrieval failures limit coverage. Selected-document counts do not measure answer completeness.

## Preserve and replay evidence

Use `search --full-record` when an audit or replay needs raw responses and scoring traces.
Use `report RUN_DIR --variant NAME` to rebuild a saved run's report without retrieval or scoring.
The new report does not add evidence or refresh old facts.

Keep runs that a later review needs in a separate output directory. Set `--retain-days 0` when automatic pruning must preserve those runs.
Inspect `prune --dry-run` before an authorized cleanup. Do not remove evidence that an active review needs.
