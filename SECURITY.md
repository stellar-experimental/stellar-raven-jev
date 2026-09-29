# Security policy

stellar-raven-jev reads public Stellar sources with the user's own API keys and sends source text to a Jev provider.
Report security problems privately.

## Report a vulnerability

Use GitHub private vulnerability reporting:
[open a private report](https://github.com/stellar-experimental/stellar-raven-jev/security/advisories/new).
Do not open a public issue for a vulnerability.

Include these items:

- the commit or `Cargo.toml` version you built;
- the command and the steps that show the problem;
- the effect, for example exposed credentials, a request to a host the tool must not reach, or spending above the budget.

Never send API keys or `.env` files.

## Scope

In scope: the `stellar-raven-jev` binary and the agent skill in this repository.

Out of scope: LumenLoop, Algolia, Stellar Scout, Cloudflare, TypeSafe, OpenRouter, and the pages the tool reads.
Report problems in those services to their operators.

This repository is not part of the Stellar Development Foundation bug bounty program.

## Supported versions

stellar-raven-jev is before version 1.0. Only the latest commit on `main` receives fixes.
