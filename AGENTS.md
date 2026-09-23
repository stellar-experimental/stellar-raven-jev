# Agent rules for stellar-raven-jev

## General purpose only

This tool answers any question about the Stellar ecosystem. It must stay general.

- Do not add code, flags, output fields, prompts, or tests that exist for one question or one
  narrow question type.
- Do not branch on question wording, named entities, or topics (for example "protocol",
  "USDT", "who is", "stablecoin") to change routing, retrieval, scoring, ranking, or output.
- Do not store facts that answer questions: version numbers, dates, names, roles, asset codes,
  or issuers. They go stale, and they hide retrieval failures.
- Keyword lists, synonym tables, and regular expressions that encode evaluation vocabulary are
  overfitting. A rule must hold for questions that nobody has written yet.
- Jev decides what a question needs through general questions asked of every question and
  every document. Code may compute generic facts from text (dates, version-like numbers,
  hosts), but must not decide which topics get special treatment.
- Source knowledge is allowed when it describes the source, not the question: connector API
  shapes, field meanings, source authority by host, a listing's own name, and words the source
  says are true of every row it returns.
- General language lists are allowed: grammar words and conversational request words in any
  language, and the corpus word "stellar". They must hold for any question. Words that can name
  a requested property (for example "live", "latest", "current", "experienced") never go in
  them.
- Tests use made-up names and questions. Do not copy evaluation questions into tests, fixtures,
  examples, or docs.

When an evaluation question fails, fix the general mechanism. Then check the fix on questions
that were written before anyone looked at the results. Do not tune on the test set.

## Other rules

- Forward only: no legacy fallbacks, compatibility flags, or history notes in code or docs.
- The tool is for agents: compact JSON by default, no human-oriented output modes.
- Before a commit, scan staged files for `.env` values, tokens, account IDs, emails, and local
  home paths.
