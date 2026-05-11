# Misa Farcaster Automation

Misa Farcaster Automation is a local-first automation framework for building a safer autonomous Farcaster operator. It is designed to evaluate signals, draft useful replies or casts, and stop at a clear pre-publish audit boundary before any live action happens.

The project is intentionally dry-run by default. It does not publish casts, load signer credentials, persist API keys, start webhook servers, create cron jobs, or call external AI providers unless a separate production publisher and explicit authorization layer are added.

## Why This Exists

Autonomous social agents often fail in two ways: they post too much low-value content, or they hide risky live actions behind a single opaque "agent" step. This project separates the operator into auditable stages so each decision can be inspected before anything reaches a publisher.

The core idea is simple:

```text
observe signals
-> filter low-quality content
-> review whether Misa can add real value
-> draft a reply, quote, or cast
-> run safety checks
-> build a publish packet
-> audit limits, authorization, and rollback
-> hand off to an external publisher only if approved
```

## Current Status

This repository contains the local dry-run operator layer:

- Neynar read-only request planning and fixture ingest
- Farcaster webhook payload normalization
- signal digest and topic scoring
- local AI second-pass adapter for review simulation
- run-cycle decision logic for reply, cast, quote, or skip
- draft precheck and publish-packet generation
- send-audit checks for authorization, limits, duplicate sends, and rollback
- unit tests covering the full local safety loop

It is not a production bot and does not include a live x402 publisher, signer service, API keys, or deployment configuration.

## Architecture

```text
external scheduler or manual call
-> dry-run-cycle
-> Neynar read-only fetcher plan or fixture ingest
-> webhook ingest
-> normalized Farcaster events
-> signal digest
-> AI second-pass review packet
-> local guarded AI provider adapter
-> operator run-cycle
-> draft and precheck
-> publish packet
-> send-audit
-> blocked before live publish by default
```

The live production shape should keep the same boundary:

```text
Misa operator
-> validated publish packet
-> send-audit approval
-> external x402 publisher
-> Farcaster submit
-> outcome recording
-> 2h / 24h feedback sampling
-> daily learning review
```

## Safety Model

The operator is built around fail-closed defaults:

- no real Farcaster submission from the operator
- no API key loading or key writing in dry-run mode
- no signer loading inside the operator
- no webhook server startup
- no scheduler, cron, timer, or service creation
- no external AI call in the local provider adapter
- no raw private memory in public drafts
- no publish without send-audit approval
- rollback is part of the pre-publish checklist

Even if a local override tries to enable live publisher behavior, the dry-run cycle forces the publisher boundary closed.

## Repository Layout

```text
tools/
  misa_farcaster_autonomy.py

tests/
  test_misa_farcaster_autonomous_operator.py
```

The implementation uses only Python standard-library modules.

## Quick Start

Run the test suite:

```bash
python -m unittest tests.test_misa_farcaster_autonomous_operator
```

Initialize local non-secret state:

```bash
python tools/misa_farcaster_autonomy.py --state-root state/farcaster init-state --pretty
```

Run the full dry-run automation loop without writing state:

```bash
python tools/misa_farcaster_autonomy.py --state-root state/farcaster dry-run-cycle --no-state-write --pretty
```

Build a Neynar read-only request plan without network access:

```bash
python tools/misa_farcaster_autonomy.py --state-root state/farcaster neynar-fetch-plan --no-state-write --pretty
```

## Key Commands

```bash
# Run one event through the operator
python tools/misa_farcaster_autonomy.py run-event --event-file event.json --pretty

# Rank and process a batch of candidate events
python tools/misa_farcaster_autonomy.py run-cycle --events-file events.json --pretty

# Normalize a Neynar response fixture
python tools/misa_farcaster_autonomy.py ingest-neynar --payload-file neynar-response.json --build-digest --pretty

# Normalize a webhook payload
python tools/misa_farcaster_autonomy.py webhook-ingest --payload-file webhook.json --run-operator --pretty

# Build an AI second-pass review packet without calling an LLM
python tools/misa_farcaster_autonomy.py ai-review-packet --events-file events.json --pretty

# Run the local guarded AI provider adapter
python tools/misa_farcaster_autonomy.py ai-provider-dry-run --packet-file ai-review-packet.json --pretty

# Audit a publish packet before any external publisher can submit it
python tools/misa_farcaster_autonomy.py send-audit --packet-file publish-packet.json --pretty
```

## Production Integration Notes

A production deployment should add these pieces outside this repository:

- a real read-only Neynar fetch worker
- a signed webhook receiver with signature verification
- a separately authorized AI review provider, if needed
- an external x402 publisher
- durable publish-result recording
- rollback controls for publisher failures
- monitoring for repeated sends, low-quality loops, and stale topics

The operator should remain packet-only. Live signing and submission should stay in a separate publisher service with its own credentials, rate limits, and rollback policy.

## Development

Run tests after any operator change:

```bash
python -m unittest tests.test_misa_farcaster_autonomous_operator
```

Run a syntax check:

```bash
python -m py_compile tools/misa_farcaster_autonomy.py tests/test_misa_farcaster_autonomous_operator.py
```

## License

No license has been selected yet. Until a license is added, all rights are reserved by the repository owner.
