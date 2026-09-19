# Project: Coordinated Amplification & Meme Evolution

HopHacks 2026, memetics track. Judged on polish, usefulness, creativity, and
technical skill.

> **This file was rewritten on 2026-09-19.** It previously described a planned
> pipeline (Stage 0–3, four parallel analysis tracks, UPGMA trees, a Detoxify
> toxicity channel) that the project did not end up building. That description
> conflicted with what actually shipped — the README explains, for instance, why
> the trees are deliberately *not* UPGMA — and with the approved consumer
> extension scope. `README.md` is the authority on the built system.

## What exists

An offline Python/DuckDB pipeline over a 395M-tweet Twitter firehose, and a
static dashboard that draws the result.

The method, in order: find accounts that repeatedly amplify the same content
within seconds of each other (coordination from behaviour alone, no keyword
seed list), then ask what those networks push, then trace how each narrative's
*wording* mutates across the month as a rooted descent tree whose nodes are
real tweets.

Read `README.md` before changing anything. It documents the corpus's real
shape, the stage contracts other code joins against, and the limits the whole
project is built around — the 2026-09-01 collection cliff, the absence of any
account metadata, and why "coordinated inauthentic amplification" is the
strongest claim the evidence supports.

Pipeline entry point is `./run.sh`. Stages are resumable.

## What is being built now

A consumer Chrome extension over the same evidence, on
`feature/consumer-extension`. Three approved features and nothing else:

1. Show measured **historical coordination evidence** beside visible X posts.
2. Show **earlier observed wording and source context**, with readable diffs.
3. Let users **mute repeated versions of a story**, reversibly.

Scope, architecture, API contracts, data availability and setup commands:
`docs/consumer-extension-plan.md`. Current state and verification status:
`docs/review-handoff.md`.

## Rules

`AGENTS.md` carries the binding rules for this repository — evidence scope,
what we may and may not attribute, identity handling, muting reversibility, and
repository hygiene. Read it before writing code. The ones that catch people out:

- Coordination evidence is **historical and narrative-level**, never a property
  of the account currently on screen.
- We cannot show "bot", "foreign", or a truth score. The corpus has no account
  metadata and never will.
- The tree root is the **earliest phrasing observed in this corpus**, not the
  origin of the idea.
- Ids are **strings**. Never join records on truncated display text.
- No bulk data and no secrets in Git.

## Data source

```
aws s3 cp --recursive --no-sign-request \
  s3://calcifer-hot/hopkins-hackathon-2026/twitter-firehose-last-month/ ./twitter-firehose/
```

396 zstd parquet shards, 55.7 GB, 377,271,528 distinct tweets, `created_at`
2026-08-17 → 2026-09-17. `./run.sh download` is the resumable way to get it.
Do not re-download it just to work on the extension — that work runs against
prebuilt artifacts or labelled fixtures.

## Scope discipline

Depth on a few case studies beats shallow coverage of everything. The dashboard
opens on one August-only prose lineage for exactly this reason.

## Accuracy note

Spot-check by hand anything shown in the live demo. A visible false positive in
front of judges is worse than cutting the slide. This applies with particular
force to the `burst` behaviour class, which is a shortlist and not a verdict —
most of what it catches is football transfer aggregators racing the same scoop.
