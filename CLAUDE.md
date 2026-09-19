# Project: Phylogenetics of Inauthenticity — Inauthenticity Pipeline

HopHacks memetics-track project. Judged on polish, usefulness, creativity, and technical skill.

## One-line pitch
Measures how much of a topic/hashtag's Twitter buzz is authentic vs. bot-driven or coordinated, and — as of this pivot — tracks how propaganda/hate narratives mutate and spread through that inauthentic layer over time ("phylogenetics of inauthenticity").

## Data source
Twitter firehose, ~495M tweets, last month:
```
aws s3 cp --recursive --no-sign-request s3://calcifer-hot/hopkins-hackathon-2026/twitter-firehose-last-month/ ./twitter-firehose/
```
Do not process the full 495M tweets directly in any single stage — see Stage 0 below.

## Architecture

Four analysis tracks share three pieces of infrastructure. Build the shared infra once; each track reads from it rather than reimplementing its own version.

### Stage 0 — Data reduction (blocking, do first)
Filter the firehose down to a workable subset before anything else runs.
- Seed list: known slurs/dogwhistles, or a handful of flashpoint hashtags/topics from the target month
- Tooling: DuckDB or Polars — not pandas, at this scale
- Output: a filtered subset (target: well under 1M tweets) that every downstream stage reads from

### Stage 1 — Shared infrastructure (build once, feeds all four tracks)
1. **Near-duplicate / paraphrase clustering** — MinHash + LSH for surface-level dedup at scale; embeddings (sentence-transformers, e.g. MiniLM) only within candidate clusters for looser paraphrase matching. Output: variant clusters with timestamps.
2. **Account behavioral features → bot score** — posting cadence, vocabulary entropy, template reuse rate, follower/following ratio if available. Output: a bot-likelihood score per account.
3. **Toxicity / hate scoring per tweet** — Detoxify or a HateBERT-style classifier; don't hand-label training data in a hackathon window. Output: a toxicity score per tweet.
4. **Interaction graph (conditional)** — retweet/quote/reply edges, IF the dataset has this field. Verify early — see Open questions.

### Stage 2 — Analysis tracks (each reads two of the three shared outputs)

| Track | Reads from | Produces |
|---|---|---|
| Mutation tree ("meme phylogenetics") | variant clusters + timestamps | distance matrix → UPGMA/hierarchical clustering → tree structure, rooted at earliest instance |
| CIB (coordinated inauthentic behavior) detection | variant clusters + account features | flagged bursts: near-identical text from many distinct accounts in a tight time window, inconsistent with organic diffusion curves |
| Account role classification | account features + toxicity scores + interaction graph | per-account role: originator / amplifier / "launderer" (rephrases toxic content into more acceptable language) |
| Authenticity vs. toxicity overlay | account features (bot score) + toxicity scores | time series: bot-driven engagement vs. toxicity intensity per hashtag/topic |

### Stage 3 — Unified outputs (the actual demo)
1. **Phylogenetic tree** — the headline visual. Nodes = narrative variant clusters, x-axis = time, color = toxicity score, node size = engagement/bot-amplification, CIB-flagged bursts annotated directly on the tree. Rendering: D3, or `ete3`/`toytree` in Python.
2. **Authenticity timeline** — companion chart, topic-level, showing the authenticity-vs-toxicity overlay for whichever narrative the tree is built from.

Don't build these as four separate demos. The tree should visually encode outputs from all three shared-infra pieces plus the CIB flags — one coherent tree + one companion chart reads stronger to judges than four disconnected panels.

## Scope discipline
Don't try to phylogenize the whole filtered corpus. Pick 3–5 specific narratives already flagged as suspicious by the authenticity meter, and build a detailed tree for each. Depth on a few case studies beats shallow coverage of everything.

## Open questions to resolve early
- **Does the firehose schema include retweet/quote/reply fields?** Account role classification depends entirely on this. If absent, either cut that track or redefine "amplification" using timestamp-ordered near-duplicate phrasing as a weak proxy for the missing edge.
- Who owns which Stage 2 track — the four tracks parallelize cleanly across teammates once Stage 1 is done.

## Build order
1. Stage 0 — blocking, do first
2. Stage 1, all pieces in parallel — highest-leverage phase, everything downstream depends on it
3. Stage 2 tracks — parallelizable across teammates once Stage 1 outputs exist
4. Stage 3 unification — needs at least the mutation tree plus one other track's output to be meaningful

## If time runs short, cut in this order
1. Account role classification (most dependent on data that may not exist)
2. Reduce CIB detection from a standalone feature to an annotation layer on the tree
3. Protect: mutation tree + authenticity-vs-toxicity overlay — most demoable, least dependent on things outside our control

## Accuracy note
Toxicity/hate classifiers conflate actual hate speech with condemnation of it and reclaimed language. Spot-check any "top toxic cluster" shown in the live demo by hand before presenting it — a visible false positive in front of judges is worse than cutting that slide.
