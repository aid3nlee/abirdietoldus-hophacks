# Project: Phylogenetics of Inauthenticity — narrative evolution on Twitter

HopHacks memetics-track project. Judged on polish, usefulness, creativity, and
technical skill.

## One-line pitch

Traces how ideas mutate as they spread: a phrasing appears, gets reworded, the
rewordings compete, and some win. Built from a month of Twitter firehose. It
also flags coordinated and spam-like amplification, but that is a side channel,
not the headline.

**This is an evolution-of-ideas project first.** The inauthenticity detection is
a column on the tree, not the thesis. An earlier version of this file described
the reverse; if something in the repo still argues bot-detection-first, it is
stale and should be fixed rather than followed.

## What the data actually is

`s3://calcifer-hot/hopkins-hackathon-2026/twitter-firehose-last-month/`
396 zstd parquet shards, 52 GB on disk, **395,352,258 rows / 377,270,972
distinct tweets**, `created_at` 2026-08-17 → 2026-09-17 UTC.

```
aws s3 cp --recursive --no-sign-request s3://calcifer-hot/hopkins-hackathon-2026/twitter-firehose-last-month/ ./twitter-firehose/
```

Facts that shape every design decision — verified, not assumed:

- **The crawl collapses on 2026-09-01.** Aug 16–31 is 352.7M tweets (22.0M/day);
  Sep 01–17 is 24.6M (1.4M/day). A 15.3× cliff. Any time series crossing it
  measures the crawler, not the discourse. Normalise to rates or stay one side.
- **It is an amplification feed, not a conversation feed.** 62–73% retweets,
  ~26% originals, only 1.4–2.4% replies. This is good for us: retweeting is
  literal replication, so mutation-during-copying is real Darwinian structure.
- **No account metadata at all** — no handle, bio, follower count, or age for
  the authoring account. Every profile-based bot heuristic is off the table.
  `source`, `poll` and `embed` are 100% NULL.
- **Handles are recoverable for amplified accounts.** Retweet bodies are
  literally `RT @handle: <text>`, so regex reconstructs a retweeter → target
  graph. Richest structure in the dataset.
- **Retweet bodies truncate at exactly 140 chars** (~36%). Deterministic, so
  every retweeter of one tweet yields a byte-identical string — which is what
  makes hashing text a valid key for the upstream tweet.
- Shards are time-ordered (~77 min each). "A few shards" is three hours of one
  day, not a sample. Any subset must stratify across all 396.

## Language scope — know this before quoting a corpus size

Analysis currently runs on **English only**: 138,130,463 of 377,270,972 tweets
(36.6%). That filter is a leftover from the propaganda-hunting era, not a
technical necessity, and it is the single biggest lever on project scope:

| Bucket | Tweets | Status |
|---|---:|---|
| English | 138.1M | in scope |
| Latin-script (es/pt/fr/it/de/tr/id/tl) | 79.4M | **tokenises correctly today** — one predicate change |
| CJK / Thai / Arabic / Indic | 130.6M | produces **zero** tokens; needs char n-grams |
| Cyrillic | 1.15M | **actively broken** — must be denied explicitly |
| no-language / media codes | 26.6M | not useful |

Two traps if anyone widens it:

- **Cyrillic is mangled, not merely missed.** The confusable folder that catches
  `Тhеу аrе rерlасіng us` turns genuine Russian into junk ASCII — `Они заменяют
  население` becomes `['ameh','eh','hace','hom','ka','kpy','oh','om']`. Unrelated
  Russian tweets share that junk and would form **false lineages**.
- **Japanese is 24.7% of the corpus** and currently invisible, because
  `[^a-z0-9']+` strips it entirely. Claiming to trace how ideas mutate while
  dropping a quarter of the data is a question you do not want from a judge.

The raw data for every language is already on disk in
`data/normalized/content/` — the filter is applied at stage 4 step 1, so
widening needs no re-download and no re-normalise.

## Pipeline (what actually exists)

```
twitter-firehose/*.parquet              52 GB raw, 396 shards
  ├─ 01_normalize.py    ~5 min   events / content / trajectory
  ├─ 01b_dedup.py       ~2 min   repartition by day; asserts dedup correctness
  ├─ 02_coordinate.py   ~1 min   co-retweet graph → Leiden clusters
  ├─ 03_label.py        ~3 s     lineage keywords + topics  ← labels the export
  ├─ 04_evolution.py    ~20 min  six cached steps → variant families → trees
  ├─ 05_enrich.py                puts back cut text + engagement counts
  ├─ 06_comments.py     ~6 min   replies/quotes → stance + polarity
  └─ 07_deadends.py     ~6 min   once-only rewordings that never spread
dashboard/  reads data/export/, published as an Artifact
```

Stages 03, 05, 06 and 07 are **additive post-passes**: they rewrite or extend an
export that already exists and never touch the clustering, distances or trees.
That is deliberate — it means a failed re-run cannot take the demo down. Keep
new work in that shape where possible.

`04_evolution.py --steps N --force` re-runs one step; steps 1–5 are cached on
disk, so rebuilding just the export (step 6) is cheap.

## Method, in brief

**Finding variants without comparing everything to everything.** 54.9M distinct
English messages is 10^15 pairs. Each message is indexed under its four rarest
tokens; only messages sharing one are compared. The justification is specific,
not merely cheap: a mutation preserves the distinctive vocabulary — that is what
makes it recognisable as the same narrative — and it is the connective words
that get swapped. Exact token-set Jaccard on survivors, threshold 0.45.

**Truncation is not mutation.** A 140-char cut copy is folded into the full text
it was cut from before families form, or the platform's truncation would show up
as a mutation the network never made.

**Rooting and descent.** Within a family each variant attaches to the earlier
variant it most resembles. Time gives edges direction; similarity picks the
ancestor. This is deliberately **not UPGMA** — a UPGMA dendrogram puts every
real variant at a leaf and invents internal nodes nobody posted. We want a
descent tree whose internal nodes are real tweets you can read.

**Dead ends (stage 7).** `MIN_COPIES = 2` in stage 4 means a wording emitted
once is invisible — and 47.4M English messages were emitted exactly once. Under
an evolution framing those are *mutations that failed to reproduce*, so a tree
without them has survivorship bias designed in. Stage 7 attaches them, scoped to
a few hundred showcase lineages. Scoping is measured, not squeamish: blocking
against all 10,674 lineages reduces nothing (45.4M of 47.4M match *something*),
because with ten thousand diverse lineages almost any English sentence matches
one. Restricting to globally rare tokens collapses it to 2.9M candidates.

**Behaviour classes are a side channel.** `farm / evade / promo / burst /
organic`. Only `farm` is a confident call; `burst` is explicitly a shortlist,
not a verdict — most of what it catches in this corpus is football transfer
aggregators racing the same scoop, which is entirely legitimate. Do not let the
UI or a slide upgrade it to an accusation.

## Scope discipline

Don't phylogenize the whole corpus. Pick 3–5 narratives and build them
properly — depth on a few case studies beats shallow coverage. This applies to
stage 7 too, which is scoped by design for exactly this reason.

## If time runs short, cut in this order

1. Widening the language filter (biggest win, biggest risk — needs the Cyrillic
   deny-list and, for CJK, a new tokenizer)
2. Account role classification (depends on data that mostly is not there)
3. Behaviour classes down to a badge rather than a filterable facet
4. **Protect:** the mutation tree, the dead-ends layer, and the lineage labels —
   most demoable, least dependent on anything outside our control

## Accuracy notes

- Spot-check by hand anything shown live. A visible false positive in front of
  judges is worse than cutting the slide.
- Topic labels come from a curated lexicon and cover ~32% of lineages; the
  per-lineage **keywords** are derived and cover 100%. Prefer keywords when
  answering "what is this about".
- Most high-obfuscation messages are K-pop fandom styling, not filter evasion.
  The obfuscation counter is a lead, never a verdict.
