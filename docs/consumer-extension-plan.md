# Consumer extension — architecture, data, contracts, milestones

Status: setup milestone (Prompt 0). No feature code written yet.
Branch: `feature/consumer-extension`, based on `main` @ `c0535a9` ("V0.13 Node Physics").

## Approved scope

Three features, and nothing else:

1. **Coordination evidence** — while scrolling X, show measured historical
   coordination evidence beside a post whose wording matches a narrative in our
   corpus.
2. **Earlier context and wording changes** — a "See earlier context" inspector
   showing the closest supported historical wording, the earliest observed
   wording in that family, dates, available source links, and a readable diff.
3. **Mute repeated versions of a story** — user-initiated, reversible, expiring
   muting of near-duplicate posts.

Explicitly out of scope: bot scoring shown to users, country/actor attribution,
truth scoring, any paid API or LLM in the serving path, and any change to the
offline pipeline's analysis semantics.

## Current architecture (as inspected at `c0535a9`)

Offline Python/DuckDB pipeline, then a static dashboard. `run.sh` is the real
entry point; `package.json` only wraps it.

```
twitter-firehose/*.parquet          396 shards, ~52 GB raw (gitignored)
   │
   ├─ pipeline/01_normalize.py      -> data/normalized/{events,content,trajectory}/
   ├─ pipeline/01b_dedup.py         -> data/normalized/events_daily/dt=YYYY-MM-DD/
   ├─ pipeline/02_coordinate.py     -> data/graph/{edges,accounts,clusters}.parquet
   ├─ pipeline/04_evolution.py      -> data/evolution/{tokens,pairs,families,alias,
   │                                     variants,timeline}.parquet
   │                                   data/export/phylo/index.json
   │                                   data/export/phylo/trees-NN.json  (24 buckets)
   └─ pipeline/05_enrich.py         rewrites the export in place: restores text cut
                                    at 140 chars, attaches platform counts as node.met

dashboard/index.html                single file, no bundler; fetches phylo/index.json
                                    then one trees-NN.json on demand
```

Shared text normalization lives in `pipeline/textnorm.py` and is mirrored as a
DuckDB SQL expression in `04_evolution.norm_expr()`. **Both must stay in
parity** — the serving index has to normalize incoming tweet text exactly the
way the corpus was normalized, or nothing will match. Testing that parity is an
explicit Prompt 1 deliverable.

### Export shapes we have to serve from

`data/export/phylo/index.json`:

- `meta`: `t0` (corpus start, ISO), `n_families`, `n_variants`, `n_emissions`,
  `buckets` (24), `kinds`, `offlang`, `cliff_h`, `cliff_note`, `params`.
- `families[]`: `id`, `nv`, `emis`, `acc`, `co`, `coord_share`, `t0`, `t1`,
  `depth`, `drift`, `obf`, `sty`, `mix`, `kind`, `why`, `peak6`, `hshare`,
  `nonlatin`, `offlang`, `ht`, `topics`, `win`, `root` (**sliced to 180 chars**),
  `top` (**sliced to 180 chars**), `handles`, `q`.

`data/export/phylo/trees-NN.json`, keyed by `str(family_id)`, bucket =
`family_id % 24`:

- `nodes[]`: `i` (**family-local index**), `p` (parent index or null), `sim`,
  `txt` (**sliced to 280 chars at export**; `05_enrich.py` may later replace it
  with the full body), `n`, `acc`, `co`, `t0`/`t1` (**hours since `meta.t0`,
  not timestamps**), `sub`, `d`, `add`, `del`, `obf`, `mix`, `emo`, `drift`,
  `src` (= `rt_handle`, the *amplified* account), `spark`, and optionally `met`
  (`like`/`reply`/`rt`/`quote`/`views`/`save`).

### The identity problem, stated precisely

**The export carries no canonical variant key.** `04_evolution.step6_trees()`
writes `"i": i` and `"txt": r.text[:280]` but never writes `r.canonical` (the
`content_key`). So today there is no reliable way to go from a tree node back to
`variants.parquet`, and no way to go from a matched piece of text to a node
except by joining on truncated display text — which is exactly what Prompt 1
forbids, and rightly: `txt` is a 280-char slice, `root`/`top` are 180-char
slices, and `05_enrich.py` rewrites some of them afterwards.

Prompt 1 therefore has to **add `ck` (the canonical `content_key`) to every
exported node**, plus a dataset version identifier, and keep the existing
dashboard working unchanged. That is an additive export change; archived trees
are not rewritten in place (see AGENTS.md rule on versioning exports).

### Two hard capability limits found during inspection

These are not design choices, they are properties of the corpus, and the
features must be built around them rather than papering over them.

**1. There is no author-level join from a visible X post to our corpus.**
`data/normalized/events` carries `author_id` but the firehose has *no handle,
bio, follower count, account age or verified flag for the authoring account*
(README, "What the data actually is"). Handles are recoverable only for
*amplified* accounts, by regexing `RT @handle: ` out of a retweet body — that
is `rt_handle`/`node.src`, and it identifies **the account being retweeted, not
the retweeter**.

Consequence: given a tweet we scrape from the DOM (which gives us a handle), we
cannot look up that author's history. All coordination evidence we can show is
**narrative-level and historical**. Author-level evidence is `unknown`, and the
API must say so as a distinct field rather than leaving the client to infer it.

**2. There is no id-based provenance for a retweet's upstream source.**
Per `05_enrich.py`: the firehose has no `retweeted_status`, the embed column is
null throughout, and a retweet's `conversation_id` points at itself. Text is the
only available join to the original. So:

- A *sampled event* can carry a permalink of the form
  `https://x.com/i/status/{id}` (works without a handle).
- An *upstream source* of a retweet can only be identified by matching
  `rt_handle` + full text back to an `original`-kind event, the way
  `05_enrich.py` already does. Where that match fails, the source is unknown.
- These two are different claims and the UI must label them differently.
  Never synthesize a URL for a source we did not actually match.

### Other constraints carried forward

- **Collection cliff.** The crawl drops 15.3× on 2026-09-01 (22.0M tweets/day
  before, 1.4M/day after). Counts either side are not comparable. `meta.cliff_h`
  and `meta.cliff_note` exist and must survive into anything user-facing.
- **Corpus window.** `created_at` runs 2026-08-17 → 2026-09-17 14:32 UTC. Any
  post outside it is unassessable, not "clean".
- **Earliest observed ≠ origin.** The root is the earliest phrasing *in this
  corpus*. Anything older is invisible to us.
- **`coord_share` denominator.** `co`/`acc` at family level are **sums of
  per-variant accounts**, so an account active in three variants is counted
  three times. A family-level sum is not a unique-account count and must not be
  presented as one.

## Data availability

**This checkout contains no generated artifacts.** `data/` did not exist before
this milestone (importing `config.py` creates the empty directory tree). There
are no parquet files, no `data/export/phylo/`, and no firehose shards.
`.gitignore` excludes `twitter-firehose/`, `data/`, `*.parquet`, `dist/`,
`.dev/` — correctly, and that stays.

| Artifact | Needed for | Present |
|---|---|---|
| `data/evolution/variants.parquet` | full text + canonical ids for the serving index | **no** |
| `data/evolution/alias.parquet` | truncation fold-in mapping | **no** |
| `data/evolution/families.parquet` | content_key → family_id | **no** |
| `data/export/phylo/*.json` | family/node evidence, dashboard deep links | **no** |
| `data/normalized/events/*.parquet` | event ids, timestamps, permalinks | **no** |
| `data/graph/clusters.parquet` | coordination cluster membership | **no** |
| `twitter-firehose/*.parquet` | re-deriving any of the above | **no** |

**Action required from the team:** the teammate who ran the pipeline needs to
copy `data/evolution/variants.parquet`, `alias.parquet`, `families.parquet`,
`data/export/phylo/`, and ideally `data/normalized/events/` to the backend
machine, out of band. Keep them out of Git. Do **not** re-download the 52 GB
firehose to start; Prompt 1 is buildable against fixtures and only needs the
real artifacts for a real-data demonstration.

Until then Prompt 1 ships **fixture mode**: a small, clearly labelled synthetic
index, with `/health` reporting capability gaps so nothing fabricates a
measurement.

## Intended API contracts

Serving is read-only over an index built once at startup. No per-request DuckDB
scan of the firehose, no pipeline rebuild.

Cross-cutting rules:

- **All tweet and account ids are strings**, end to end. They exceed 2^53 and
  JSON numbers will silently corrupt them.
- Every response carries `dataset_version` so caches and deep links cannot mix
  exports.
- Match quality is reported as `exact | similar | ambiguous | none` plus the
  `method` that produced it. It is **not** a probability and must not be
  rendered as a confidence percentage.
- Evidence fields are separate from match fields, and "we did not measure this"
  is `unknown`, distinct from zero.

### `GET /health`

```jsonc
{
  "status": "ok",
  "dataset_version": "phylo-2026-09-19-c0535a9",   // stable id for this index
  "mode": "real" | "fixture",
  "corpus": {
    "start": "2026-08-17T00:00:00Z",
    "end":   "2026-09-17T14:32:00Z",
    "collection_cliff": "2026-09-01T00:00:00Z",
    "cliff_note": "Crawl volume drops 15.3x on 2026-09-01 ..."
  },
  "capabilities": {
    "narrative_match": true,
    "coordination_evidence": true,
    "earlier_context": true,
    "event_sources": false,      // true only when normalized events are loaded
    "author_level_evidence": false  // permanently false: no author handle in corpus
  },
  "index": { "families": 0, "variants": 0 }
}
```

### `POST /analyze/batch`

Request — a bounded batch (hard cap, rejected above it) of observations:

```jsonc
{
  "observations": [
    {
      "tweet_id": "1958...",          // string
      "text": "visible main text",
      "quote_text": "quoted post text, separately identified",  // optional
      "handle": "someaccount",         // optional, display only
      "author_id": null,               // almost always null; see capability limits
      "posted_at": "2026-08-22T14:03:00Z",   // optional
      "observed_at": "2026-09-19T15:00:00Z",
      "is_truncated": false
    }
  ]
}
```

Response — one result per observation, same order, ids echoed as strings:

```jsonc
{
  "dataset_version": "phylo-2026-09-19-c0535a9",
  "results": [
    {
      "tweet_id": "1958...",
      "match": {
        "status": "exact" | "similar" | "ambiguous" | "none",
        "method": "normalized_exact" | "token_candidate" | "none",
        "family_id": 2823,
        "canonical": "<content_key>",
        "node_index": 4,
        "score": 0.81,               // similarity, NOT a probability
        "candidates": 1
      },
      "coordination": {
        "status": "measured" | "unknown" | "not_applicable",
        "scope": "narrative_historical",
        "window": { "start": "...", "end": "..." },
        "n_accounts": 312,            // unique, per variant
        "n_coord_accounts": 41,
        "denominator": "distinct accounts amplifying this variant in corpus",
        "family_kind": "burst",
        "why": ["78% of spread in 6 hours", "..."],
        "author_level": "unknown"     // always, see capability limits
      },
      "earlier_context": {
        "status": "available" | "none",
        "earliest_observed_at": "2026-08-18T09:12:00Z",
        "earliest_text": "full text",
        "closest_text": "full text",
        "relationship": "closest_wording"   // never "ancestor"
      },
      "coverage": {
        "in_corpus_window": true,
        "crosses_collection_cliff": false
      }
    }
  ]
}
```

`status: "none"` and `coverage.in_corpus_window: false` are **different
answers** and the extension must render them differently: unmatched vs.
unassessable.

### `GET /families/{family_id}`

Everything the inspector needs for one family: node list with canonical keys,
full texts, absolute timestamps (resolved from `meta.t0` + hour offsets), parent
links with an explicit `parent_status` of `supported | unknown`, per-node
source records where we actually have them, and the family-level behaviour
class with its evidence strings. Rejects unknown ids rather than returning an
empty tree.

## Dependency separation

Two requirement sets, because a laptop serving the extension should not need
`leidenalg` or a 52 GB corpus.

- `requirements.txt` — full offline pipeline (unchanged): duckdb, pyarrow,
  pandas, scipy, python-igraph, leidenalg.
- `requirements-serve.txt` — **new**, lightweight serving only: fastapi,
  uvicorn, pydantic, plus pyarrow to read the prebuilt index. No leidenalg, no
  scipy, no igraph.

Verified on this machine (Python 3.14.2, linux x86_64) that wheels exist for
every package in both sets, including `leidenalg` (cp38-abi3) and
`python-igraph` (py3-none-any).

## Setup commands for this OS

This environment: **Linux (GitHub Codespaces), bash, Python 3.14.2, Node
v24.21.0, npm 11.19.0, git 2.55.0, gh 2.100.0.** No WSL involved. Teammates on
macOS or Windows/WSL should record their own variants below rather than assume
these.

```bash
# from the repo root
python3 -m venv .venv
source .venv/bin/activate

# serving only (what Prompts 1-4 need)
python3 -m pip install -r requirements-serve.txt

# full offline pipeline (only if re-deriving artifacts)
python3 -m pip install -r requirements.txt

# baseline checks that run without any data
python3 pipeline/textnorm.py            # normalizer self-test
python3 -m compileall -q pipeline config.py
./run.sh status                          # reports what artifacts exist

# dashboard, once data/export/phylo/ exists
./run.sh serve                           # http://localhost:8000
```

`.venv/` is already gitignored.

### Known environment blocker

`config.py` sets `TMP_DIR = "/private/tmp/claude-501/duckdb-spill"`, a macOS
path that does not exist on Linux. Any DuckDB stage that spills will fail here.
Left unchanged in this milestone so as not to break a teammate's working macOS
setup; the fix is to make it OS-aware (e.g. `tempfile.gettempdir()`) and belongs
in Prompt 1, where the serving path first touches DuckDB. Serving itself reads
prebuilt parquet via pyarrow and does not depend on it.

## Milestone checklist

- [x] **Prompt 0 — setup.** Inspect, branch, plan, AGENTS.md rules, handoff, PR.
- [ ] **Prompt 1 — shared evidence service.** Export `ck` + dataset version from
      the tree exporter; build the serving index; FastAPI `/health`,
      `/analyze/batch`, `/families/{id}`; normalization parity tests; fixture
      mode; latency benchmark.
- [ ] **Prompt 2 — coordination evidence in the browser.** MV3 extension,
      content script, service worker, badge, replay page.
- [ ] **Prompt 3 — earlier context inspector.** Sequence diffs, dashboard deep
      links with validated family/node + dataset version, source cards.
- [ ] **Prompt 4 — muting.** Deliberate, reversible, expiring, abstain-when-
      uncertain.
- [ ] **Prompt 5 — validation and demo.** CI on branch push and PR, evaluation
      pack, measured findings, 60–90s demo script.

## Verification stance

Every claim in `docs/review-handoff.md` is tagged with how it was established:
inspected, executed locally, run in CI, or unverified. Nothing gets reported as
tested because it looks correct.
