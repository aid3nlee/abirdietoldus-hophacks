# Review handoff

One entry per checkpoint, newest first. Every claim is tagged with how it was
established: **[inspected]** (read the code), **[executed]** (ran it here and
saw the output), **[CI]** (ran in GitHub Actions), **[manual]** (clicked through
a browser), or **[unverified]** (not checked — say so rather than implying it
works).

The final pushed SHA for each checkpoint lives in the PR description and the
message brought back to the orchestrator, not inside this file — a commit
cannot contain its own hash.

---

## Checkpoint 0 — setup, plan, rules, handoff

Branch `feature/consumer-extension`, based on `main` @ `c0535a9`.

### Completed behaviour

No feature code. This checkpoint establishes shared context only.

- **[executed]** Inspected the checkout: `pipeline/` (6 stages), `config.py`,
  `run.sh`, `dashboard/index.html` (1854 lines), `README.md`, git state and
  remotes.
- **[executed]** Verified `origin` → `github.com/aid3nlee/hophacks`, default
  branch `main`, repository private.
- **[executed]** Confirmed push access via `gh` as account `ughRural`
  (`permissions.push: true`).
- **[executed]** Found local `main` one commit **behind** `origin/main` and
  fast-forwarded before branching, so this work is based on the teammate's
  `c0535a9` "V0.13 Node Physics" rather than the stale `87d4a84`. That commit
  replaced `pipeline/05_untruncate.py` with `pipeline/05_enrich.py` and
  rewrote much of the dashboard; the plan reflects the new files, not the ZIP
  baseline.
- Added `docs/consumer-extension-plan.md` — architecture, export shapes, data
  availability, API contracts, dependency split, OS-specific setup, milestones.
- Added `AGENTS.md` — 18 binding rules (evidence scope, no bot/country
  attribution, earliest-observed framing, reversible muting, quote/rebuttal
  preservation, explicit missing evidence, string ids, no truncated-text joins,
  versioned exports, no secrets or bulk data in Git, honest verification).
  No pre-existing `AGENTS.md` existed, so nothing was overwritten.
- Added `requirements-serve.txt` — serving deps only, separate from the
  pipeline's `requirements.txt`.
- Rewrote `CLAUDE.md`, which described a superseded plan (UPGMA trees, a
  Detoxify toxicity track, four parallel analysis tracks) that contradicted both
  the shipped system and the approved scope.

### Verification commands and results

```
$ ./run.sh status
raw shards       0 / 396
normalized       0 / 0
graph            not built yet
phylogeny        not built yet
```
**[executed]** Passes. Correctly reports that no artifacts exist.

```
$ python3 pipeline/textnorm.py
confusable map: 1026 codepoints
'Тhеу аrе rерlасіng us 🔥🔥 https://t.co/abc' -> norm='they are replacing us' obf=10
```
**[executed]** Passes. The normalizer self-test is the only executable test in
the repository today.

```
$ python3 -m compileall -q pipeline config.py   # exit 0
```
**[executed]** Passes.

```
$ ./run.sh dist
no export yet - run ./run.sh evolution first
```
**[executed]** Expected failure — there is no data to build a site from.

**[executed]** Wheel availability on this machine (Python 3.14.2, linux
x86_64): duckdb 1.5.5, pyarrow 25.0.1, pandas 3.0.6, scipy 1.18.1,
python-igraph 1.0.0, leidenalg 0.12.0, fastapi 0.141.1, uvicorn 0.53.0,
pydantic 2.13.5 all resolve. No dependency in either requirement set is blocked
on this Python version.

**Not run:** the pipeline itself (`normalize`, `coordinate`, `evolution`,
`enrich`), `./run.sh serve`, and the dashboard in a browser — all require data
that does not exist in this checkout.

### Known gaps and blockers

1. **No data, at all.** `data/` did not exist. No parquet artifacts, no
   `data/export/phylo/`, no firehose shards. **This is the top blocker for a
   real-data demonstration.** A teammate must transfer `variants.parquet`,
   `alias.parquet`, `families.parquet`, `data/export/phylo/`, and ideally
   `data/normalized/events/` out of band. Prompt 1 is buildable against
   fixtures without them, but anything it reports would be synthetic.

2. **The export has no canonical variant key.** **[inspected]**
   `04_evolution.step6_trees()` writes `"i"` (family-local index) and
   `"txt": r.text[:280]` but never `r.canonical`. There is currently no safe
   path from a tree node back to `variants.parquet`, and joining on display text
   is unsound — `txt` is a 280-char slice, `root`/`top` are 180-char slices, and
   `05_enrich.py` rewrites some afterwards. Prompt 1 must add `ck` to every
   exported node plus a dataset version id, additively.

3. **No author-level join is possible, ever.** **[inspected]** The firehose
   carries no handle, bio, follower count or account age for the *authoring*
   account. `rt_handle` is the account being *retweeted*. So a tweet scraped
   from the DOM (which yields a handle) cannot be looked up. All coordination
   evidence is narrative-level and historical; author-level evidence is
   permanently `unknown`. The API surfaces this as a distinct field.

4. **No id-based provenance for a retweet's upstream source.**
   **[inspected]** Per `05_enrich.py`: no `retweeted_status`, embed column null
   throughout, retweet `conversation_id` self-points. Text is the only join.
   Sampled events can carry `https://x.com/i/status/{id}`; upstream sources can
   only be identified by text match, and must be labelled differently.

5. **`config.py` `TMP_DIR` is a macOS path.** **[executed]**
   `/private/tmp/claude-501/duckdb-spill` does not exist on Linux, so any DuckDB
   stage that spills fails here. Left unchanged to avoid breaking a teammate's
   macOS setup; should become OS-aware in Prompt 1. Does not affect serving,
   which reads parquet via pyarrow.

6. **Family-level `co`/`acc` are sums of per-variant accounts**, not unique
   family accounts. **[inspected]** An account active in three variants counts
   three times. Must not be presented as a unique-account figure.

7. **No CI, no test suite, no extension, no backend** exist yet. The only
   executable check is the `textnorm.py` self-test. Building out CI is Prompt 5.

8. **ChatGPT repository access is unresolved.** The repository is private and
   the plan document records a 404 on the GitHub plugin read as of 2026-09-19.
   Until that is fixed, review is manual: PR link plus SHA brought back to the
   orchestrator chat.

### Next milestone

**Prompt 1 — the shared evidence service.** Entry criteria: this checkpoint
reviewed and blockers resolved. First tasks in order:

1. Add `ck` (canonical `content_key`) and a `dataset_version` to the tree
   exporter, without breaking the existing dashboard.
2. Build the serving index from `variants.parquet` + `alias.parquet` + the
   phylo exports; load once at startup.
3. FastAPI `/health`, `/analyze/batch`, `/families/{id}` per the contracts in
   the plan.
4. Normalization parity tests against `pipeline/textnorm.py`.
5. Fixture mode with capability gaps reported, since there is no real data here.
6. Warm-lookup latency benchmark, with sample size and hardware recorded.

Open question for the orchestrator: **can a teammate supply the real artifacts
before Prompt 1 is reviewed?** If not, Prompt 1's sample responses will be
fixture-derived and must be read as such.
