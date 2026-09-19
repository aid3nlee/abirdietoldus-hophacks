# Coordinated Amplification & Meme Evolution — HopHacks 2026, Memetics Track

Finding bot networks in 395M tweets from behaviour alone, then tracing how the
narratives they push mutate over a month.

## The idea in one paragraph

We do **not** start by deciding what counts as hate speech and searching for it.
We start by finding accounts that amplify the same content within seconds of each
other, repeatedly, over a month — a pattern organic users do not produce. Only
*then* do we ask what those networks are pushing. Finding the network first and
reading its content second keeps our assumptions out of the detector, and it
means the findings can surprise us. The memetics payload is stage 4: within a
confirmed network, how does a narrative mutate, and which mutations spread?

## What the data actually is

Source: `s3://calcifer-hot/hopkins-hackathon-2026/twitter-firehose-last-month/`
396 zstd parquet shards, 55.7 GB, **395,352,258 rows / 377,271,528 distinct
tweets**, `created_at` 2026-08-17 00:00 → 2026-09-17 14:32 UTC. Verified
byte-exact against the S3 listing; all 396 shards open as valid parquet.

> The dataset README says "~363.5M distinct". That figure is ~3.6% low and looks
> like a `approx_count_distinct` (HyperLogLog) estimate. The exact count, from an
> exhaustive per-day pass, is **377,271,528**. We hit the identical wrong number
> ourselves before checking it exactly — worth knowing if you quote the README.

### ⚠️ The collection collapses on 2026-09-01 — read this before planning anything

The corpus is **not** a uniform month. Crawl volume drops off a cliff:

| Period | Days | Tweets | Share | Per day |
|---|---:|---:|---:|---:|
| Aug 16–31 | 16 | 352,700,927 | **93.5%** | 22.0M |
| Sep 01–17 | 17 | 24,569,633 | **6.5%** | 1.4M |

**August is 15.3× denser per day than September.** August days run 22–29M
tweets; September days run 0.8–2.5M. This is a collection artifact, not a change
in how much the world tweeted.

Consequences we have to design around:

- **Raw time series across the whole window are meaningless.** Any "this
  narrative grew/declined" chart spanning 1 September will be measuring our
  crawler, not the discourse. Normalise to a per-day *rate*, or don't cross the
  boundary.
- **Coordination detection is much weaker in September.** Co-retweet evidence
  scales with volume; 1.4M tweets/day yields far fewer co-events, so clusters
  will look like they "die" on 1 September when they have merely gone unobserved.
- **The honest framing is two windows, not one month.** Either run the memetic
  evolution story inside Aug 17–31 (15 dense days, still plenty for mutation —
  variants turn over in hours), or present August and September as separate
  samples and compare rates rather than counts.

A judge who plots tweets-per-day will find this in thirty seconds. Far better
that it is our slide than their question.

Things we learned the hard way — read before you design anything:

- **It is an amplification feed, not a conversation feed.** 62–73% retweets,
  26% originals, 9% quotes, and only **1.4–2.4% replies**. Any plan built around
  reply/comment analysis is fighting the data.
- **`source`, `poll` and `embed` are 100% NULL.** The client-app field — normally
  the single best automation tell — is unusable.
- **There is no account metadata at all.** No handle, bio, follower count,
  account age, or verified flag for the *authoring* account. Every profile-based
  bot heuristic is off the table.
- **But handles are recoverable for amplified accounts.** Retweet bodies are
  literally `RT @handle: <text>`, so regex reconstructs a retweeter → target
  graph. This is the richest structure in the dataset.
- **Retweet bodies truncate at exactly 140 chars** (~36% of them). Truncation is
  deterministic, so every retweeter of one tweet yields a byte-identical string —
  which is what makes hashing the text a valid key for the original tweet.
- **Shards are time-ordered, ~77 min of firehose each.** Grabbing "a few shards"
  gives you three hours of one day, not a sample. Any subset must stratify
  across all 396.
- `author_id` is roughly monotonic with signup date. It is the only account-age
  signal available, and it is crude — treat it as suggestive, never as proof.
- **Shard-local dedup is globally correct.** Shards partition cleanly on
  `created_at`, so every observation of a tweet lands in one shard. Stage 1b
  verifies this each run and has measured **zero** cross-shard duplicates.
- 412 rows have a NULL `created_at`; stage 1b drops them.

## Pipeline

```
twitter-firehose/*.parquet          52 GB raw
        │
        ├─ 01_normalize.py          ~5 min, fully parallel
        │     data/normalized/events/       one row per tweet, no text
        │     data/normalized/content/      deduped text by content_key
        │     data/normalized/trajectory/   engagement curves
        │
        ├─ 01b_dedup.py             ~2 min, repartition by day
        │     data/normalized/events_daily/dt=YYYY-MM-DD/
        │     (much faster time-sliced queries; also asserts dedup correctness)
        │
        ├─ 02_coordinate.py         ~1 min on 112M tweets
        │     data/graph/edges.parquet       u, v, n_coevents, jaccard
        │     data/graph/accounts.parquet    per-account behaviour features
        │     data/graph/clusters.parquet    author_id → cluster_id
        │
        ├─ 03_label.py              Claude API, cluster exemplars only
        │     data/labels/clusters_labeled.json
        │
        ├─ 04_evolution.py          ~20 min end to end, six cached steps
        │     data/normalized/content_global.parquet   deduped English content
        │     data/evolution/tokens.parquet            tokens + evasion features
        │     data/evolution/pairs.parquet             u, v, jaccard
        │     data/evolution/families.parquet          content_key -> family_id
        │     data/evolution/variants.parquet          spread, timing, coordination
        │     data/export/phylo/index.json             family index
        │     data/export/phylo/trees-NN.json          mutation trees, 24 buckets
        │
        └─ dashboard/               reads data/export/, published as an Artifact
```

### Stage contracts

These file schemas are the interfaces between people. Agree on changes before
making them; code against the schema, not against someone's branch.

| File | Key columns |
|---|---|
| `normalized/events/*.parquet` | `id, author_id, created_at, version, lang, kind, rt_handle, content_key, reply_to_status_id, reply_to_user_id, conversation_id, quoting_id, is_truncated, like_count, retweet_count, reply_count, quote_count, views_count` |
| `normalized/content/*.parquet` | `content_key, kind, rt_handle, text, lang, n_copies, first_seen` |
| `normalized/trajectory/*.parquet` | `id, version, like_count, retweet_count, views_count` |
| `graph/edges.parquet` | `u, v, n_coevents, jaccard` |
| `graph/accounts.parquet` | `author_id, n_tweets, n_retweets, n_originals, n_replies, n_targets, first_seen, last_seen, author_id_num, retweet_ratio, target_diversity, active_hours` |
| `graph/clusters.parquet` | `author_id, cluster_id` |
| `evolution/tokens.parquet` | `content_key, kind, rt_handle, text, norm, tokens, n_tokens, n_copies, first_seen, obf_chars, n_emoji, n_hashtags, is_truncated` |
| `evolution/variants.parquet` | `canonical, family_id, n_emissions, n_accounts, n_coord_accounts, n_clusters, clusters, first_seen, last_seen, text, norm, tokens, kind, rt_handle, obf_chars, n_emoji, n_hashtags` |

`kind` ∈ `retweet | reply | quote | original`.
`content_key` = `md5(rt_handle || '|' || text)` — identifies the *upstream*
tweet, so it joins a retweet to everyone else who retweeted the same thing.

## Method: co-retweet coordination

Two accounts get an edge when they retweet the same content within 60 seconds.
Edge weight is how many distinct pieces of content they co-amplified; `jaccard`
normalises that by each account's total activity, so a pair of low-volume
accounts moving in lockstep outranks two firehose-scale accounts that overlap by
chance.

The naive version is O(n²) inside each content group and explodes on viral
tweets — a single tweet with 500k retweets would emit 10¹¹ pairs. Instead we
bucket retweets into time windows and pair only within a bucket, using two
bucketings offset by half a window so a pair straddling a boundary is still
caught. Cost becomes linear in volume rather than quadratic in virality.

Communities come from Leiden (`leidenalg`), with connected components as a
fallback if the optional dependency is missing.

## Method: meme phylogenetics

Stage 4 asks the memetics question directly: a narrative is not a fixed string,
it is a lineage. A phrasing appears, spreads, gets reworded, and the rewordings
compete. That is variation plus differential reproduction, which is all
"evolution" ever means.

**Finding variants without comparing everything to everything.** 54.9M distinct
English messages is 10^15 pairs. Instead each message is indexed under its four
rarest tokens and only messages sharing one are compared. The justification is
specific rather than merely cheap: a mutation preserves the distinctive
vocabulary — that is what makes it recognisable as the same narrative — and it
is the connective words that get swapped. Rare tokens are the part of a message
that survives rewording, so they are the right blocking key. A pair is scored
only if it shares two blocking tokens, or one rare enough that coincidence is
implausible. Exact token-set Jaccard on the survivors, threshold 0.45.

**Truncation is not mutation.** Retweet bodies are cut at 140 chars, so ~36%
arrive pre-mutilated. A cut copy is folded into the full text it was cut from
before families are formed, otherwise the platform's truncation would show up as
a mutation the network never made.

**Rooting and descent.** Within a family, each variant attaches to the earlier
variant it most resembles. Time gives the edges direction — a phrasing cannot
descend from one that did not exist yet — and similarity picks which ancestor.
This is deliberately *not* UPGMA: a UPGMA dendrogram encodes similarity, puts
every real variant at a leaf, and invents internal nodes that nobody ever
posted. We want a descent tree whose internal nodes are real tweets, so the root
is a phrasing you can read and the edges are diffs you can show.

**The collection cliff is drawn, not hidden.** 34% of families (47% of traced
retweets) cross 1 September, where the crawl thins 15.3x. Stage 4 tags every
family `aug` / `span` / `sep`, the dashboard draws the seam as a dashed rule on
both the tree and the emission curve, a lineage that crosses it carries a
warning, and the rail filters to August-only for a clean sample. The map opens
on an August-only lineage so the first thing a judge sees is not a compromised
one.

**Obfuscation is measured, not discarded.** Normalization folds 1,026
confusable codepoints — Cyrillic `а`, math-bold `𝐚`, fullwidth `ａ` — back to
ASCII, and counts how many it had to fold. `Тhеу аrе rерlасіng us` normalizes to
`they are replacing us` with an evasion count of 10. Caveat found in the data:
most high-obfuscation messages are K-pop fandom styling, not filter evasion, so
the count is a lead rather than a verdict.

### Validation (396 shards, 31 days, English)

```
54,851,440 distinct English messages
 6,569,230 traceable (>=2 copies, >=5 content tokens), 65.2M emissions
   436,335 usable blocking tokens -> 10.3M candidate pairs
   524,865 pairs above Jaccard 0.45
    24,438 families / 133,094 variants
     4,004 families exported (>=3 variants, >=150 emissions), 40,412 variants
```

Families split cleanly into two kinds, and the hashtag-per-variant number
separates them:

- **Prose narratives** (≈0 hashtags/variant) — a claim reworded by people.
  Lineage 2823 is the clearest: `"Outstanding Refugee" winner Salman Elmi was
  just arrested…` mutates over five generations — `arrested`→`charged`, `Somali`
  added, `Minnesota` added, 🚨 BREAKING framing bolted on — and the deepest
  descendant still pulls ~1,000 retweets. That is selection, visible.
- **Promo templates** (2+ hashtags/variant) — the hashtag block is the conserved
  region and the free text is the variable region. Fandom campaigns, film PR.
  Real mutation by the same mechanism, and they dominate by volume, which is why
  the dashboard opens on a prose narrative instead.

Cross-checking against stage 2 is what makes the coordination column worth
having. The families with the highest coordinated-account share are not
political at all — they are engagement-farming rings (`Drop your X handles
let's gain massively`, `Repost & drop a comment. Follow who reposts`), several
of them written in math-bold unicode, which the obfuscation counter picks up
independently. Two different signals agreeing on the same accounts is the
result worth showing.

### Validation so far (112M tweets, 9 days, English only)

28.5M retweet events → 1.09M active accounts → 26,882 edges → **304 clusters
covering 6,582 accounts**, in 18 seconds. Spot-checking the tightest ones:

| Cluster | n | jaccard | density | What it is |
|---|---|---|---|---|
| 112 | 9 | 0.504 | 0.75 | Generated handles (`@AartiGuptafuq5`, `@sukreetiSisxlz`) — astroturf |
| 132 | 8 | 0.647 | 0.64 | Forex/gold pump (`@goldtrade99`, `@xauusd_awii`) |
| 120 | 9 | 0.455 | 0.72 | Telugu film PR |
| 140 | 8 | 0.435 | 0.50 | Indian political |
| 31 | 28 | 0.373 | 0.60 | US MAGA-adjacent |

The method separates operation *types* without being told what to look for,
which is the result the whole project rests on.

## Method: behaviour classes

Topic says what a lineage argues. It says nothing about whether the spread was
authentic, and conflating the two is the easiest way to mislead a reader. The
most "notable" lineages in this corpus by every structural measure are Thai and
Korean entertainment promotions: deeply reworded, heavily amplified, organised
— and neither covert nor political.

So behaviour is classified on its own axis, from signals the pipeline already
computes, and every family carries the evidence for its call.

| Class | n | Rule | What it means |
|---|---|---|---|
| `farm` | 19 | ≥4 solicitation terms surviving into several wordings | Giveaways, airdrops, follow-to-win. The payload *is* the instruction. A confident call. |
| `evade` | 0 | ≥0.15 mixed-script words per wording | A lookalike character inside an otherwise Latin word. **None found** — see below. |
| `promo` | 847 | ≥1.5 hashtags per wording, or a lower load from one dominant source | Conserved hashtag block, variable free text. Coordinated by construction, but disclosed and commercial. |
| `burst` | 16 | ≥75% of spread in 6h, ≥10% coordinated accounts, ≥250 accounts, no source above 50% | Near-identical wordings from *several* upstream accounts at once. A shortlist, not a verdict. |
| `organic` | 3,122 | none of the above | Absence of evidence only. |

Two of these deserve elaboration.

**`burst` is a shortlist, not an accusation.** Hand-inspected, most of what it
catches is football transfer aggregators racing the same scoop — structurally
identical to a press campaign and entirely legitimate. It is worth keeping
because it also surfaces the two Nigerian state-politics press campaigns in the
corpus (30+ rewordings, 250–350 accounts, dispersed sources), which nothing
else in the pipeline was finding. The UI says "worth a look" and never more.

Source dispersion is what makes the class mean anything. An earlier version
flagged NCT and ATEEZ fan posts as coordinated: one official account retweeted
several thousand times is fast, and travels through a dense mutually-following
community, so it passes a burst-plus-coordination test easily. Requiring the
near-identical text to originate from *several* accounts separates "one post
went viral" from "many accounts published the same thing at once".

**`evade` is empty, and that is the finding.** Across 4,004 lineages, exactly
two contain any mixed-script word at all and both are false positives. There is
no homoglyph-based filter evasion in this corpus. An earlier counter reported
105 evasion families, but it was counting every character the normalizer folds
— and the top seven by frequency were the horizontal ellipsis (11,816
occurrences, appended by Twitter itself when it truncates a retweet), the curly
apostrophe, the em dash and smart quotes. It was ranking "was this tweet cut
off". Styled characters (math-bold and similar) *are* common and *do* defeat a
naive keyword match, but inspection shows they are decorative — headline
emphasis and idol promo — so they are reported per-wording and never charged as
evasion.

## Running it

```bash
python3 -m pip install duckdb pyarrow pandas python-igraph leidenalg scipy

# one-time, ~25 min
mkdir -p twitter-firehose && seq -f "%06g" 0 395 | xargs -P 10 -I{} sh -c \
  'curl -sf -o "twitter-firehose/tweets-{}.parquet" \
   "https://calcifer-hot.s3.us-east-2.amazonaws.com/hopkins-hackathon-2026/twitter-firehose-last-month/tweets-{}.parquet"'

python3 pipeline/01_normalize.py --workers 5
python3 pipeline/02_coordinate.py --lang en --window 60 --min-coevents 3
python3 pipeline/04_evolution.py            # ~20 min, six cached steps
```

Every stage is resumable — each skips work already on disk. Stage 4 caches per
step, so `--steps 3,4,5,6` re-runs only the clustering and export after a
threshold change, and `--force` rebuilds a step whose inputs moved underneath
it. **Re-run stage 4 with `--force` after normalizing more shards**: step 1
caches a global content table, and it will happily keep serving a table built
from a partial corpus.

The dashboard in `dashboard/index.html` reads `data/export/phylo/` and is
published as an Artifact. It fetches `phylo/index.json` plus one tree bucket on
demand, so whatever hosts it needs both the page and that directory beside it.

```bash
npm run dev       # live dev server on :8000  (= ./run.sh serve)
npm run build     # -> dist/  (= ./run.sh dist)
```

There is no bundler and no Node dependency — `package.json` exists only so the
usual two commands do the usual two things. Both shell out to `run.sh`, which
is the real entry point:

```bash
./run.sh serve    # .dev/ symlinks the page and the export, so an edit to
                  # dashboard/index.html is live on refresh
./run.sh dist     # real copies -> dist/index.html + dist/phylo/*.json (~20 MB)
```

`dist/` is the whole site: one HTML file and the JSON it fetches. No bundler, no
server, no API — anything that serves static files will host it unchanged.

### The look

A typed lab sheet with the tree drawn on it. There are no cards, shadows or
rounded corners anywhere; 1px rules and whitespace do the separating. Courier
Prime sets everything typed, and `authenticfont.ttf` — a Calligraphr font made
from real handwriting — sets the page title and, on the diagram, the marginalia
only: the hour labels, the word "root", the counts under the heaviest circles,
the way a pencil annotates a printout. It is inlined as a base64 data URI (18.5 KB) because the
artifact CSP serves fonts from Google alone, and because `dist/` has to carry it
too. That face has 85 glyphs and no em dash, `*` or `×`, so every string handed
to it is written with the punctuation it actually draws. Tree edges carry a small
deterministic bow so they read as drawn rather than plotted; node circles stay
true, because their area encodes spread and a wobbly circle would lie about it.

Two inks (`#1c1c1a`, `#5a5a54`) on `#f3f3ef` notebook stock, one accent
(`#20477c` ballpoint blue) and one warning (`#a4382c` red pencil, reserved for
coordination and the collection cliff). The metric ramp is a five-step blue ink
wash — monotonic in lightness, and every node label flips between white and ink
by the fill's own luminance so it clears 4.5:1 at every step in both themes.

### Two layouts, because they answer different questions

The default is the **classic rooted tree**: a child sits below the wording it
was derived from and parents are centred over their children, so going down the
board is going down the generations. It is the layout that shows *structure* —
the branching factor, which wordings are siblings, how deep a lineage actually
runs — and horizontal position deliberately carries no measure. Nodes are
lettered breadth-first, so A is the root and the letters read down the tree.
There are no numbered level rules: a row index is not a measurement, and ruling
one across the board gave the depth an authority the other axis never earned.
Depth is still on the page — in the panel, in the table column, and in the
shape of the tree itself.

A **time layout** sits beside it in the same control. There a variant sits at
the hour it was first observed, so time runs down the board and the vertical
gap along an edge is the real waiting time before the rewording appeared. The
1 September collection cliff is a dashed horizontal seam. Structure gets harder
to read; timing becomes exact.
The reading note beside the diagram changes with the layout, because a sentence
about "the dashed line" is wrong in a view that has no time axis.

### The board is a map, and the tree is sprung

The diagram is not refitted into the window every time you touch it. It is a
fixed window onto a canvas with no edges: drag the paper to pan, scroll or
pinch to zoom about the pointer from 2% to 5000%, double-click to zoom in, and
`fit` to come back to the whole lineage. A dot grid drawn in screen space and
re-tiled by zoom decade gives the panning something to move against and keeps
the dots the same size at every scale. Opening a lineage floors the automatic
fit at 34%, because the widest lineage in the corpus is 226 wordings and fitted
to a laptop that is a field of specks; the zoom readout says so, and `fit`
pressed on purpose still fits.

Every circle is a mass on a spring anchored at the position the layout gave it,
tied to its parent and children by more springs and solid enough not to sit on
a neighbour. Pull one aside to read what is underneath it and its relatives
follow; let go and the whole arrangement eases back to the measured one, which
is the point — the physics is a way of handling the tree, never a way of
changing it. Overlap repulsion only fires when two circles actually touch, so a
settled tree keeps exactly the spacing the tidy layout produced, and the
simulation parks itself the moment nothing is moving rather than burning a core
redrawing a still picture. Switching layouts keeps the circles where they are
and lets the springs carry them to the new arrangement, which is also the
clearest available answer to what the switch changed. Under
`prefers-reduced-motion` there is no simulation at all: a dragged circle still
follows the pointer and snaps home on release.

Selecting a variant shades its subtree and rings its parent, the inspector
turns the tree's own vocabulary (parent / child / sibling) into buttons that
move the selection, and arrow keys walk the same relations. Under the diagram a
table carries every value the colour ramp and node area encode, so nothing on
the page is readable only as a colour.

Tuning lives in `config.py`. The levers that matter most: `CORETWEET_WINDOW_S`
(tighter = higher precision, fewer clusters), `MIN_COEVENTS` (raise to cut
noise), and `PRIMARY_LANG`.

## Honest limitations — say these before a judge finds them

- **Really 15 dense days, not one month.** See the collection collapse above.
  "Evolution" means mutation across Aug 17–31; September is a thin sample we can
  compare rates against but cannot trend through. The `gov-tweets` corpus in the
  same bucket reaches back to 1999 *and* carries profile history, if we want
  genuine longitudinal depth.
- **Coordination is not intent.** A film studio's PR push and a hate network are
  both coordinated. The separation is stage 3's job, and it is a judgement call
  we should publish rather than hide.
- **We cannot prove "bot".** We can show coordination. Claiming automation
  without account metadata is a claim we cannot support — so we will not make it.
  "Coordinated inauthentic amplification" is what the evidence actually supports.
- **English-first biases the sample** toward Anglophone and Indian networks.
- **The 60s window is a parameter, not a fact.** Report sensitivity across a
  couple of values rather than presenting one setting as ground truth.
- **The root is the earliest phrasing *in this corpus*.** Not patient zero for
  the idea. The corpus starts 2026-08-17; anything older is invisible to us, and
  a narrative that entered mid-month will be rooted at whatever we saw first.
- **Blocking trades recall for tractability.** Messages whose every token is
  either unique or extremely common get no blocking key and cannot join a
  family. Roughly a third of traceable messages fall in that gap.
- **Topic tags are a keyword lexicon, not a classifier.** They exist to make
  thousands of families navigable. Shown as a heuristic in the UI, never used
  as a finding. Two gates keep the worst misfires out: a term must survive into
  several of a family's wordings (a lineage is a set of rewordings of one
  claim, so a term that is part of the claim gets reworded with it), and the
  surviving terms must include an unambiguous anchor. Before those gates, one
  tweet reading "I woke up to god" filed all 226 wordings of a cosmetics
  campaign under identity politics, and `white` plus `black` in a description
  of an outfit scored exactly as high as `antisemitic` plus `sharia`. Coverage
  is 502 of 4,004 families; the untagged remainder is mostly entertainment the
  lexicon does not cover.
- **`lang` is Twitter's guess and it is wrong in one direction.** Hashtag-heavy
  Thai, Korean and Japanese posts get tagged English because their Latin-script
  hashtags outweigh the body; 11% of variants in the "English" set carried
  non-Latin script. Tokenisation keeps only `[a-z0-9']`, so those lineages were
  built from their hashtag blocks rather than from a reworded sentence. Stage 1
  now drops the clear cases and the export labels the borderline ones
  (`offlang`, 52 families), filterable in the UI.
- **No toxicity score yet.** The headline tree currently colours by drift,
  spread or coordination. Hate/toxicity scoring is the missing fourth channel
  and slots into the same control.

## Ethics

Findings are about *networks*, not individuals. We do not publish a list
accusing named private accounts of being bots. Cluster-level claims, handles
shown only where the account is already a large public broadcaster, and every
threshold disclosed so a reader can disagree with us.
