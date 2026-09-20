"""
Stage 6 — comment ecology: what the audience said back, and how it clusters.

The tree in stage 4 is a lineage of *claims*. This stage builds the second
half of the picture: the lineage of *responses* to those claims. A narrative
does not travel into silence -- it travels into a reply space that answers it,
contradicts it, ridicules it, or farms engagement off it, and that response
space has structure of its own.

Three things this stage establishes, in order of how much they mattered:

  1. QUOTES ARE THE COMMENT LAYER, NOT REPLIES. The obvious read of this
     corpus is that comments are replies, and replies are 1.4-2.4% of it --
     which is where the project first concluded that reply analysis fights
     the data. That conclusion is right about replies and wrong about
     comments. A quote tweet is commentary on another tweet with its own
     body, and `quoting_id` is a clean edge to the tweet it answers. There
     are 39.6M quotes against 10.1M replies, and against the wordings in the
     export they land 36x better:

         replies attached to variant wordings     17,925 over  9,922 wordings
         quotes  attached to variant wordings    ~646,000 over 49,521 wordings

     Both are carried here. `ctype` keeps them apart so a chart never adds a
     quote to a reply without saying so.

  2. THE BRIDGE IS TEXT, NOT content_key. A variant's content_key is
     md5(rt_handle || '|' || text), which identifies the *retweet*. People
     reply to and quote the *original*, whose key is md5('' || '|' || text)
     and therefore different. Joining comments on content_key was measured
     and returns 2,516 -- an eighth of what text matching returns. So this
     reuses stage 5's prefix-bucket resolution, with one fix: stage 5 takes
     arg_max to a single tweet per wording, and the firehose stores several
     `version` rows per tweet, so the ids have to be deduped or every count
     downstream is inflated.

  3. CLUSTERS SPAN LINEAGES. Comment clusters are built once, globally, over
     every comment attached to any wording -- not per node and not per family.
     That is the point rather than an optimisation: a response template that
     shows up under one narrative is a crowd, and the same template under
     fourteen unrelated narratives is an operation. `nfam` on a cluster is
     what separates those two, and it is not a number a per-node clustering
     could ever produce.

Each comment carries a stance (endorse / dispute / mock / question / promo)
and a VADER polarity -- see pipeline/stance.py for why both, and for what
the lexicon cannot do.

Outputs:
  data/comments/replies.parquet     deduped reply rows from the firehose
  data/comments/quotes.parquet      deduped quote rows from the firehose
  data/comments/linked.parquet      comment -> variant wording, scored, clustered
  data/export/phylo/comments.json   the cross-lineage cluster catalogue
  data/export/phylo/trees-*.json    rewritten: each node gains a `cmt` block
  data/export/phylo/index.json      rewritten: each family gains a `cmt` block

Run:  python3 pipeline/06_comments.py [--steps 1,2,3,4,5] [--force]
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C
from pipeline.stance import STANCES, TONES, score
from pipeline.textnorm import normalize

CMT = C.DATA / "comments"
CMT.mkdir(parents=True, exist_ok=True)
PHYLO = C.EXPORT / "phylo"
FIREHOSE = C.ROOT / "twitter-firehose" / "*.parquet"

CUT = "…"
KEY = 40          # prefix bucket, same as stage 5
MAX_TEXT = 560
MIN_BODY = 12     # a comment shorter than this says nothing clusterable

# --- clustering knobs -------------------------------------------------------
# Deliberately stricter than the variant clustering in stage 4. A variant
# family wants to catch a reworded claim; a comment cluster wants to catch a
# reused *template*, and a loose threshold here merges every short angry reply
# into one blob.
# MIN_TOKENS was 4 and that was wrong: at four content tokens the clustering
# merges generic internet reactions -- "genuinely what the actual fuck is
# happening" -- and because everyone posts those everywhere they turned up
# under forty unrelated lineages and scored as the most cross-lineage
# "templates" in the corpus. Raising it to 7 fixed that and broke something
# else, cutting short but genuinely scripted entries ("Metawin ID: __ #skel"
# is four tokens). Five is the compromise: the distinctiveness gate below is
# the filter that is actually supposed to reject common speech, and a blunt
# length floor should not be doing that job for it.
MIN_TOKENS = 5
SIM = 0.60
BLOCK_KEYS = 3
MAX_DF = 3000     # a token this common is not a template marker
MAX_BLOCK = 300   # pairs compared inside one block
MIN_CLUSTER = 3   # a template needs three emissions to be a template

# Distinctiveness gate. A cluster of common words repeated by many accounts is
# a coincidence of vocabulary; a cluster of *rare* words repeated by many
# accounts is a script. GENERIC_DF is the mean corpus frequency above which a
# cluster's shared vocabulary is treated as common speech.
#
# Calibrated against the clusters rather than picked: the first value, 900,
# sat almost exactly on the median and marked 69% of clusters generic --
# including the 246-account gambling spam and the 230-account @grok chain
# prompt, which are the clearest scripted templates in the corpus. Measured,
# real templates land at mdf 450-3400 and common speech at 4700-6000
# ("people dyin who ain't never died before", "What God cannot do does not
# exist"), so the boundary belongs at 4000.
GENERIC_DF = 4000

# What the flag claims, and what it does not. The observation is that many
# distinct accounts posted near-identical distinctive text; calling that
# coordination is an inference this data cannot settle, so the field is named
# for the observation and the UI says "worth a look" the way the behaviour
# classes already do. Reach and burst are reported alongside it rather than
# folded into it: a 246-account template confined to one lineage and spread
# over days is still a template, and an earlier rule that required either
# cross-lineage reach or a tight burst threw exactly that case away.
REUSE_MIN_ACC = 25

SHOW = 4          # sample comments kept per cluster
TOP_CLUSTERS = 6  # clusters attached per node / per family
# Every cluster, not a top slice. A node's `cl` list points at cluster ids,
# and an id the catalogue omits renders as nothing at all -- at 400 that
# silently dropped 3,614 references. The whole catalogue costs about a
# megabyte, which is less than one tree bucket.
CATALOGUE = 100000


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute(f"SET threads={C.THREADS}")
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


# ---------------------------------------------------------------------------
# step 1 -- pull the two comment types out of the firehose
# ---------------------------------------------------------------------------
def step1_extract(con, force: bool) -> None:
    """Replies and quotes, deduped to one row per tweet.

    The firehose re-observes a tweet as its counters move, so a tweet appears
    once per `version`. Without the row_number filter every later join
    multiplies by however many times the crawler happened to revisit.
    """
    for name, pred in (("replies", "reply_to_status_id IS NOT NULL"),
                       ("quotes", "quoting_id IS NOT NULL AND body NOT LIKE 'RT @%'")):
        out = CMT / f"{name}.parquet"
        if out.exists() and not force:
            n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
            print(f"  [1] {name}: {n:,} already extracted, skipping")
            continue
        t = time.time()
        con.execute(f"""COPY (
            WITH c AS (
                SELECT id, author_id, body, created_at, like_count, lang,
                       reply_to_status_id, conversation_id, quoting_id,
                       row_number() OVER (PARTITION BY id ORDER BY version DESC) rn
                FROM read_parquet('{FIREHOSE}')
                WHERE {pred}
            ) SELECT * EXCLUDE rn FROM c WHERE rn = 1
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        print(f"  [1] {name}: {n:,} rows  ({time.time() - t:.0f}s)")


# ---------------------------------------------------------------------------
# step 2 -- bridge comments to the wordings in the export
# ---------------------------------------------------------------------------
def _wanted(idx: dict, trees: list) -> list[tuple]:
    """Every distinct wording in the export, with its prefix-match key."""
    seen: dict[str, tuple] = {}
    def add(t):
        if not isinstance(t, str) or t in seen or len(t) < KEY:
            return
        s = t.rstrip()
        if s.endswith(CUT):
            stem = s[: s.rindex(CUT)]
            if len(stem) >= KEY:
                seen[t] = (t, stem, stem[:KEY], True)
        else:
            seen[t] = (t, t, t[:KEY], False)
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                add(n.get("txt"))
    return list(seen.values())


def step2_bridge(con, idx, trees, force: bool) -> None:
    out = CMT / "linked.parquet"
    if out.exists() and not force:
        n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        print(f"  [2] linked: {n:,} comments already bridged, skipping")
        return
    want = _wanted(idx, trees)
    print(f"  [2] bridging {len(want):,} wordings to comments...")
    t = time.time()
    con.execute("CREATE OR REPLACE TEMP TABLE want "
                "(txt VARCHAR, stem VARCHAR, k VARCHAR, clipped BOOLEAN)")
    con.executemany("INSERT INTO want VALUES (?, ?, ?, ?)", want)

    # The emitting tweets for each wording. DISTINCT is load-bearing: the same
    # tweet id occurs once per crawl revisit.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE emit AS
        SELECT DISTINCT w.txt, f.id AS tweet_id,
               (f.conversation_id = f.id) AS is_root
        FROM want w
        JOIN read_parquet('{FIREHOSE}') f
          ON substr(f.body, 1, {KEY}) = w.k
        WHERE f.body NOT LIKE 'RT @%'
          AND f.body NOT LIKE '%{CUT}'
          AND length(f.body) <= {MAX_TEXT}
          AND ( (w.clipped     AND starts_with(f.body, w.stem))
             OR (NOT w.clipped AND f.body = w.txt) )""")
    e = con.execute("SELECT count(*), count(DISTINCT txt) FROM emit").fetchone()
    print(f"      {e[0]:,} emitting tweets for {e[1]:,} wordings  ({time.time() - t:.0f}s)")

    # Three equijoins unioned. Never an OR-join: that plans as a nested loop
    # over 10M rows and does not finish.
    t = time.time()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE linked AS
        SELECT txt, cid, any_value(ctype) AS ctype, any_value(author_id) AS author_id,
               any_value(body) AS body, any_value(created_at) AS created_at,
               any_value(like_count) AS like_count
        FROM (
            SELECT m.txt, r.id AS cid, 'reply' AS ctype, r.author_id, r.body,
                   r.created_at, r.like_count
              FROM emit m JOIN read_parquet('{CMT}/replies.parquet') r
                ON r.reply_to_status_id = m.tweet_id
              WHERE r.lang = 'en'
            UNION ALL
            SELECT m.txt, r.id, 'thread', r.author_id, r.body, r.created_at, r.like_count
              FROM emit m JOIN read_parquet('{CMT}/replies.parquet') r
                ON r.conversation_id = m.tweet_id
              WHERE m.is_root AND r.lang = 'en'
            UNION ALL
            SELECT m.txt, q.id, 'quote', q.author_id, q.body, q.created_at, q.like_count
              FROM emit m JOIN read_parquet('{CMT}/quotes.parquet') q
                ON q.quoting_id = m.tweet_id
              WHERE q.lang = 'en'
        )
        WHERE length(body) >= {MIN_BODY}
        GROUP BY txt, cid""")
    con.execute(f"COPY linked TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    r = con.execute("""SELECT count(*), count(DISTINCT txt), count(DISTINCT author_id),
                              sum((ctype='quote')::int), sum((ctype!='quote')::int)
                       FROM linked""").fetchone()
    print(f"      {r[0]:,} comments on {r[1]:,} wordings from {r[2]:,} accounts "
          f"({r[3]:,} quotes, {r[4]:,} replies)  ({time.time() - t:.0f}s)")


# ---------------------------------------------------------------------------
# step 3 -- stance + polarity
# ---------------------------------------------------------------------------
def step3_score(con, force: bool) -> None:
    out = CMT / "scored.parquet"
    if out.exists() and not force:
        print("  [3] scores exist, skipping")
        return
    import pandas as pd
    t = time.time()
    df = con.execute(f"SELECT * FROM read_parquet('{CMT}/linked.parquet')").df()
    print(f"  [3] scoring {len(df):,} comments (stance lexicon + VADER)...")
    # One comment id can attach to several wordings; score the text once.
    uniq = df.drop_duplicates("cid")[["cid", "body"]]
    res = {c: score(b) for c, b in zip(uniq.cid, uniq.body)}
    df["stance"] = [res[c]["stance"] for c in df.cid]
    df["tone"] = [res[c]["tone"] for c in df.cid]
    df["pol"] = [res[c]["pol"] for c in df.cid]
    df.to_parquet(out, index=False)
    mix = collections.Counter(res[c]["stance"] for c in uniq.cid)
    tn = collections.Counter(res[c]["tone"] for c in uniq.cid)
    tot = max(1, sum(mix.values()))
    print("      stance " + " ".join(f"{s}={100 * mix[s] / tot:.0f}%" for s in STANCES))
    print("      tone   " + " ".join(f"{t}={100 * tn[t] / tot:.0f}%" for t in TONES))
    print(f"      ({time.time() - t:.0f}s)")


# ---------------------------------------------------------------------------
# step 4 -- one global clustering, so a cluster can span lineages
# ---------------------------------------------------------------------------
def step4_cluster(con, force: bool) -> None:
    out = CMT / "clusters.parquet"
    if out.exists() and not force:
        print("  [4] clusters exist, skipping")
        return
    import pandas as pd
    t = time.time()
    df = con.execute(f"""SELECT DISTINCT cid, any_value(body) body
                         FROM read_parquet('{CMT}/scored.parquet')
                         GROUP BY cid""").df()
    print(f"  [4] clustering {len(df):,} distinct comments...")

    toks, ids = [], []
    for cid, body in zip(df.cid, df.body):
        T = set(normalize(body)["tokens"])
        if len(T) >= MIN_TOKENS:
            toks.append(T); ids.append(cid)
    print(f"      {len(ids):,} with >= {MIN_TOKENS} content tokens  ({time.time() - t:.0f}s)")

    t = time.time()
    dfq: collections.Counter = collections.Counter()
    for T in toks:
        dfq.update(T)
    blocks: dict[str, list[int]] = collections.defaultdict(list)
    for i, T in enumerate(toks):
        for w in sorted(T, key=lambda w: dfq[w])[:BLOCK_KEYS]:
            if dfq[w] <= MAX_DF:
                blocks[w].append(i)

    par = list(range(len(toks)))
    def find(x):
        while par[x] != x:
            par[x] = par[par[x]]; x = par[x]
        return x

    merges = 0
    for w, idx in blocks.items():
        if len(idx) < 2 or len(idx) > MAX_BLOCK:
            continue
        for a in range(len(idx)):
            ia = idx[a]; ta = toks[ia]
            for b in range(a + 1, len(idx)):
                ib = idx[b]
                ra, rb = find(ia), find(ib)
                if ra == rb:
                    continue
                inter = len(ta & toks[ib])
                if not inter:
                    continue
                if inter / (len(ta) + len(toks[ib]) - inter) >= SIM:
                    par[ra] = rb; merges += 1
    print(f"      {merges:,} merges  ({time.time() - t:.0f}s)")

    groups: dict[int, list[int]] = collections.defaultdict(list)
    for i in range(len(toks)):
        groups[find(i)].append(i)
    rows = []
    cl = 0
    generic = 0
    for members in groups.values():
        if len(members) < MIN_CLUSTER:
            continue
        # The vocabulary the members actually share, and how common it is in
        # the comment corpus. This is what separates a script from a phrase
        # everybody happens to use.
        #
        # Majority membership rather than strict intersection: a template
        # picks up stray words as it is passed around, so over twenty members
        # the full intersection erodes to whatever connective tissue survived
        # and reports the cluster as generic on the strength of two common
        # words. Tokens carried by half the members are the template.
        seen: collections.Counter = collections.Counter()
        for i in members:
            seen.update(toks[i])
        half = len(members) / 2
        core = [w for w, k in seen.items() if k >= half]
        mdf = (sum(dfq[w] for w in core) / len(core)) if core else 10 ** 6
        is_generic = mdf > GENERIC_DF
        generic += int(is_generic)
        for i in members:
            rows.append((ids[i], cl, round(float(mdf), 1), is_generic))
        cl += 1
    pd.DataFrame(rows, columns=["cid", "cluster", "mdf", "generic"]).to_parquet(out, index=False)
    print(f"      {cl:,} clusters covering {len(rows):,} comments "
          f"({generic:,} flagged generic vocabulary)")


# ---------------------------------------------------------------------------
# step 5 -- aggregate and write into the export
# ---------------------------------------------------------------------------
def _stance_mix(series) -> dict:
    c = collections.Counter(series)
    return {s: int(c[s]) for s in STANCES if c[s]}


def _tone_mix(series) -> dict:
    c = collections.Counter(series)
    return {t: int(c[t]) for t in TONES if c[t]}


def _sample(rows, t0) -> list[dict]:
    """The loudest few comments, with what it takes to draw one as a post.

    The dashboard renders these in X's own card, the same one the wording
    above them is drawn in, so each carries the three things that card needs
    beyond its text: the comment's own tweet id -- x.com/i/status/<id>
    resolves without a handle, and on X the date *is* the permalink -- the
    hour it was posted on the same corpus clock every other timestamp in the
    export uses, and a hue.

    The hue is the only field here that is derived rather than recorded, and
    it exists because of what this corpus does not have. There is no handle,
    bio or avatar for a posting account anywhere in the firehose, so the card
    cannot name whoever wrote a comment and must not look as though it has.
    What can be said honestly is *the same account again*: the circle is
    tinted by a hash of the author id and left as a silhouette, so two
    comments from one account match without anybody being handed a name they
    did not have. On a page about manufactured engagement that distinction is
    the whole point -- see the same rule applied to the counters in stage 5.
    """
    out = []
    for r in rows.head(SHOW).itertuples():
        out.append({
            "txt": str(r.body)[:300],
            "st": r.stance,
            "tn": r.tone,
            "like": int(r.like_count or 0),
            "q": r.ctype == "quote",
            "id": str(r.cid),
            "t": int((r.created_at - t0).total_seconds() // 3600),
            "hue": int(hashlib.blake2s(str(r.author_id).encode(),
                                       digest_size=2).hexdigest(), 16) % 360,
        })
    return out


def step5_export(con, idx, trees, force: bool) -> None:
    import numpy as np
    import pandas as pd
    t = time.time()
    print("  [5] aggregating into the export...")

    df = con.execute(f"""
        SELECT s.*, c.cluster, c.mdf, c.generic
        FROM read_parquet('{CMT}/scored.parquet') s
        LEFT JOIN read_parquet('{CMT}/clusters.parquet') c USING (cid)
    """).df()
    df["created_at"] = pd.to_datetime(df.created_at, utc=True)
    t0 = pd.Timestamp(json.loads((PHYLO / "index.json").read_text())["meta"]["t0"])

    # wording -> the families it appears in, so a cluster can be told how many
    # separate lineages it turns up under.
    fam_of: dict[str, set] = collections.defaultdict(set)
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                if isinstance(n.get("txt"), str):
                    fam_of[n["txt"]].add(int(fam["id"]))
    df["fams"] = df.txt.map(lambda x: fam_of.get(x, set()))

    # --- cluster catalogue (cross-lineage by construction) -----------------
    cat = {}
    has = df[df.cluster.notna()]
    for cid, g in has.groupby("cluster"):
        fams = set().union(*g.fams) if len(g) else set()
        best = g.sort_values("like_count", ascending=False)
        hrs = ((g.created_at - t0).dt.total_seconds() // 3600).astype(int)
        cat[int(cid)] = {
            "id": int(cid),
            "n": int(len(g)),
            "acc": int(g.author_id.nunique()),
            "w": int(g.txt.nunique()),
            "fams": sorted(fams)[:40],
            "nfam": len(fams),
            "st": _stance_mix(g.stance),
            "tn": _tone_mix(g.tone),
            "pol": round(float(g.pol.mean()), 3),
            "q": int((g.ctype == "quote").sum()),
            "t0": int(hrs.min()), "t1": int(hrs.max()),
            "mdf": float(g.mdf.iloc[0]),
            "gen": bool(g.generic.iloc[0]),
            "sample": [str(b)[:300] for b in best.body.head(SHOW)],
        }
    # An earlier rule fired on reach alone and put "Don't piss me off" at the
    # top of the list across 22 lineages, which is a fact about English and
    # not about an operation. Distinctive vocabulary plus a real number of
    # distinct accounts is all that is claimed now.
    for c in cat.values():
        span = max(1, c["t1"] - c["t0"])
        c["burst"] = round(c["acc"] / span, 2)
        c["reuse"] = bool(not c["gen"] and c["acc"] >= REUSE_MIN_ACC)

    # Ranked by how many accounts repeated it, not by how many lineages it
    # touched: reach is the interesting *attribute* of a template but a poor
    # way to find one, because the phrases with the widest reach are the
    # least distinctive things anybody says.
    order = sorted(cat.values(),
                   key=lambda c: (c["reuse"], not c["gen"], c["acc"], c["nfam"]),
                   reverse=True)
    (PHYLO / "comments.json").write_text(json.dumps({
        "meta": {
            "n_comments": int(len(df)),
            "n_clustered": int(has.shape[0]),
            "n_clusters": len(cat),
            "n_reuse": sum(1 for c in cat.values() if c["reuse"]),
            "quotes": int((df.ctype == "quote").sum()),
            "replies": int((df.ctype != "quote").sum()),
            "stances": _stance_mix(df.stance),
            "tones": _tone_mix(df.tone),
            "n_generic": sum(1 for c in cat.values() if c["gen"]),
            "params": {"sim": SIM, "min_cluster": MIN_CLUSTER,
                       "min_tokens": MIN_TOKENS, "max_df": MAX_DF,
                       "generic_df": GENERIC_DF, "reuse_min_acc": REUSE_MIN_ACC},
            "note": "Quote tweets and replies are both comments and are counted "
                    "separately; `q` is the quote share. Clusters are built once "
                    "over all comments, so `nfam` counts the distinct lineages a "
                    "single response template turns up under.",
        },
        "clusters": order[:CATALOGUE],
    }, ensure_ascii=False, separators=(",", ":")))
    print(f"      {len(cat):,} clusters, "
          f"{sum(1 for c in cat.values() if c['gen']):,} generic vocabulary, "
          f"{sum(1 for c in cat.values() if c['reuse']):,} flagged template reuse")

    # --- per wording, then onto the nodes ----------------------------------
    by_txt = {}
    for txt, g in df.groupby("txt"):
        cl = collections.Counter(int(c) for c in g.cluster.dropna())
        best = g.sort_values("like_count", ascending=False)
        by_txt[txt] = {
            "n": int(len(g)),
            "acc": int(g.author_id.nunique()),
            "q": int((g.ctype == "quote").sum()),
            "st": _stance_mix(g.stance),
            "tn": _tone_mix(g.tone),
            "pol": round(float(g.pol.mean()), 3),
            "cl": [[c, n] for c, n in cl.most_common(TOP_CLUSTERS)],
            "sample": _sample(best, t0),
        }

    hit = 0
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                c = by_txt.get(n.get("txt"))
                if c:
                    n["cmt"] = c
                    hit += 1

    # --- per family --------------------------------------------------------
    # Exploded once and grouped, rather than filtered per family: a wording
    # can sit in more than one lineage, and re-scanning 384k comments for each
    # of ~50k families is quadratic and does not finish.
    ex = df[["author_id", "ctype", "stance", "tone", "pol", "cluster", "fams"]].explode("fams")
    ex = ex[ex.fams.notna()]
    ex["fams"] = ex.fams.astype(int)
    by_fam = {int(k): g for k, g in ex.groupby("fams", sort=False)}

    fam_hit = 0
    for f in idx["families"]:
        fid = int(f["id"])
        rows = by_fam.get(fid)
        if rows is None or not len(rows):
            continue
        cl = collections.Counter(int(c) for c in rows.cluster.dropna())
        # Only distinctive templates are worth calling out as shared: a
        # lineage whose "shared response" is a common exclamation has not
        # told anybody anything.
        shared = [c for c, _ in cl.most_common()
                  if cat.get(c, {}).get("nfam", 0) >= 2 and not cat.get(c, {}).get("gen")]
        f["cmt"] = {
            "n": int(len(rows)),
            "acc": int(rows.author_id.nunique()),
            "q": int((rows.ctype == "quote").sum()),
            "st": _stance_mix(rows.stance),
            "tn": _tone_mix(rows.tone),
            "pol": round(float(rows.pol.mean()), 3),
            "ncl": len(cl),
            "cl": [[c, n] for c, n in cl.most_common(TOP_CLUSTERS)],
            "shared": shared[:TOP_CLUSTERS],
            "reuse": sum(1 for c in cl if cat.get(c, {}).get("reuse")),
        }
        fam_hit += 1

    def put(p: Path, obj):
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
        tmp.replace(p)
    put(PHYLO / "index.json", idx)
    for p, tree in trees:
        put(p, tree)
    print(f"      attached to {hit:,} nodes and {fam_hit:,} lineages  ({time.time() - t:.0f}s)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--steps", default="1,2,3,4,5")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    steps = {int(s) for s in a.steps.split(",") if s.strip()}

    if not (PHYLO / "index.json").exists():
        sys.exit(f"no export at {PHYLO} - run ./run.sh evolution first")

    idx = json.loads((PHYLO / "index.json").read_text())
    trees = [(p, json.loads(p.read_text())) for p in sorted(PHYLO.glob("trees-*.json"))]

    con = connect()
    t = time.time()
    if 1 in steps: step1_extract(con, a.force)
    if 2 in steps: step2_bridge(con, idx, trees, a.force)
    if 3 in steps: step3_score(con, a.force)
    if 4 in steps: step4_cluster(con, a.force)
    if 5 in steps: step5_export(con, idx, trees, a.force)
    print(f"  done in {time.time() - t:.0f}s - run ./run.sh dist to publish")


if __name__ == "__main__":
    main()
