"""
Stage 4 — meme phylogenetics: variant families and their mutation trees.

The claim this stage tests: a narrative is not a fixed string. It is a lineage.
A phrasing appears, spreads, gets reworded -- by paraphrase, synonym swap,
homoglyph substitution, emoji standing in for the flagged word -- and the
rewordings compete. Some die, some out-spread the original. That is variation
plus differential reproduction, which is the only thing "evolution" ever means.

Method, in four moves:

  1. CANDIDATES. Every distinct English message, keyed by content_key, with its
     global copy count. `content_key = md5(rt_handle || '|' || text)`, so for a
     retweet the key identifies the *upstream* tweet and n_copies is how many
     accounts amplified it; for an original it is how many accounts posted that
     exact string. Either way n_copies is "how many times this string was
     emitted", which is the variant's reproductive success.

  2. BLOCKING. Comparing every message to every other is 10^14 pairs. Instead,
     index each message by its k rarest tokens and only compare messages that
     share one. The justification is specific rather than generic: mutations
     preserve the distinctive vocabulary (that is what makes them recognisable
     as the same narrative) and it is the common connective words that get
     swapped. So rare tokens are exactly the part of the message that survives
     mutation, which makes them good blocking keys and not merely cheap ones.

  3. FAMILIES. Exact Jaccard on token sets for the candidate pairs only; keep
     pairs over a threshold; connected components of that graph are variant
     families. Retweet truncation (the platform cuts at 140 chars) is collapsed
     first, so a cut-off copy is not mistaken for a mutation.

  4. TREES. Within a family, sort variants by first appearance and attach each
     to whichever earlier variant it most resembles. The result is a rooted
     tree: the root is the earliest phrasing observed, edges are mutations, and
     each edge carries the tokens that were added and dropped. Subtree spread
     tells you which mutations were selected for.

Every variant also carries an evasion profile -- homoglyph characters, emoji
count, how much of its spread came from accounts stage 2 flagged as
coordinated -- so the tree can be read against moderation pressure.

Outputs:
  data/normalized/content_global.parquet   deduped English content, global counts
  data/evolution/tokens.parquet            content_key -> tokens, evasion features
  data/evolution/pairs.parquet             u, v, jaccard  (above threshold)
  data/evolution/families.parquet          content_key -> family_id
  data/export/phylo/index.json             family list for the dashboard
  data/export/phylo/family-<id>.json       one tree, with timelines

Run:  python3 pipeline/04_evolution.py [--steps 1,2,3,4,5] [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C
from pipeline.textnorm import CONF_FROM, CONF_TO, EMOJI_RE2, STOPWORDS

EVO = C.DATA / "evolution"
PHYLO = C.EXPORT / "phylo"
CONTENT_GLOBAL = C.DATA / "normalized" / "content_global.parquet"

# --- tunables ---------------------------------------------------------------
MIN_LEN = 40          # chars; shorter messages are too generic to trace
MAX_LEN = 600
MIN_TOKENS = 5        # distinct content tokens required to be traceable
MIN_COPIES = 2        # a string emitted once is not yet a meme
BLOCK_KEYS = 4        # rarest tokens indexed per message
MAX_DF = 2000         # a token in more messages than this is not distinctive
MAX_BLOCK = 100       # messages paired inside one block (caps quadratic blowup)
RARE_DF = 20          # a shared token this rare is on its own enough to pair on
SIM_THRESHOLD = 0.45  # token-set Jaccard for "same narrative, reworded"
MIN_FAMILY = 3        # variants required to call something a lineage
MAX_FAMILY = 300      # single-linkage blobs get truncated to their top variants
MIN_FAMILY_EMISSIONS = 150  # a lineage nobody spread is not worth a tree
N_BUCKETS = 24        # tree files; the dashboard fetches one on demand

# The crawl is not a uniform month: Aug 16-31 carries 93.5% of the corpus at
# 22M tweets/day, Sep 1-17 carries 6.5% at 1.4M/day -- a 15.3x collection drop,
# not a change in the discourse. Every variant whose lineage crosses this date
# is measured against a thinner sample on the far side, so a lineage will look
# like it died on 1 September when it has only gone unobserved. We mark it
# rather than hide it, and rank August-only lineages first.
COLLECTION_CLIFF = "2026-09-01T00:00:00+00:00"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute(f"SET threads={C.THREADS}")
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def sql_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# SQL mirror of textnorm.normalize(). Kept as one expression so the whole
# 30M-row pass stays inside DuckDB -- a Python UDF here costs an hour.
def norm_expr(col: str) -> str:
    conf_from, conf_to = sql_literal(CONF_FROM), sql_literal(CONF_TO)
    ent = col
    for a, b in [("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                 ("&quot;", '"'), ("&#39;", "'"), ("&nbsp;", " ")]:
        ent = f"replace({ent}, {sql_literal(a)}, {sql_literal(b)})"
    folded = f"translate({ent}, {conf_from}, {conf_to})"
    de_url = rf"regexp_replace({folded}, 'https?://\S+|\bt\.co/\S*|\bht(?:t(?:p(?:s)?)?)?…', ' ', 'g')"
    de_at = rf"regexp_replace({de_url}, '@[A-Za-z0-9_]{{1,15}}', ' ', 'g')"
    de_hash = f"replace({de_at}, '#', ' ')"
    ascii_ = f"strip_accents({de_hash})"
    words = rf"regexp_replace(lower({ascii_}), '[^a-z0-9'']+', ' ', 'g')"
    return f"trim(regexp_replace({words}, '\\s+', ' ', 'g'))"


def step1_content_global(con, force: bool) -> None:
    """Collapse per-shard content tables into one globally-deduped table."""
    if CONTENT_GLOBAL.exists() and not force:
        print("  [1] content_global exists, skipping")
        return
    print("  [1] building global content table (dedup across shards)...")
    t = time.time()
    con.execute(f"""
        COPY (
            SELECT content_key,
                   any_value(kind)      AS kind,
                   any_value(rt_handle) AS rt_handle,
                   any_value(text)      AS text,
                   sum(n_copies)::BIGINT AS n_copies,
                   min(first_seen)      AS first_seen
            FROM read_parquet('{C.CONTENT}/*.parquet')
            WHERE lang = 'en' AND length(text) BETWEEN {MIN_LEN} AND {MAX_LEN}
            GROUP BY content_key
        ) TO '{CONTENT_GLOBAL}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = con.execute(f"SELECT count(*) FROM '{CONTENT_GLOBAL}'").fetchone()[0]
    print(f"      {n:,} distinct English messages  ({time.time() - t:.0f}s)")


def step2_tokens(con, force: bool) -> None:
    """Tokenize and extract evasion features. One pass, all in DuckDB."""
    out = EVO / "tokens.parquet"
    if out.exists() and not force:
        print("  [2] tokens exist, skipping")
        return
    print("  [2] tokenizing + extracting evasion features...")
    t = time.time()
    stop = "[" + ", ".join(sql_literal(w) for w in sorted(STOPWORDS)) + "]"
    norm = norm_expr("text")
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT content_key, kind, rt_handle, text, n_copies, first_seen,
                       {norm} AS norm,
                       -- deleting the confusable set and measuring the shortfall
                       -- counts homoglyph/styled characters without a second pass
                       length(text) - length(translate(text, {sql_literal(CONF_FROM)}, '')) AS obf_chars,
                       len(regexp_extract_all(text, '{EMOJI_RE2}')) AS n_emoji,
                       len(regexp_extract_all(text, '#(\\w+)')) AS n_hashtags,
                       (text LIKE '%…') AS is_truncated
                FROM '{CONTENT_GLOBAL}'
                WHERE n_copies >= {MIN_COPIES}
            ),
            tok AS (
                SELECT *, list_sort(list_distinct(list_filter(
                           string_split(norm, ' '),
                           w -> length(w) >= 2 AND NOT list_contains({stop}, w)
                       ))) AS tokens
                FROM base
            )
            SELECT content_key, kind, rt_handle, text, norm, tokens,
                   len(tokens) AS n_tokens,
                   n_copies, first_seen, obf_chars, n_emoji, n_hashtags, is_truncated
            FROM tok
            WHERE len(tokens) >= {MIN_TOKENS}
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n, nc = con.execute(f"SELECT count(*), sum(n_copies) FROM '{out}'").fetchone()
    print(f"      {n:,} traceable messages, {nc:,} total emissions  ({time.time() - t:.0f}s)")


def step3_pairs(con, force: bool) -> None:
    """Rare-token blocking, then exact Jaccard on the candidates only.

    Runs against an on-disk DuckDB database rather than TEMP tables: TEMP is
    memory-resident, and the intermediate join here is hundreds of millions of
    rows. Everything is keyed by dense integers for the same reason -- carrying
    32-char md5 strings through a window function over 59M rows is what made
    the first version die at 30 GB.
    """
    out = EVO / "pairs.parquet"
    if out.exists() and not force:
        print("  [3] pairs exist, skipping")
        return
    print("  [3] blocking on rare tokens and scoring candidate pairs...")
    t = time.time()
    dbp = EVO / "evo.duckdb"
    if dbp.exists():
        dbp.unlink()
    db = duckdb.connect(str(dbp))
    db.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    db.execute(f"SET threads={C.THREADS}")
    db.execute(f"SET temp_directory='{C.TMP_DIR}'")
    db.execute("SET preserve_insertion_order=false")

    db.execute(f"CREATE VIEW docs AS SELECT * FROM '{EVO}/tokens.parquet'")
    db.execute("""
        CREATE TABLE dict AS
        SELECT content_key, tokens, n_tokens, n_copies,
               (row_number() OVER (ORDER BY content_key) - 1)::BIGINT AS doc_id
        FROM docs
    """)
    db.execute("CREATE TABLE tk AS SELECT doc_id, unnest(tokens) AS tok FROM dict")
    db.execute(f"""
        CREATE TABLE df AS
        SELECT tok, count(*)::INT AS n,
               (row_number() OVER (ORDER BY tok) - 1)::INT AS tok_id
        FROM tk GROUP BY tok HAVING count(*) BETWEEN 2 AND {MAX_DF}
    """)
    ndf = db.execute("SELECT count(*) FROM df").fetchone()[0]
    print(f"      {ndf:,} usable blocking tokens (df 2..{MAX_DF})")

    # Index each message under its BLOCK_KEYS rarest tokens, then cap each
    # block. When a block overflows we keep the most-copied messages: if we
    # have to compare only some of a crowded block, compare the ones that
    # actually spread.
    db.execute(f"""
        CREATE TABLE blocks AS
        SELECT tok_id, doc_id, n FROM (
            SELECT d.tok_id, t.doc_id, d.n,
                   row_number() OVER (PARTITION BY d.tok_id ORDER BY k.n_copies DESC, t.doc_id) AS brn
            FROM (
                SELECT doc_id, tok, rk FROM (
                    SELECT t.doc_id, t.tok,
                           row_number() OVER (PARTITION BY t.doc_id ORDER BY d.n, t.tok) AS rk
                    FROM tk t JOIN df d USING (tok)
                ) WHERE rk <= {BLOCK_KEYS}
            ) t
            JOIN df d USING (tok)
            JOIN dict k ON k.doc_id = t.doc_id
        ) WHERE brn <= {MAX_BLOCK}
    """)
    nb, ndoc = db.execute(
        "SELECT count(*), count(DISTINCT doc_id) FROM blocks").fetchone()
    tot = db.execute("SELECT count(*) FROM dict").fetchone()[0]
    print(f"      {nb:,} block postings covering {ndoc:,}/{tot:,} messages")

    # A pair is worth scoring if it shares two blocking tokens, or one rare
    # enough that coincidence is implausible. This cuts the expensive
    # set-intersection step by an order of magnitude with little recall cost.
    db.execute(f"""
        CREATE TABLE cand AS
        SELECT u, v FROM (
            SELECT a.doc_id AS u, b.doc_id AS v, count(*) AS shared, min(a.n) AS rarest
            FROM blocks a JOIN blocks b ON a.tok_id = b.tok_id AND a.doc_id < b.doc_id
            GROUP BY 1, 2
        ) WHERE shared >= 2 OR rarest <= {RARE_DF}
    """)
    ncand = db.execute("SELECT count(*) FROM cand").fetchone()[0]
    print(f"      {ncand:,} candidate pairs to score  ({time.time() - t:.0f}s)")

    db.execute(f"""
        CREATE TABLE scored AS
        SELECT du.content_key AS u, dv.content_key AS v, j.jaccard
        FROM (
            SELECT c.u, c.v,
                   len(list_intersect(du.tokens, dv.tokens))::DOUBLE
                     / (du.n_tokens + dv.n_tokens
                        - len(list_intersect(du.tokens, dv.tokens))) AS jaccard
            FROM cand c
            JOIN dict du ON du.doc_id = c.u
            JOIN dict dv ON dv.doc_id = c.v
        ) j
        JOIN dict du ON du.doc_id = j.u
        JOIN dict dv ON dv.doc_id = j.v
        WHERE j.jaccard >= {SIM_THRESHOLD}
    """)
    db.execute(f"COPY scored TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = db.execute("SELECT count(*) FROM scored").fetchone()[0]
    db.close()
    dbp.unlink(missing_ok=True)
    print(f"      {n:,} variant pairs above Jaccard {SIM_THRESHOLD}  ({time.time() - t:.0f}s)")


def step4_families(con, force: bool) -> None:
    """Connected components of the variant graph, with truncation collapsed."""
    out = EVO / "families.parquet"
    if out.exists() and not force:
        print("  [4] families exist, skipping")
        return
    import numpy as np
    import pandas as pd
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components

    print("  [4] grouping variants into families...")
    t = time.time()
    pairs = pd.read_parquet(EVO / "pairs.parquet")
    docs = pd.read_parquet(
        EVO / "tokens.parquet",
        columns=["content_key", "norm", "n_copies", "is_truncated", "n_tokens"],
    )

    # Retweet bodies are cut at 140 chars, so ~36% of amplifications arrive
    # pre-mutilated. A cut copy shares a prefix with its full form; treating it
    # as a distinct variant would invent a mutation the platform made, not the
    # network. Collapse it into the full text before anything else.
    print("      collapsing truncation artifacts...")
    trunc = docs[docs.is_truncated & (docs.n_tokens >= MIN_TOKENS)]
    full = docs[~docs.is_truncated]
    # A truncated body's last token is usually a fragment; drop it before matching.
    stem = trunc.norm.str.rsplit(" ", n=1).str[0]
    lookup: dict[str, str] = {}
    by_len = full.sort_values("n_copies", ascending=False)
    prefix_map: dict[str, str] = {}
    for ck, nm in zip(by_len.content_key.values, by_len.norm.values):
        for L in (60, 80, 100):
            prefix_map.setdefault(nm[:L], ck)
    for ck, st in zip(trunc.content_key.values, stem.values):
        for L in (100, 80, 60):
            hit = prefix_map.get(st[:L])
            if hit is not None and hit != ck:
                lookup[ck] = hit
                break
    print(f"      {len(lookup):,} truncated copies folded into their full form")

    def canon(k):
        return lookup.get(k, k)

    pairs["u"] = pairs.u.map(canon)
    pairs["v"] = pairs.v.map(canon)
    pairs = pairs[pairs.u != pairs.v]

    nodes = pd.unique(pd.concat([pairs.u, pairs.v], ignore_index=True))
    idx = {k: i for i, k in enumerate(nodes)}
    src = pairs.u.map(idx).to_numpy()
    dst = pairs.v.map(idx).to_numpy()
    m = coo_matrix((np.ones(len(src)), (src, dst)), shape=(len(nodes), len(nodes)))
    n_comp, labels = connected_components(m, directed=False)
    fam = pd.DataFrame({"content_key": nodes, "family_id": labels})

    sizes = fam.groupby("family_id").size()
    keep = sizes[sizes >= MIN_FAMILY].index
    fam = fam[fam.family_id.isin(keep)]
    # Single-linkage will occasionally chain unrelated messages into a blob.
    # Keep the most-spread variants of oversized families rather than dropping
    # them: the tail of a 5,000-node component is noise, the head is the meme.
    fam = fam.merge(docs[["content_key", "n_copies"]], on="content_key", how="left")
    fam["rk"] = fam.groupby("family_id").n_copies.rank(method="first", ascending=False)
    over = fam.family_id.map(sizes) > MAX_FAMILY
    fam = fam[~over | (fam.rk <= MAX_FAMILY)].drop(columns=["rk", "n_copies"])

    # Carry the fold-in mapping forward so step 5 can reattribute copy counts.
    alias = pd.DataFrame(
        {"content_key": list(lookup.keys()), "canonical": list(lookup.values())}
    )
    alias.to_parquet(EVO / "alias.parquet", index=False)
    fam.to_parquet(out, index=False)
    n_fam = fam.family_id.nunique()
    print(f"      {n_fam:,} families covering {len(fam):,} variants "
          f"(largest {fam.groupby('family_id').size().max():,})  ({time.time() - t:.0f}s)")


def step5_enrich(con, force: bool) -> None:
    """Attach spread, timing and coordination to every variant in a family."""
    out = EVO / "variants.parquet"
    if out.exists() and not force:
        print("  [5] enrichment exists, skipping")
        return
    print("  [5] measuring spread, timing and coordination per variant...")
    t = time.time()
    con.execute(f"CREATE OR REPLACE VIEW fam AS SELECT * FROM '{EVO}/families.parquet'")
    con.execute(f"CREATE OR REPLACE VIEW alias AS SELECT * FROM '{EVO}/alias.parquet'")
    con.execute(f"CREATE OR REPLACE VIEW tok AS SELECT * FROM '{EVO}/tokens.parquet'")

    clusters = C.GRAPH / "clusters.parquet"
    has_clusters = clusters.exists()
    if has_clusters:
        con.execute(f"CREATE OR REPLACE VIEW cl AS SELECT * FROM '{clusters}'")
        coord_join = "LEFT JOIN cl ON cl.author_id = e.author_id"
        coord_cols = ("count(DISTINCT CASE WHEN cl.cluster_id IS NOT NULL THEN e.author_id END) "
                      "AS n_coord_accounts, "
                      "count(DISTINCT cl.cluster_id) AS n_clusters, "
                      "list(DISTINCT cl.cluster_id) FILTER (cl.cluster_id IS NOT NULL) AS clusters,")
    else:
        coord_join, coord_cols = "", "0 AS n_coord_accounts, 0 AS n_clusters, [] AS clusters,"

    # Every emission of every variant in a family, with truncated copies
    # reattributed to the full text they were cut from.
    con.execute(f"""
        CREATE OR REPLACE VIEW keys AS
        SELECT f.content_key AS canonical, f.family_id, f.content_key AS raw
        FROM fam f
        UNION ALL
        SELECT a.canonical, f.family_id, a.content_key AS raw
        FROM alias a JOIN fam f ON f.content_key = a.canonical
    """)
    con.execute(f"""
        COPY (
            WITH ev AS (
                SELECT k.canonical, k.family_id, e.author_id, e.created_at
                FROM read_parquet('{C.EVENTS}/*.parquet') e
                JOIN keys k ON k.raw = e.content_key
                WHERE e.lang = 'en'
            ),
            agg AS (
                SELECT e.canonical, e.family_id,
                       count(*)::BIGINT              AS n_emissions,
                       count(DISTINCT e.author_id)   AS n_accounts,
                       {coord_cols}
                       min(e.created_at)             AS first_seen,
                       max(e.created_at)             AS last_seen
                FROM ev e {coord_join}
                GROUP BY e.canonical, e.family_id
            )
            SELECT a.*, t.text, t.norm, t.tokens, t.n_tokens, t.kind, t.rt_handle,
                   t.obf_chars, t.n_emoji, t.n_hashtags
            FROM agg a JOIN tok t ON t.content_key = a.canonical
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    print(f"      {n:,} enriched variants  ({time.time() - t:.0f}s)")

    # Hourly emission curve per variant -- the raw material for the timeline.
    tl = EVO / "timeline.parquet"
    con.execute(f"""
        COPY (
            SELECT k.canonical, k.family_id,
                   date_trunc('hour', e.created_at) AS hr,
                   count(*)::INT AS n
            FROM read_parquet('{C.EVENTS}/*.parquet') e
            JOIN keys k ON k.raw = e.content_key
            WHERE e.lang = 'en'
            GROUP BY 1, 2, 3
        ) TO '{tl}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"      timelines written  ({time.time() - t:.0f}s)")

# Coarse topical tags. This is a keyword heuristic, not a classifier, and the
# UI says so -- its only job is to make thousands of families navigable. Terms
# marked with "!" are unambiguous enough to tag on their own; everything else
# needs corroboration, so that a family mentioning "white" or "strike" in
# passing does not get filed under identity or conflict.
TOPICS = {
    "politics": "trump biden election! vote voter ballot! congress senate president campaign "
                "democrat! republican! maga! conservative liberal government policy impeach!",
    "conflict": "israel! palestine! gaza! hamas! idf! zionist! genocide! ukraine russia war "
                "strike military airstrike! ceasefire! hostage occupation settler",
    "migration": "immigrant! immigration! migrant! refugee! asylum! border deport! "
                 "deportation! illegal alien! invasion assimilate visa amnesty!",
    "identity": "muslim! islam! jew! jewish! christian racist! racism! antisemitic! "
                "islamophobia! sharia! woke! dei trans lgbtq groomer! white black",
    "health": "vaccine! vaccinated! covid! pandemic! fauci! cdc! fda mrna! autism pharma "
              "outbreak measles! medicaid",
    "crypto": "bitcoin! btc! crypto! ethereum! token airdrop! presale! wallet trading forex! "
              "gold xauusd! signal pump profit",
    "fandom": "comeback teaser! album mv kpop! bts! nct! concert fancam! stan lightstick! "
              "preorder weverse! photocard! debut tour",
    "sports": "match goal league season transfer fixture cricket! football nba! ufc!",
}


def _parse(spec: str) -> dict[str, int]:
    out = {}
    for w in spec.split():
        out[w.rstrip("!")] = 2 if w.endswith("!") else 1
    return out


TOPIC_SETS = {k: _parse(v) for k, v in TOPICS.items()}


def classify(tokens: set) -> list[str]:
    hits = [(k, sum(w for tok, w in s.items() if tok in tokens))
            for k, s in TOPIC_SETS.items()]
    hits = [(k, n) for k, n in hits if n >= 2]
    hits.sort(key=lambda x: -x[1])
    return [k for k, _ in hits[:2]]


def step6_trees(con, force: bool) -> None:
    """Build one rooted mutation tree per family and export it for the map."""
    import numpy as np
    import pandas as pd

    idx_path = PHYLO / "index.json"
    if idx_path.exists() and not force:
        print("  [6] export exists, skipping")
        return
    print("  [6] building mutation trees...")
    t = time.time()

    v = pd.read_parquet(EVO / "variants.parquet")
    tl = pd.read_parquet(EVO / "timeline.parquet")
    v["first_seen"] = pd.to_datetime(v.first_seen, utc=True)
    v["last_seen"] = pd.to_datetime(v.last_seen, utc=True)
    tl["hr"] = pd.to_datetime(tl.hr, utc=True)

    # Only families with enough structure to be worth drawing a tree for.
    keep = v.groupby("family_id").agg(nv=("canonical", "size"),
                                      emis=("n_emissions", "sum"))
    keep = keep[(keep.nv >= MIN_FAMILY) & (keep.emis >= MIN_FAMILY_EMISSIONS)]
    v = v[v.family_id.isin(keep.index)]
    print(f"      {len(keep):,} families qualify ({len(v):,} variants)")

    t0_global = v.first_seen.min()
    cliff_h = int((pd.Timestamp(COLLECTION_CLIFF) - t0_global).total_seconds() // 3600)
    tl_by_fam = {k: g for k, g in tl[tl.family_id.isin(keep.index)].groupby("family_id")}

    index, buckets = [], {}
    for fid, g in v.groupby("family_id", sort=False):
        g = g.sort_values(["first_seen", "n_emissions"],
                          ascending=[True, False]).reset_index(drop=True)
        toks = [set(x) for x in g.tokens]
        n = len(g)

        # Attach each variant to the earlier variant it most resembles. Time
        # gives the edges their direction -- a phrasing cannot descend from one
        # that did not exist yet -- and similarity picks which ancestor. The
        # root is simply the earliest phrasing we observed, which is a claim
        # about this corpus and this month, not about the origin of the idea.
        parent = [-1] * n
        psim = [0.0] * n
        for i in range(1, n):
            best, bj = -1, -1.0
            ti = toks[i]
            for k in range(i):
                inter = len(ti & toks[k])
                if not inter:
                    continue
                j = inter / (len(ti) + len(toks[k]) - inter)
                if j > bj:
                    best, bj = k, j
            parent[i], psim[i] = (best, bj) if best >= 0 else (0, 0.0)

        # Subtree spread = the lineage's total reproductive success. Children
        # are always later in the array than their parent, so one reverse pass
        # accumulates it.
        sub = g.n_emissions.to_numpy().astype(float).copy()
        depth = np.zeros(n, dtype=int)
        for i in range(n - 1, 0, -1):
            sub[parent[i]] += sub[i]
        for i in range(1, n):
            depth[i] = depth[parent[i]] + 1

        root_tok = toks[0]
        nodes = []
        for i in range(n):
            r = g.iloc[i]
            added = sorted(toks[i] - toks[parent[i]])[:8] if i else []
            lost = sorted(toks[parent[i]] - toks[i])[:8] if i else []
            fam_tl = tl_by_fam.get(fid)
            spark = []
            if fam_tl is not None:
                s = fam_tl[fam_tl.canonical == r.canonical]
                spark = [[int((h - t0_global).total_seconds() // 3600), int(c)]
                         for h, c in zip(s.hr, s.n)]
            nodes.append({
                "i": i,
                "p": int(parent[i]) if i else None,
                "sim": round(float(psim[i]), 3),
                "txt": r.text[:280],
                "n": int(r.n_emissions),
                "acc": int(r.n_accounts),
                "co": int(r.n_coord_accounts),
                "t0": int((r.first_seen - t0_global).total_seconds() // 3600),
                "t1": int((r.last_seen - t0_global).total_seconds() // 3600),
                "sub": int(sub[i]),
                "d": int(depth[i]),
                "add": added,
                "del": lost,
                "obf": int(r.obf_chars),
                "emo": int(r.n_emoji),
                "drift": round(1 - (len(toks[i] & root_tok) /
                                    max(1, len(toks[i] | root_tok))), 3),
                "src": (r.rt_handle or None),
                "spark": spark,
            })

        all_tok = set().union(*toks) if toks else set()
        emis = int(g.n_emissions.sum())
        acc = int(g.n_accounts.sum())
        co = int(g.n_coord_accounts.sum())
        handles = (g[g.rt_handle.notna()].groupby("rt_handle").n_emissions.sum()
                   .sort_values(ascending=False).head(5))
        fam = {
            "id": int(fid),
            "nv": n,
            "emis": emis,
            "acc": acc,
            "co": co,
            "coord_share": round(co / acc, 3) if acc else 0.0,
            "t0": int((g.first_seen.min() - t0_global).total_seconds() // 3600),
            "t1": int((g.last_seen.max() - t0_global).total_seconds() // 3600),
            "depth": int(depth.max()),
            "drift": round(float(np.mean([nd["drift"] for nd in nodes])), 3),
            "obf": int(g.obf_chars.sum()),
            # Mean hashtags per variant separates the two kinds of lineage this
            # method finds: a prose narrative reworded by people (low) and a
            # promo template whose hashtag block is the conserved part and whose
            # free text is the variable region (high). Both are real mutation;
            # only one is the kind this project is about.
            "ht": round(float(g.n_hashtags.mean()), 2),
            "topics": classify(all_tok),
            "win": ("aug" if int((g.last_seen.max() - t0_global).total_seconds() // 3600) < cliff_h
                    else "sep" if int((g.first_seen.min() - t0_global).total_seconds() // 3600) >= cliff_h
                    else "span"),
            "root": nodes[0]["txt"][:180],
            "top": g.sort_values("n_emissions", ascending=False).iloc[0].text[:180],
            "handles": [[h, int(c)] for h, c in handles.items()],
            "q": " ".join(sorted(all_tok))[:320],
        }
        index.append(fam)
        buckets.setdefault(int(fid) % N_BUCKETS, {})[str(int(fid))] = {
            "id": int(fid), "nodes": nodes,
        }

    index.sort(key=lambda f: -f["emis"])
    meta = {
        "t0": t0_global.isoformat(),
        "n_families": len(index),
        "n_variants": int(v.shape[0]),
        "n_emissions": int(v.n_emissions.sum()),
        "buckets": N_BUCKETS,
        "cliff_h": cliff_h,
        "cliff_note": "Crawl volume drops 15.3x on 2026-09-01 (22.0M tweets/day "
                      "before, 1.4M/day after). Counts either side are not comparable.",
        "params": {
            "sim_threshold": SIM_THRESHOLD, "min_copies": MIN_COPIES,
            "block_keys": BLOCK_KEYS, "max_df": MAX_DF, "max_block": MAX_BLOCK,
            "min_family": MIN_FAMILY, "min_family_emissions": MIN_FAMILY_EMISSIONS,
        },
    }
    idx_path.write_text(json.dumps({"meta": meta, "families": index},
                                   separators=(",", ":")))
    for b, payload in buckets.items():
        (PHYLO / f"trees-{b:02d}.json").write_text(
            json.dumps(payload, separators=(",", ":")))
    size = sum(f.stat().st_size for f in PHYLO.glob("*.json")) / 1e6
    print(f"      {len(index):,} families exported in {len(buckets)} buckets, "
          f"{size:.1f} MB  ({time.time() - t:.0f}s)")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="1,2,3,4,5,6")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    EVO.mkdir(parents=True, exist_ok=True)
    PHYLO.mkdir(parents=True, exist_ok=True)
    steps = {int(s) for s in args.steps.split(",") if s.strip()}

    con = connect()
    t0 = time.time()
    print("meme phylogenetics")
    if 1 in steps:
        step1_content_global(con, args.force)
    if 2 in steps:
        step2_tokens(con, args.force)
    if 3 in steps:
        step3_pairs(con, args.force)
    if 4 in steps:
        step4_families(con, args.force)
    if 5 in steps:
        step5_enrich(con, args.force)
    if 6 in steps:
        step6_trees(con, args.force)
    con.close()
    print(f"\ndone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
