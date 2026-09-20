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

  4. TREES. Strictly matched variants form the family backbone. Lower-confidence
     matches may attach only as terminal leaves, never as edges that merge two
     families. Within a family, sort variants by first appearance and attach
     each strict variant to whichever earlier strict variant it most resembles.
     The result is a rooted tree: the root is the earliest phrasing observed,
     edges carry the tokens that were added and dropped, and subtree spread
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
from pipeline.textnorm import (CONF_FROM, CONF_TO, EMOJI_RE2, EVASION_FROM,
                              NONLATIN_THRESHOLD, STOPWORDS, evasion_profile,
                              nonlatin_share, search_blob)

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
BLOCK_HEAD = 50       # of those, reserved for the most-copied, as before
BLOCK_STRATA = 8      # the rest are dealt round-robin across copy-count bands
RARE_DF = 20          # a shared token this rare is on its own enough to pair on
SIM_THRESHOLD = 0.45  # token-set Jaccard for "same narrative, reworded"
MIN_FAMILY = 3        # variants required to call something a lineage
MAX_FAMILY = 300      # single-linkage blobs get truncated to their top variants
MIN_FAMILY_EMISSIONS = 50   # enough observed spread to inspect, not just a stray trio
N_BUCKETS = 24        # tree files; the dashboard fetches one on demand

# Strict edges are allowed to join wordings into a family.  These settings are
# intentionally looser, but only for one-way leaf attachment to an already
# strict family.  A weak similarity can therefore increase recall without
# bridging two otherwise separate narratives into the same component.
LEAF_SIM_THRESHOLD = 0.35
LEAF_BLOCK_KEYS = 8
LEAF_RARE_DF = 50

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
              -- lang='en' is Twitter's guess and it is wrong in one direction
              -- in particular: hashtag-heavy posts in Thai, Korean and
              -- Japanese get tagged English because their Latin-script
              -- hashtags outweigh the body. Left alone, 11% of the resulting
              -- "English" variants carry non-Latin script. Drop the ones that
              -- are plainly not English prose; borderline cases survive and
              -- are labelled per-family at export.
              AND NOT regexp_matches(text, '\\p{{Thai}}|\\p{{Hangul}}|\\p{{Hiragana}}|\\p{{Katakana}}')
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
                       length(text) - length(translate(text, {sql_literal(EVASION_FROM)}, '')) AS obf_chars,
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
    # block. The cap has to exist -- a block of 50k messages is 10^9 pairs --
    # but *which* messages it keeps decides what the phylogeny can see.
    #
    # This used to keep the MAX_BLOCK most-copied messages per token, on the
    # reasoning that if we can only compare some of a crowded block we should
    # compare the ones that actually spread. That is right for a coordination
    # detector and backwards for a descent tree. A new mutation enters the
    # corpus with n_copies=2 and grows from there, so ranking a block purely by
    # spread evicts exactly the young wordings a mutation tree is about. In a
    # crowded block it evicts all of them: measured on a 1,000-message block,
    # top-N kept 100 already-spread wordings and 0 of the 700 that had been
    # copied twice. A message whose every blocking token sits in an overflowing
    # block is never compared to anything, so it cannot enter any lineage.
    #
    # Pure round-robin across copy-count bands overcorrects -- it starts
    # evicting the high-copy wordings that form the backbone a young variant
    # needs to attach *to*, and a pair needs both of its ends present. So the
    # block is split: BLOCK_HEAD slots still go to the most-copied, exactly as
    # before, and the remainder is dealt round-robin across log2(n_copies)
    # bands, richest band first. The backbone is preserved and the young band
    # gets a guaranteed share of what is left. An uncrowded block is unaffected
    # either way: everything fits.
    db.execute(f"""
        CREATE TABLE blocks AS
        SELECT tok_id, doc_id, n FROM (
            SELECT tok_id, doc_id, n,
                   row_number() OVER (PARTITION BY tok_id ORDER BY ord_key, doc_id) AS brn
            FROM (
                SELECT tok_id, doc_id, n,
                       CASE WHEN hrn <= {BLOCK_HEAD} THEN hrn
                            ELSE {BLOCK_HEAD} + srn * {BLOCK_STRATA}
                                 + ({BLOCK_STRATA} - 1 - stratum) END AS ord_key
                FROM (
                    SELECT d.tok_id, t.doc_id, d.n, k.stratum,
                           row_number() OVER (PARTITION BY d.tok_id
                                              ORDER BY k.n_copies DESC, t.doc_id) AS hrn,
                           row_number() OVER (PARTITION BY d.tok_id, k.stratum
                                              ORDER BY k.n_copies DESC, t.doc_id) AS srn
                    FROM (
                        SELECT doc_id, tok, rk FROM (
                            SELECT t.doc_id, t.tok,
                                   row_number() OVER (PARTITION BY t.doc_id ORDER BY d.n, t.tok) AS rk
                            FROM tk t JOIN df d USING (tok)
                        ) WHERE rk <= {BLOCK_KEYS}
                    ) t
                    JOIN df d USING (tok)
                    JOIN (
                        SELECT doc_id, n_copies,
                               least(greatest(floor(log2(greatest(n_copies, 1)))::INT - 1, 0),
                                     {BLOCK_STRATA} - 1) AS stratum
                        FROM dict
                    ) k ON k.doc_id = t.doc_id
                )
            )
        ) WHERE brn <= {MAX_BLOCK}
    """)
    nb, ndoc = db.execute(
        "SELECT count(*), count(DISTINCT doc_id) FROM blocks").fetchone()
    tot, tot_emis = db.execute("SELECT count(*), sum(n_copies) FROM dict").fetchone()
    print(f"      {nb:,} block postings covering {ndoc:,}/{tot:,} messages")

    # A message with no posting is invisible to everything downstream: it can
    # never be paired, so it can never join a family, so it can never reach the
    # export. That makes this the pipeline's largest silent filter, and it is
    # worth printing rather than inferring from a shortfall two stages later.
    miss, miss_emis = db.execute("""
        SELECT count(*), coalesce(sum(d.n_copies), 0)
        FROM dict d
        LEFT JOIN (SELECT DISTINCT doc_id FROM blocks) b USING (doc_id)
        WHERE b.doc_id IS NULL
    """).fetchone()
    if miss:
        print(f"      {miss:,} messages ({100 * miss / tot:.1f}%) got no blocking key -- "
              f"{miss_emis:,} emissions ({100 * miss_emis / tot_emis:.1f}%) unreachable")

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


def attach_lower_confidence_leaves(strict, alias_path: Path):
    """Attach weaker matches to strict families without letting them merge.

    The strict pair graph is deliberately left untouched: only its connected
    components define a family.  This pass asks whether a wording outside that
    graph has one reasonably similar, earlier wording inside a strict family.
    If it does, it becomes a leaf of that one anchor.  It can never supply an
    edge between families or become another node's parent.
    """
    import pandas as pd

    if strict.empty:
        strict["attach_to"] = None
        strict["attach_sim"] = float("nan")
        strict["is_leaf"] = False
        return strict

    strict_path = EVO / "strict-families.parquet"
    db_path = EVO / "leaf-attachments.duckdb"
    strict.to_parquet(strict_path, index=False)
    db_path.unlink(missing_ok=True)
    db = duckdb.connect(str(db_path))
    db.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    db.execute(f"SET threads={C.THREADS}")
    db.execute(f"SET temp_directory='{C.TMP_DIR}'")
    db.execute("SET preserve_insertion_order=false")

    print("      attaching lower-confidence leaves to strict families...")
    try:
        db.execute(f"CREATE VIEW docs AS SELECT * FROM '{EVO}/tokens.parquet'")
        db.execute(f"CREATE VIEW strict AS SELECT * FROM '{strict_path}'")
        db.execute(f"CREATE VIEW aliases AS SELECT * FROM '{alias_path}'")
        db.execute("""
            CREATE TABLE seed AS
            SELECT s.content_key, s.family_id, d.tokens, d.n_copies, d.first_seen
            FROM strict s JOIN docs d USING (content_key)
        """)
        # Recompute global token frequency.  This is the same vocabulary-aware
        # blocking idea as step 3, but the target side is only the strict seed
        # set, so it remains tractable without a per-block popularity cap.
        db.execute("""
            CREATE TABLE df AS
            SELECT tok, count(*)::INT AS n
            FROM docs CROSS JOIN UNNEST(tokens) AS t(tok)
            GROUP BY tok
            HAVING count(*) BETWEEN 2 AND 2000
        """)
        db.execute(f"""
            CREATE TABLE leaf_blocks AS
            SELECT content_key, tok, n FROM (
                SELECT d.content_key, t.tok, f.n,
                       row_number() OVER (
                           PARTITION BY d.content_key ORDER BY f.n, t.tok
                       ) AS rk
                FROM docs d
                CROSS JOIN UNNEST(d.tokens) AS t(tok)
                JOIN df f ON f.tok = t.tok
                WHERE NOT EXISTS (SELECT 1 FROM strict s
                                  WHERE s.content_key = d.content_key)
                  -- A platform-truncated copy belongs to its full text, not
                  -- to a second circle beside it.
                  AND NOT EXISTS (SELECT 1 FROM aliases a
                                  WHERE a.content_key = d.content_key)
            ) WHERE rk <= {LEAF_BLOCK_KEYS}
        """)
        db.execute("""
            CREATE TABLE seed_blocks AS
            SELECT s.content_key, s.family_id, t.tok
            FROM seed s
            CROSS JOIN UNNEST(s.tokens) AS t(tok)
            JOIN df f ON f.tok = t.tok
        """)
        db.execute(f"""
            CREATE TABLE candidates AS
            SELECT l.content_key AS leaf, s.content_key AS anchor, s.family_id,
                   count(*)::INT AS shared, min(l.n)::INT AS rarest
            FROM leaf_blocks l
            JOIN seed_blocks s USING (tok)
            JOIN docs ld ON ld.content_key = l.content_key
            JOIN seed sd ON sd.content_key = s.content_key
            WHERE ld.first_seen >= sd.first_seen
            GROUP BY 1, 2, 3
            HAVING count(*) >= 2 OR min(l.n) <= {LEAF_RARE_DF}
        """)
        db.execute(f"""
            CREATE TABLE scored AS
            SELECT * FROM (
                SELECT c.leaf, c.anchor, c.family_id, l.n_copies,
                       len(list_intersect(l.tokens, s.tokens))::DOUBLE /
                         (len(l.tokens) + len(s.tokens) -
                          len(list_intersect(l.tokens, s.tokens))) AS attach_sim
                FROM candidates c
                JOIN docs l ON l.content_key = c.leaf
                JOIN seed s ON s.content_key = c.anchor
            ) WHERE attach_sim >= {LEAF_SIM_THRESHOLD}
        """)
        n_cand, n_scored = db.execute(
            "SELECT (SELECT count(*) FROM candidates), (SELECT count(*) FROM scored)"
        ).fetchone()
        print(f"      {n_cand:,} leaf candidates, {n_scored:,} pass similarity "
              f"{LEAF_SIM_THRESHOLD:.2f}")

        # Choose one best strict anchor per leaf, then respect the existing
        # maximum tree size by using any spare slots for the most-spread leaves.
        db.execute(f"""
            CREATE TABLE attachments AS
            WITH best AS (
                SELECT *, row_number() OVER (
                    PARTITION BY leaf
                    ORDER BY attach_sim DESC, n_copies DESC, anchor
                ) AS pick
                FROM scored
            ), capacity AS (
                SELECT family_id, {MAX_FAMILY} - count(*) AS room
                FROM strict GROUP BY family_id
            ), ranked AS (
                SELECT b.*, c.room,
                       row_number() OVER (
                           PARTITION BY b.family_id
                           ORDER BY b.n_copies DESC, b.attach_sim DESC, b.leaf
                       ) AS family_rank
                FROM best b JOIN capacity c USING (family_id)
                WHERE b.pick = 1
            )
            SELECT leaf AS content_key, family_id, anchor AS attach_to, attach_sim,
                   true AS is_leaf
            FROM ranked
            WHERE family_rank <= room
        """)
        attached = db.execute("SELECT * FROM attachments").fetchdf()
        print(f"      attached {len(attached):,} leaves; strict edges still define every family")
    finally:
        db.close()
        db_path.unlink(missing_ok=True)
        strict_path.unlink(missing_ok=True)

    strict = strict.copy()
    strict["attach_to"] = None
    strict["attach_sim"] = float("nan")
    strict["is_leaf"] = False
    return pd.concat([strict, attached], ignore_index=True)


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
    alias_path = EVO / "alias.parquet"
    alias.to_parquet(alias_path, index=False)

    # Keep strict connected components as the family backbone.  Broader
    # matching is only allowed to add terminal leaves to that backbone.
    fam = attach_lower_confidence_leaves(fam, alias_path)
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
        SELECT f.content_key AS canonical, f.family_id, f.attach_to, f.attach_sim,
               f.is_leaf, f.content_key AS raw
        FROM fam f
        UNION ALL
        SELECT a.canonical, f.family_id, f.attach_to, f.attach_sim, f.is_leaf,
               a.content_key AS raw
        FROM alias a JOIN fam f ON f.content_key = a.canonical
    """)
    con.execute(f"""
        COPY (
            WITH ev AS (
                SELECT k.canonical, k.family_id, k.attach_to, k.attach_sim, k.is_leaf,
                       e.author_id, e.created_at
                FROM read_parquet('{C.EVENTS}/*.parquet') e
                JOIN keys k ON k.raw = e.content_key
                WHERE e.lang = 'en'
            ),
            agg AS (
                SELECT e.canonical, e.family_id,
                       any_value(e.attach_to)         AS attach_to,
                       any_value(e.attach_sim)        AS attach_sim,
                       any_value(e.is_leaf)           AS is_leaf,
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
# UI says so -- its only job is to make thousands of families navigable.
#
# Terms marked "!" are ANCHORS: specific enough that their presence is about
# the topic rather than coincident with it. Unmarked terms only CORROBORATE.
# A tag requires an anchor plus one more term, because one word is a
# coincidence and two is a subject -- see classify().
#
# Anchor status is about ambiguity, not importance. "woke" is unmarked
# because "I woke up" is ordinary English and used to file whole lineages
# under identity on the strength of a single tweet; "vote" is unmarked
# because fandoms vote in awards polls constantly; "alien" is unmarked
# because of science fiction. Proper nouns are anchors because almost
# nothing else produces them.
TOPICS = {
    "politics": "trump! biden! election! vote voter ballot! congress! senate! president "
                "campaign democrat! republican! maga! conservative liberal government "
                "policy impeach!",
    "conflict": "israel! palestine! gaza! hamas! idf! zionist! genocide! ukraine! russia! "
                "war strike military airstrike! ceasefire! hostage occupation settler",
    "migration": "immigrant! immigration! migrant! refugee! asylum! border deport! "
                 "deportation! illegal alien invasion assimilate visa amnesty!",
    "identity": "muslim! islam! jew! jewish! christian racist! racism! antisemitic! "
                "islamophobia! sharia! woke dei trans lgbtq groomer! white black",
    "health": "vaccine! vaccinated! covid! pandemic! fauci! cdc! fda mrna! autism pharma "
              "outbreak measles! medicaid!",
    "crypto": "bitcoin! btc! crypto! ethereum! token airdrop! presale! wallet trading "
              "forex! gold xauusd! signal pump profit",
    "fandom": "comeback teaser! album mv kpop! bts! nct! concert fancam! stan lightstick! "
              "preorder weverse! photocard! debut tour",
    "sports": "match goal league season transfer fixture cricket! football! nba! ufc! fifa!",
}

# A keyword has to show up in this many of a family's variants before it
# counts. The old code unioned the tokens of every variant and tested that,
# so one tweet reading "I woke up to god" tagged all 226 wordings of a
# cosmetics campaign as identity politics. A lineage is a set of rewordings
# of the same claim: a term that is genuinely part of the claim survives the
# rewording, and a term that appears once does not.
MIN_TOPIC_DF = 2
MIN_TOPIC_SHARE = 0.05
MAX_TOPIC_DF = 4   # cap, so a 200-wording lineage is not held to a 10-term bar


def _parse(spec: str) -> dict[str, int]:
    out = {}
    for w in spec.split():
        out[w.rstrip("!")] = 2 if w.endswith("!") else 1
    return out


TOPIC_SETS = {k: _parse(v) for k, v in TOPICS.items()}


def _df_floor(n_variants: int) -> int:
    """How many wordings a term must survive into before it counts."""
    return max(MIN_TOPIC_DF, min(MAX_TOPIC_DF,
                                 int(n_variants * MIN_TOPIC_SHARE + 0.999)))


def classify(tok_df: dict, n_variants: int) -> list[str]:
    """Tag a family from the token->variant-count map of its wordings.

    Two gates, and they fix different halves of the same failure.

    Persistence: a term has to appear in several wordings. A lineage is a set
    of rewordings of one claim, so a term that is part of the claim survives
    the rewording. This is what stops one tweet in two hundred from setting
    the label.

    Anchors: the surviving terms have to include one that is unambiguous.
    Weak terms can no longer combine into a tag on their own -- "white" plus
    "black" in a description of an outfit used to score exactly as high as
    "antisemitic" plus "sharia". They still rank a topic once an anchor has
    earned it, so the two-term cases that are genuine keep their ordering.
    """
    floor = _df_floor(n_variants)
    hits = []
    for topic, terms in TOPIC_SETS.items():
        kept = [(tok, w) for tok, w in terms.items() if tok_df.get(tok, 0) >= floor]
        if not any(w == 2 for _, w in kept):
            continue
        hits.append((topic, sum(w for _, w in kept)))
    hits.sort(key=lambda x: -x[1])
    return [k for k, _ in hits[:2]]


# --- behaviour classes ------------------------------------------------------
#
# Topic says what a lineage is about. It says nothing about whether the
# spread was authentic, and the two get confused constantly: the most
# "notable" lineages in this corpus are Thai and Korean entertainment promo,
# which is organised and inauthentic-looking by every structural measure
# while being neither covert nor political.
#
# So classify the *behaviour* on a separate axis, from signals the pipeline
# already computes. Each class is a distinct mechanism, and the evidence for
# each is reported alongside it so a judge can disagree with the call:
#
#   farm     engagement farming. Giveaways, airdrops, follow-and-retweet.
#            Inauthentic and openly so; the payload is the instruction.
#   evade    moderation evasion. Words carrying cross-script lookalikes, so
#            the text reads normally and does not match a keyword filter.
#   promo    promotional template. A conserved hashtag block with variable
#            free text -- scheduled marketing and fan-campaign material.
#            Coordinated by construction, but disclosed and commercial.
#   burst    synchronised burst. Near-identical wordings appearing from many
#            distinct upstream sources inside a tight window, spread by
#            accounts that co-retweet each other elsewhere. This is the
#            coordinated-inauthentic-behaviour *shortlist*, not a verdict:
#            inspected by hand, most of what it catches in this corpus is
#            football transfer aggregators racing the same scoop, which has
#            the identical structure and is entirely legitimate. It also
#            catches the two state-politics press campaigns in the corpus,
#            which is the reason to keep it. Read it as "worth a look".
#   organic  no structural evidence of any of the above.
#
# Source dispersion is what makes that class mean anything. A fandom lineage
# where one official account is retweeted 10,000 times is a broadcast, and it
# used to land here because a broadcast is also fast and also travels through
# a dense mutually-following community. Requiring the near-identical text to
# come from *several* upstream accounts is what separates "one post went
# viral" from "many accounts published the same thing at once".
#
# Thresholds sit near the top of each observed distribution rather than at
# round numbers, and are listed here so they can be argued with.
FARM_TERMS = frozenset("""
    giveaway giveway airdrop presale whitelist winner prize enter claim mint
    referral bonus deposit withdraw retweet follow followers subscribe
    tag friends free join dm signal pump profit forex trading spots spot
""".split())

FARM_MIN_TERMS = 4      # 4+ of the above co-occurring is a solicitation
EVADE_MIN_MIXED = 0.15  # mixed-script words per variant...
EVADE_MIN_TOTAL = 3     # ...and enough of them that it is not one odd wording
PROMO_MIN_HT = 1.5      # mean hashtags per variant (corpus p90 = 2.3)
PROMO_HANDLE_HT = 0.8   # ...or a lower hashtag load from one dominant source
PROMO_MIN_SHARE = 0.85
BURST_MIN_PEAK6 = 0.75   # share of spread inside its busiest 6 hours (p90)
BURST_MIN_COORD = 0.10   # share of accounts in a stage-2 co-retweet cluster (p90)
BURST_MIN_ACC = 250      # too few accounts to call it a network
BURST_MAX_HSHARE = 0.50  # one source above this is a broadcast, not a chorus


def behaviour(*, tok_df: dict, nv: int, ht: float, mixed: int, coord_share: float,
              acc: int, peak6: float, handle_share: float) -> tuple[str, list[str]]:
    """Return (class, human-readable evidence) for one family.

    Order matters. A crypto giveaway also has a hashtag block, and a promo
    template can also burst; the earlier tests name the more specific
    mechanism, and the evidence list keeps the losing signals visible.
    """
    ev = []
    floor = _df_floor(nv)
    farm_hits = sorted(t for t in FARM_TERMS if tok_df.get(t, 0) >= floor)
    mixed_rate = mixed / max(1, nv)

    if len(farm_hits) >= FARM_MIN_TERMS:
        return "farm", [f"solicitation terms: {', '.join(farm_hits[:6])}"]
    if mixed_rate >= EVADE_MIN_MIXED and mixed >= EVADE_MIN_TOTAL:
        return "evade", [f"{mixed_rate:.2f} mixed-script words per wording",
                         f"{mixed} in total"]
    if ht >= PROMO_MIN_HT or (ht >= PROMO_HANDLE_HT and handle_share >= PROMO_MIN_SHARE):
        ev.append(f"{ht:.1f} hashtags per wording")
        if handle_share >= PROMO_MIN_SHARE:
            ev.append(f"{handle_share:.0%} of spread from one account")
        return "promo", ev
    if (peak6 >= BURST_MIN_PEAK6 and coord_share >= BURST_MIN_COORD
            and acc >= BURST_MIN_ACC and handle_share <= BURST_MAX_HSHARE):
        return "burst", [f"{peak6:.0%} of spread in 6 hours",
                         f"{acc:,} accounts, no single source above "
                         f"{handle_share:.0%}",
                         f"{coord_share:.0%} of accounts co-retweet elsewhere"]
    return "organic", []


def peak_window(hours, counts, width: int = 6) -> float:
    """Largest share of a family's spread falling in any `width`-hour window."""
    import numpy as np
    total = float(sum(counts))
    if total <= 0 or len(hours) == 0:
        return 0.0
    lo, hi = int(min(hours)), int(max(hours))
    dense = np.zeros(hi - lo + 1)
    for h, c in zip(hours, counts):
        dense[int(h) - lo] += c
    if len(dense) <= width:
        return 1.0
    return float(np.convolve(dense, np.ones(width), "valid").max() / total)


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

    # An alias reattribution can very occasionally move a strict wording's
    # observed first-seen time after a leaf selected from the unaliased table.
    # Do not draw a time-reversed edge (or let that leaf become a root): such a
    # match is simply omitted from this export.
    anchor_time = v[["canonical", "first_seen"]].rename(
        columns={"canonical": "attach_to", "first_seen": "anchor_first_seen"}
    )
    v = v.merge(anchor_time, on="attach_to", how="left")
    invalid_leaf = (v.is_leaf &
                    (v.anchor_first_seen.isna() | (v.first_seen < v.anchor_first_seen)))
    if invalid_leaf.any():
        print(f"      omitting {int(invalid_leaf.sum()):,} time-reversed leaf attachments")
        v = v[~invalid_leaf].copy()
    v = v.drop(columns=["anchor_first_seen"])

    # Only families with enough structure to be worth drawing a tree for.
    keep = v.groupby("family_id").agg(nv=("canonical", "size"),
                                      emis=("n_emissions", "sum"))
    keep = keep[(keep.nv >= MIN_FAMILY) & (keep.emis >= MIN_FAMILY_EMISSIONS)]
    v = v[v.family_id.isin(keep.index)]
    print(f"      {len(keep):,} families qualify ({len(v):,} variants)")

    # Evasion is recomputed here from the message text rather than read from
    # the column step 2 wrote. That column counted every character the
    # normalizer folds, which is dominated by curly apostrophes and by the
    # ellipsis Twitter appends to a truncated retweet -- so it ranked
    # "was this tweet cut off" and called it obfuscation. Step 2 now uses the
    # narrower set too, but recomputing costs a second over 10^5 variants and
    # means an existing build does not have to be thrown away to get it right.
    prof = v.text.map(evasion_profile)
    v["ev_styled"] = [d["styled"] for d in prof]
    v["ev_homo"] = [d["homo"] for d in prof]
    v["ev_mixed"] = [d["mixed"] for d in prof]

    # Twitter's own lang field is what selected this corpus, and it is wrong
    # often enough to matter: a Thai or Korean post whose Latin-script
    # hashtags outweigh its body is routinely tagged "en". Those lineages are
    # real and worth keeping -- but they cluster on their hashtag block, not
    # on a reworded claim, so they answer a different question. Label them so
    # the dashboard can separate them instead of quietly presenting them as
    # English narrative drift.
    v["nonlatin"] = v.text.map(nonlatin_share)

    t0_global = v.first_seen.min()
    cliff_h = int((pd.Timestamp(COLLECTION_CLIFF) - t0_global).total_seconds() // 3600)
    tl_by_fam = {k: g for k, g in tl[tl.family_id.isin(keep.index)].groupby("family_id")}

    index, buckets = [], {}
    for fid, g in v.groupby("family_id", sort=False):
        # Strict nodes win ties on first-seen time, guaranteeing that a leaf's
        # recorded anchor always precedes it in the exported tree.
        g = g.sort_values(["first_seen", "is_leaf", "n_emissions"],
                          ascending=[True, True, False]).reset_index(drop=True)
        toks = [set(x) for x in g.tokens]
        n = len(g)

        # Attach each variant to the earlier variant it most resembles. Time
        # gives the edges their direction -- a phrasing cannot descend from one
        # that did not exist yet -- and similarity picks which ancestor. The
        # root is simply the earliest phrasing we observed, which is a claim
        # about this corpus and this month, not about the origin of the idea.
        parent = [-1] * n
        psim = [0.0] * n
        node_idx = {str(k): i for i, k in enumerate(g.canonical)}
        for i in range(1, n):
            r = g.iloc[i]
            if bool(r.is_leaf) and r.attach_to is not None:
                anchor = node_idx.get(str(r.attach_to))
                if anchor is not None and anchor < i:
                    parent[i] = anchor
                    psim[i] = float(r.attach_sim)
                    continue
            best, bj = -1, -1.0
            ti = toks[i]
            for k in range(i):
                # A lower-confidence leaf is terminal by design.  It may
                # hang from the strict backbone but can never redirect it.
                if bool(g.iloc[k].is_leaf):
                    continue
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
                "leaf": bool(r.is_leaf),
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
                "obf": int(r.ev_styled + r.ev_homo),
                "mix": int(r.ev_mixed),
                "emo": int(r.n_emoji),
                "drift": round(1 - (len(toks[i] & root_tok) /
                                    max(1, len(toks[i] | root_tok))), 3),
                # Pandas represents a missing handle as float NaN.  NaN is
                # truthy, but it is not valid JSON and makes a whole on-demand
                # tree bucket impossible for the browser to parse.
                "src": r.rt_handle if isinstance(r.rt_handle, str) and r.rt_handle else None,
                "spark": spark,
            })

        all_tok = set().union(*toks) if toks else set()
        # How many *wordings* each token survives into, not how many exist in
        # the family overall. classify() needs the former; a term that shows
        # up in one wording out of two hundred is not what the lineage says.
        tok_df: dict[str, int] = {}
        for ts in toks:
            for tok in ts:
                tok_df[tok] = tok_df.get(tok, 0) + 1

        emis = int(g.n_emissions.sum())
        acc = int(g.n_accounts.sum())
        co = int(g.n_coord_accounts.sum())
        handles = (g[g.rt_handle.notna()].groupby("rt_handle").n_emissions.sum()
                   .sort_values(ascending=False).head(5))
        handle_share = (float(handles.iloc[0]) / emis) if len(handles) and emis else 0.0

        fam_tl_all = tl_by_fam.get(fid)
        if fam_tl_all is not None and len(fam_tl_all):
            hrs = ((fam_tl_all.hr - t0_global).dt.total_seconds() // 3600).astype(int)
            peak6 = peak_window(hrs.to_numpy(), fam_tl_all.n.to_numpy())
        else:
            peak6 = 0.0

        mixed = int(g.ev_mixed.sum())
        coord_share = round(co / acc, 3) if acc else 0.0
        ht_mean = round(float(g.n_hashtags.mean()), 2)
        kind, evidence = behaviour(
            tok_df=tok_df, nv=n, ht=ht_mean, mixed=mixed, coord_share=coord_share,
            acc=acc, peak6=peak6, handle_share=handle_share)
        # Weight by spread: one stray non-English wording in a large lineage
        # should not relabel it, and a lineage that is mostly non-English
        # should be caught even if its wordings are individually short.
        nonlatin = float((g.nonlatin * g.n_emissions).sum() / emis) if emis else 0.0
        fam = {
            "id": int(fid),
            "nv": n,
            "emis": emis,
            "acc": acc,
            "co": co,
            "coord_share": coord_share,
            "t0": int((g.first_seen.min() - t0_global).total_seconds() // 3600),
            "t1": int((g.last_seen.max() - t0_global).total_seconds() // 3600),
            "depth": int(depth.max()),
            "drift": round(float(np.mean([nd["drift"] for nd in nodes])), 3),
            "obf": int(g.ev_styled.sum() + g.ev_homo.sum()),
            # Styled characters (math-bold and friends) are reported, not
            # charged. They defeat a naive keyword match, but inspection says
            # they are decorative here -- headline emphasis and idol promo --
            # so calling them evasion would be inventing a finding.
            "sty": int(g.ev_styled.sum()),
            "mix": mixed,
            # Behaviour class and the numbers behind it. Separate from topic:
            # topic says what a lineage argues, this says how it travelled.
            "kind": kind,
            "why": evidence,
            "peak6": round(peak6, 3),
            "hshare": round(handle_share, 3),
            # Share of spread whose text is not Latin script. Non-zero means
            # Twitter's lang field put a non-English lineage in an English
            # corpus, and that its tree was built from hashtags alone.
            "nonlatin": round(nonlatin, 3),
            "offlang": bool(nonlatin >= NONLATIN_THRESHOLD),
            # Mean hashtags per variant separates the two kinds of lineage this
            # method finds: a prose narrative reworded by people (low) and a
            # promo template whose hashtag block is the conserved part and whose
            # free text is the variable region (high). Both are real mutation;
            # only one is the kind this project is about.
            "ht": ht_mean,
            "topics": classify(tok_df, n),
            "win": ("aug" if int((g.last_seen.max() - t0_global).total_seconds() // 3600) < cliff_h
                    else "sep" if int((g.first_seen.min() - t0_global).total_seconds() // 3600) >= cliff_h
                    else "span"),
            "root": nodes[0]["txt"][:180],
            "top": g.sort_values("n_emissions", ascending=False).iloc[0].text[:180],
            "handles": [[h, int(c)] for h, c in handles.items()],
            # A short phrase-preserving blob, so the board is searchable from
            # index.json alone before search.json arrives. Stage 8 replaces it
            # with the full-recall version; this is the fallback, not the
            # feature. Ordered most-emitted first because the cap cuts the tail.
            "q": search_blob(
                [nd["txt"] for nd in sorted(nodes, key=lambda nd: -nd["n"])], 320),
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
        "kinds": {k: sum(1 for f in index if f["kind"] == k)
                  for k in ("farm", "evade", "promo", "burst", "organic")},
        "offlang": sum(1 for f in index if f["offlang"]),
        "cliff_h": cliff_h,
        "cliff_note": "Crawl volume drops 15.3x on 2026-09-01 (22.0M tweets/day "
                      "before, 1.4M/day after). Counts either side are not comparable.",
        "params": {
            "sim_threshold": SIM_THRESHOLD, "min_copies": MIN_COPIES,
            "block_keys": BLOCK_KEYS, "max_df": MAX_DF, "max_block": MAX_BLOCK,
            "block_head": BLOCK_HEAD, "block_strata": BLOCK_STRATA,
            "min_family": MIN_FAMILY, "min_family_emissions": MIN_FAMILY_EMISSIONS,
            "leaf_sim_threshold": LEAF_SIM_THRESHOLD,
            "leaf_block_keys": LEAF_BLOCK_KEYS, "leaf_rare_df": LEAF_RARE_DF,
            "topic_min_df": MIN_TOPIC_DF, "topic_min_share": MIN_TOPIC_SHARE,
            "burst_min_peak6": BURST_MIN_PEAK6, "burst_min_coord": BURST_MIN_COORD,
            "burst_max_hshare": BURST_MAX_HSHARE,
            "promo_min_ht": PROMO_MIN_HT, "evade_min_mixed": EVADE_MIN_MIXED,
            "nonlatin_threshold": NONLATIN_THRESHOLD,
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
