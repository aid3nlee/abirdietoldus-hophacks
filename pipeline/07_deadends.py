#!/usr/bin/env python3
"""Find the mutations that died: rewordings emitted once and never copied.

Why this matters
----------------
Stage 4 starts at MIN_COPIES=2, with the comment "a string emitted once is not
yet a meme". That is the right call for finding coordinated networks -- a
wording nobody repeated carries no coordination signal -- but it is the wrong
call for evolution, and it is the single biggest thing standing between this
project and the memetics claim it makes.

Selection needs variants that lost. A tree built only from wordings that spread
shows which phrasings won and can never show what they beat, which is
survivorship bias designed into the sample. 47,370,164 English messages in this
corpus were emitted exactly once. Most are unrelated to anything. Some are
somebody rewording a live narrative in a way that went nowhere, and those are
the failed mutations.

What a dead end is here
-----------------------
A message emitted exactly once, appearing no earlier than a variant already in
a lineage, and similar enough to that variant to be a rewording of it rather
than a different message that shares vocabulary. Same similarity rule the leaf
pass uses (Jaccard >= 0.35), same blocking logic as stage 4, same direction of
time: a mutation cannot precede what it mutated from.

Why it is scoped to a few lineages
----------------------------------
Measured, not assumed. Blocking 47.4M singletons against all 10,674 exported
lineages does not reduce anything -- 45.4M of them share two-plus tokens with
*some* lineage, because with ten thousand diverse lineages in the pool almost
any English sentence matches one. Restricting the blocking vocabulary to
globally rare tokens (stage 4's MAX_DF) collapses that to 2.9M candidates, and
scoping to a few hundred lineages keeps the pass honest and quick.

That is also the project's own scope discipline: depth on a handful of case
studies beats shallow coverage of everything. The lineages you demo are the
lineages that need their dead ends.

What it writes
--------------
Per node, back into the existing export:
    dx   how many once-only rewordings descend from this phrasing
    dxs  a few of them, verbatim, for the inspector
and per family, `dx` as the lineage total.

This does not touch the phylogeny. It adds a count and some text to an export
that already exists, so it is safe to re-run and cannot invalidate a demo.

    ./run.sh deadends                 # top 200 showcase lineages
    ./run.sh deadends --families 500
    ./run.sh deadends --ids 2823,5703
"""

from __future__ import annotations

import argparse
import collections
import glob
import importlib.util
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

import config as C
from pipeline.textnorm import normalize

EVO = C.DATA / "evolution"
PHYLO = C.EXPORT / "phylo"

# Borrowed verbatim from stage 4 so the two passes agree about what "similar"
# and "rare" mean. Importing rather than restating keeps them from drifting.
_spec = importlib.util.spec_from_file_location(
    "evo4", Path(__file__).resolve().parent / "04_evolution.py")
_evo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_evo)

MAX_DF = _evo.MAX_DF                      # 2000: above this a token is not distinctive
SIM = _evo.LEAF_SIM_THRESHOLD             # 0.35: a rewording, not a different message
MIN_TOKENS = _evo.MIN_TOKENS              # 5
MIN_SHARED = 2                            # rare tokens a candidate must share
CHUNKS = 32                               # bounds memory on the 47M-row scan
EXAMPLES = 6                              # dead ends kept verbatim per node

# Cheap normalisation, used only to decide which singletons are worth looking
# at. Deliberately not norm_expr(): that does entity decoding, confusable
# folding, URL and handle stripping across 47M rows and is far too slow for a
# filter whose only job is to discard the 94% that match nothing. Survivors get
# the real normalisation afterwards.
CHEAP = (r"trim(regexp_replace(lower(regexp_replace({c},'[^A-Za-z0-9'' ]+',' ','g')),"
         r"'\s+',' ','g'))")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute("SET threads=6")          # the scan is memory-bound, not CPU-bound
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def load_trees() -> tuple[dict, dict[str, dict]]:
    idx_path = PHYLO / "index.json"
    if not idx_path.exists():
        sys.exit(f"no export at {idx_path} -- run ./run.sh evolution first")
    index = json.loads(idx_path.read_text())
    trees = {}
    for fp in sorted(glob.glob(str(PHYLO / "trees-*.json"))):
        trees[fp] = json.loads(Path(fp).read_text())
    return index, trees


def pick_families(index: dict, args) -> set[int]:
    """Which lineages get their dead ends traced."""
    if args.ids:
        return {int(x) for x in args.ids.split(",") if x.strip()}
    fams = [f for f in index["families"]
            if f["emis"] >= args.min_emis and f["nv"] >= args.min_variants]
    if not args.include_promo:
        fams = [f for f in fams if f["kind"] != "promo" and not f.get("offlang")]
    fams.sort(key=lambda f: -f["emis"])
    return {int(f["id"]) for f in fams[:args.families]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--families", type=int, default=200)
    ap.add_argument("--ids", default="", help="explicit lineage ids, comma separated")
    ap.add_argument("--min-emis", type=int, default=500)
    ap.add_argument("--min-variants", type=int, default=8)
    ap.add_argument("--include-promo", action="store_true")
    args = ap.parse_args()

    t_all = time.time()
    print("dead ends -- once-only rewordings that never spread")
    index, trees = load_trees()
    want = pick_families(index, args)
    corpus_t0 = __import__("datetime").datetime.fromisoformat(index["meta"]["t0"])

    # --- 1. seed straight from the exported tree nodes ----------------------
    # Seeding from the export rather than from variants.parquet avoids having
    # to map one back to the other: 05_enrich rewrites truncated node text, so
    # only 59% of nodes still match their source row by text. The nodes on
    # screen are the nodes we attach to.
    print(f"  [1] tokenising nodes of {len(want):,} lineages...")
    rows = []
    node_meta: dict[tuple[int, int], dict] = {}
    for fp, fams in trees.items():
        for fid, fam in fams.items():
            if int(fid) not in want:
                continue
            for n in fam["nodes"]:
                toks = normalize(n["txt"] or "")["tokens"]
                if len(toks) < MIN_TOKENS:
                    continue
                node_meta[(int(fid), n["i"])] = {"file": fp, "t0": n.get("t0", 0)}
                for tok in toks:
                    rows.append((int(fid), n["i"], tok, len(toks)))
    if not rows:
        sys.exit("no usable nodes in the selected lineages")
    print(f"      {len(node_meta):,} nodes, {len(rows):,} node-token pairs")

    con = connect()
    con.execute("CREATE TABLE nodetok(family_id BIGINT, i INT, tok VARCHAR, ntok INT)")
    con.executemany("INSERT INTO nodetok VALUES (?,?,?,?)", rows)
    con.execute("""CREATE TABLE nodeinfo AS
                   SELECT family_id, i, any_value(ntok) AS ntok FROM nodetok GROUP BY 1,2""")

    # --- 2. blocking vocabulary: seed tokens that are globally rare ---------
    print("  [2] building rare-token blocking vocabulary...")
    t = time.time()
    con.execute(f"""CREATE TABLE gdf AS
        SELECT t.tok, count(*)::INT AS df
        FROM '{EVO / 'tokens.parquet'}' d CROSS JOIN UNNEST(d.tokens) AS t(tok)
        GROUP BY t.tok""")
    con.execute(f"""CREATE TABLE sv AS
        SELECT n.tok, any_value(g.df)::INT AS df
        FROM nodetok n JOIN gdf g ON g.tok = n.tok
        WHERE g.df <= {MAX_DF}
        GROUP BY n.tok""")
    nv = con.execute("SELECT count(*) FROM sv").fetchone()[0]
    print(f"      {nv:,} rare tokens (global df <= {MAX_DF})  ({time.time() - t:.0f}s)")
    if nv == 0:
        sys.exit("no rare tokens in the selected lineages -- nothing to match against")

    # --- 3. scan the singletons --------------------------------------------
    print(f"  [3] scanning once-only messages in {CHUNKS} chunks...")
    t = time.time()
    cg = C.DATA / "normalized" / "content_global.parquet"
    con.execute("CREATE TABLE cand(content_key VARCHAR, shared INT)")
    for ch in range(CHUNKS):
        con.execute(f"""
            INSERT INTO cand
            SELECT content_key, count(*)::INT FROM (
                SELECT c.content_key, t.tok
                FROM '{cg}' c
                CROSS JOIN UNNEST(string_split({CHEAP.format(c='c.text')}, ' ')) AS t(tok)
                WHERE c.n_copies = 1 AND hash(c.content_key) % {CHUNKS} = {ch}
            ) x JOIN sv v ON v.tok = x.tok
            GROUP BY content_key HAVING count(*) >= {MIN_SHARED}
        """)
    n_cand = con.execute("SELECT count(*) FROM cand").fetchone()[0]
    print(f"      {n_cand:,} candidates  ({time.time() - t:.0f}s)")

    # --- 4. real normalisation on survivors only ---------------------------
    print("  [4] normalising candidates properly...")
    t = time.time()
    stop = "[" + ",".join(_evo.sql_literal(w) for w in sorted(_evo.STOPWORDS)) + "]"
    con.execute(f"""
        CREATE TABLE ctok AS
        SELECT content_key, text, first_seen, tokens, len(tokens) AS ntok FROM (
            SELECT c.content_key, c.text, c.first_seen,
                   list_sort(list_distinct(list_filter(
                       string_split({_evo.norm_expr('c.text')}, ' '),
                       w -> length(w) >= 2 AND NOT list_contains({stop}, w)))) AS tokens
            FROM '{cg}' c JOIN cand USING (content_key)
        ) WHERE len(tokens) >= {MIN_TOKENS}
    """)
    n_tok = con.execute("SELECT count(*) FROM ctok").fetchone()[0]
    print(f"      {n_tok:,} usable  ({time.time() - t:.0f}s)")

    # --- 5. score against the nodes ----------------------------------------
    # Pair only through shared rare tokens, then score exact token-set Jaccard,
    # exactly as stage 4 does. Time is a hard gate, not a tiebreak: a rewording
    # cannot predate the phrasing it is a rewording of.
    print("  [5] scoring against tree nodes...")
    t = time.time()
    con.execute(f"""
        CREATE TABLE scored AS
        SELECT * FROM (
            SELECT c.content_key, p.family_id, p.i,
                   len(list_intersect(c.tokens, n.toks))::DOUBLE /
                     (c.ntok + n.ntok - len(list_intersect(c.tokens, n.toks))) AS sim,
                   c.first_seen, c.text,
                   -- Rank by the similarity itself, not by raw shared-token
                   -- count: a long node shares more tokens with everything,
                   -- and picking it first would hide a shorter node that is
                   -- the actual match.
                   row_number() OVER (
                       PARTITION BY c.content_key
                       ORDER BY len(list_intersect(c.tokens, n.toks))::DOUBLE /
                                (c.ntok + n.ntok - len(list_intersect(c.tokens, n.toks))) DESC,
                                n.family_id, n.i) AS rk
            FROM (
                SELECT DISTINCT ct.content_key, nt.family_id, nt.i
                FROM ctok ct
                CROSS JOIN UNNEST(ct.tokens) AS t(tok)
                JOIN sv v ON v.tok = t.tok
                JOIN nodetok nt ON nt.tok = t.tok
            ) p
            JOIN ctok c USING (content_key)
            JOIN (SELECT family_id, i, any_value(ntok) AS ntok,
                         list(tok) AS toks
                  FROM nodetok GROUP BY 1,2) n
              ON n.family_id = p.family_id AND n.i = p.i
        ) WHERE sim >= {SIM} AND rk = 1
    """)
    n_scored = con.execute("SELECT count(*) FROM scored").fetchone()[0]
    print(f"      {n_scored:,} dead ends above Jaccard {SIM}  ({time.time() - t:.0f}s)")

    # --- 6. keep only the ones that came after their parent ----------------
    hours = ("date_diff('minute', TIMESTAMP '" + corpus_t0.strftime("%Y-%m-%d %H:%M:%S")
             + "', first_seen) / 60.0")
    ni = {(f, i): m["t0"] for (f, i), m in node_meta.items()}
    con.execute("CREATE TABLE nt0(family_id BIGINT, i INT, t0 DOUBLE)")
    con.executemany("INSERT INTO nt0 VALUES (?,?,?)",
                    [(f, i, float(v)) for (f, i), v in ni.items()])
    rows = con.execute(f"""
        SELECT s.family_id, s.i, s.text, s.sim, {hours} AS h
        FROM scored s JOIN nt0 USING (family_id, i)
        WHERE {hours} >= nt0.t0
        ORDER BY s.family_id, s.i, s.sim DESC
    """).fetchall()
    print(f"      {len(rows):,} survive the time gate")
    con.close()

    # --- 7. merge into the export ------------------------------------------
    per_node: dict[tuple[int, int], list] = collections.defaultdict(list)
    for fid, i, text, sim, h in rows:
        per_node[(int(fid), int(i))].append(text)

    touched = 0
    for fp, fams in trees.items():
        changed = False
        for fid, fam in fams.items():
            fid_i = int(fid)
            if fid_i not in want:
                continue
            total = 0
            for n in fam["nodes"]:
                dead = per_node.get((fid_i, n["i"]))
                if dead:
                    n["dx"] = len(dead)
                    n["dxs"] = [d[:220] for d in dead[:EXAMPLES]]
                    total += len(dead)
                    changed = True
                else:
                    n.pop("dx", None)
                    n.pop("dxs", None)
            fam["dx"] = total
            touched += 1
        if changed:
            Path(fp).write_text(json.dumps(fams, separators=(",", ":")))

    fam_tot = collections.Counter()
    for (fid, _i), lst in per_node.items():
        fam_tot[fid] += len(lst)
    for f in index["families"]:
        fid = int(f["id"])
        if fid in want:
            f["dx"] = fam_tot.get(fid, 0)
    index.setdefault("meta", {})["deadends"] = {
        "lineages": len(want), "found": len(rows),
        "sim": SIM, "min_shared": MIN_SHARED, "max_df": MAX_DF,
    }
    (PHYLO / "index.json").write_text(json.dumps(index, separators=(",", ":")))

    with_dx = sum(1 for f in index["families"] if f.get("dx"))
    print(f"  [6] merged into {touched:,} lineages; {with_dx:,} carry dead ends")
    if fam_tot:
        top = fam_tot.most_common(5)
        print("      most-mutated lineages (failed rewordings):")
        for fid, n in top:
            print(f"        lineage {fid}: {n:,}")
    print(f"\ndone in {time.time() - t_all:.0f}s")


if __name__ == "__main__":
    main()
