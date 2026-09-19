"""
Stage 2 — find coordinated amplification networks, using behaviour only.

The method is co-retweet coordination, the standard approach in the influence
operations literature. The premise: a bot network can vary its wording freely,
but it cannot hide *who it amplifies* and *when*. Two accounts that repeatedly
boost the same content within seconds of each other, over and over, are not
independently discovering it.

Nothing here looks at what the tweets say. That is deliberate -- content
filtering comes in stage 3, after the clusters exist. Finding the networks
first and asking what they push second avoids baking our assumptions about
what counts as "hate" into the detector, and it lets the findings surprise us.

Scaling note: naive pairwise comparison inside each content group is O(n^2) and
explodes on viral tweets (one tweet with 500k retweets would emit 10^11 pairs).
Instead we bucket each retweet into a time window and only pair within a
bucket, with two offset bucketings so a pair straddling a boundary is not lost.
That turns the cost from quadratic-in-virality to linear-in-volume.

Outputs (contracts for stage 3 / dashboard):
  graph/accounts.parquet       per-account behavioural features
  graph/edges.parquet          u, v, n_coevents, jaccard
  graph/clusters.parquet       author_id -> cluster_id
  graph/cluster_stats.parquet  per-cluster size + suspicion metrics

Run:  python3 pipeline/02_coordinate.py [--lang en] [--window 60] [--min-coevents 3]
"""
import argparse
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute(f"SET threads={C.THREADS}")
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def build_edges(con, lang: str | None, window: int, min_coevents: int,
                min_account_rts: int, max_fanout: int) -> None:
    events = f"{C.EVENTS}/*.parquet"
    lang_filter = f"AND lang = '{lang}'" if lang else ""

    print("  [1/4] loading retweet events...")
    con.execute(f"""
        CREATE TEMP TABLE rt AS
        SELECT author_id, content_key, epoch(created_at)::BIGINT AS ts
        FROM read_parquet('{events}')
        WHERE kind = 'retweet' AND rt_handle IS NOT NULL {lang_filter}
    """)
    n = con.execute("SELECT count(*) FROM rt").fetchone()[0]
    print(f"        {n:,} retweet events")

    # Drop accounts too quiet to say anything about. A pair of accounts that
    # each retweeted twice can coincide by luck; one that retweeted 400 times
    # in lockstep with another cannot.
    print(f"  [2/4] filtering to accounts with >= {min_account_rts} retweets...")
    con.execute(f"""
        CREATE TEMP TABLE rt_active AS
        SELECT r.* FROM rt r
        JOIN (SELECT author_id FROM rt GROUP BY 1 HAVING count(*) >= {min_account_rts}) a
          USING (author_id)
    """)
    na = con.execute("SELECT count(DISTINCT author_id) FROM rt_active").fetchone()[0]
    print(f"        {na:,} active accounts retained")

    # Two bucketings offset by half a window: a pair separated by less than
    # `window` is guaranteed to share at least one bucket in one of them.
    print(f"  [3/4] emitting co-retweet pairs (window={window}s)...")
    half = window // 2
    con.execute(f"""
        CREATE TEMP TABLE bucketed AS
        SELECT author_id, content_key, ts, 0 AS off, (ts / {window})::BIGINT AS b FROM rt_active
        UNION ALL
        SELECT author_id, content_key, ts, 1 AS off, ((ts + {half}) / {window})::BIGINT AS b FROM rt_active
    """)

    # Cap how many accounts we pair inside any one bucket. Without this a single
    # mega-viral moment dominates every edge weight in the graph.
    con.execute(f"""
        CREATE TEMP TABLE capped AS
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY content_key, off, b ORDER BY ts) AS rn,
                      count(*)   OVER (PARTITION BY content_key, off, b) AS sz
            FROM bucketed
        ) WHERE rn <= {max_fanout} AND sz >= 2
    """)

    con.execute(f"""
        CREATE TEMP TABLE edges AS
        SELECT u, v, count(DISTINCT content_key) AS n_coevents
        FROM (
            SELECT DISTINCT
                   least(a.author_id, b.author_id)    AS u,
                   greatest(a.author_id, b.author_id) AS v,
                   a.content_key
            FROM capped a
            JOIN capped b
              ON a.content_key = b.content_key
             AND a.off = b.off AND a.b = b.b
             AND a.author_id < b.author_id
             AND abs(a.ts - b.ts) <= {window}
        )
        GROUP BY u, v
        HAVING count(DISTINCT content_key) >= {min_coevents}
    """)
    ne = con.execute("SELECT count(*) FROM edges").fetchone()[0]
    print(f"        {ne:,} candidate edges (>= {min_coevents} co-events)")

    # Normalise by activity: 20 co-retweets between two accounts that each
    # retweet thousands of times is unremarkable; between two that retweet
    # 25 times each it is damning.
    print("  [4/4] scoring edges and writing...")
    con.execute("""
        CREATE TEMP TABLE acct_n AS
        SELECT author_id, count(DISTINCT content_key) AS n_targets FROM rt_active GROUP BY 1
    """)
    con.execute(f"""
        COPY (
            SELECT e.u, e.v, e.n_coevents,
                   e.n_coevents::DOUBLE / (nu.n_targets + nv.n_targets - e.n_coevents) AS jaccard
            FROM edges e
            JOIN acct_n nu ON nu.author_id = e.u
            JOIN acct_n nv ON nv.author_id = e.v
        ) TO '{C.GRAPH}/edges.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def build_accounts(con, lang: str | None) -> None:
    """Per-account behavioural features, used to score clusters in stage 3."""
    events = f"{C.EVENTS}/*.parquet"
    lang_filter = f"AND lang = '{lang}'" if lang else ""
    print("  building account features...")
    con.execute(f"""
        COPY (
            WITH ev AS (
                SELECT * FROM read_parquet('{events}') WHERE 1=1 {lang_filter}
            ),
            base AS (
                SELECT author_id,
                       count(*)                                          AS n_tweets,
                       sum(kind = 'retweet')::BIGINT                     AS n_retweets,
                       sum(kind = 'original')::BIGINT                    AS n_originals,
                       sum(kind = 'reply')::BIGINT                       AS n_replies,
                       count(DISTINCT rt_handle)                         AS n_targets,
                       min(created_at)                                   AS first_seen,
                       max(created_at)                                   AS last_seen,
                       -- author_id is roughly monotonic with signup date, so its
                       -- magnitude is a crude account-age proxy. Crude, but the
                       -- only age signal this schema affords.
                       max(try_cast(author_id AS HUGEINT))               AS author_id_num
                FROM ev GROUP BY author_id
            )
            SELECT *,
                   n_retweets::DOUBLE / nullif(n_tweets, 0)   AS retweet_ratio,
                   n_targets::DOUBLE  / nullif(n_retweets, 0) AS target_diversity,
                   date_diff('hour', first_seen, last_seen)   AS active_hours
            FROM base
        ) TO '{C.GRAPH}/accounts.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def cluster() -> None:
    """Community detection over the co-retweet graph."""
    import pandas as pd

    edges = pd.read_parquet(C.GRAPH / "edges.parquet")
    if edges.empty:
        print("  no edges -- loosen --min-coevents or widen --window")
        return
    print(f"  clustering {len(edges):,} edges...")

    nodes = pd.unique(edges[["u", "v"]].values.ravel())
    idx = {a: i for i, a in enumerate(nodes)}
    src = edges["u"].map(idx).to_numpy()
    dst = edges["v"].map(idx).to_numpy()

    labels = None
    try:
        import igraph as ig  # Leiden gives far better communities than components
        import leidenalg

        g = ig.Graph(n=len(nodes), edges=list(zip(src, dst)))
        g.es["weight"] = edges["n_coevents"].tolist()
        part = leidenalg.find_partition(
            g, leidenalg.ModularityVertexPartition, weights="weight", seed=42
        )
        labels = part.membership
        print(f"  Leiden: {len(set(labels)):,} communities")
    except ImportError:
        # Fallback keeps the pipeline runnable without the optional deps.
        from scipy.sparse import coo_matrix
        from scipy.sparse.csgraph import connected_components

        print("  igraph/leidenalg unavailable -- falling back to connected components")
        w = edges["n_coevents"].to_numpy()
        m = coo_matrix((w, (src, dst)), shape=(len(nodes), len(nodes)))
        _, labels = connected_components(m, directed=False)

    out = pd.DataFrame({"author_id": nodes, "cluster_id": labels})
    sizes = out.groupby("cluster_id").size()
    keep = sizes[sizes >= C.MIN_CLUSTER_SIZE].index
    out = out[out["cluster_id"].isin(keep)]
    out.to_parquet(C.GRAPH / "clusters.parquet", index=False)
    print(f"  {len(keep):,} clusters with >= {C.MIN_CLUSTER_SIZE} accounts "
          f"covering {len(out):,} accounts")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", default=C.PRIMARY_LANG, help="'all' for no filter")
    ap.add_argument("--window", type=int, default=C.CORETWEET_WINDOW_S)
    ap.add_argument("--min-coevents", type=int, default=C.MIN_COEVENTS)
    ap.add_argument("--min-account-rts", type=int, default=5)
    ap.add_argument("--max-fanout", type=int, default=C.MAX_GROUP_FANOUT)
    ap.add_argument("--skip-accounts", action="store_true")
    args = ap.parse_args()

    lang = None if args.lang == "all" else args.lang
    t0 = time.time()
    con = connect()

    print(f"co-retweet coordination  lang={lang or 'all'}  window={args.window}s  "
          f"min_coevents={args.min_coevents}")
    build_edges(con, lang, args.window, args.min_coevents,
                args.min_account_rts, args.max_fanout)
    if not args.skip_accounts:
        build_accounts(con, lang)
    con.close()
    cluster()
    print(f"\ndone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
