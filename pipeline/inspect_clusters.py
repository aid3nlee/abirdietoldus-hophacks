"""
Print the tightest coordinated clusters and what they amplify.

This is the human check on stage 2. Coordination metrics alone don't tell you
whether a cluster is a bot network or a football fandom -- looking at the
handles does. Run it after every parameter change.

  python3 pipeline/inspect_clusters.py [--top 12] [--min-size 8] [--lang en]
"""
import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

SUMMARY = """
WITH cs AS (
    SELECT cluster_id, count(*) AS sz FROM cl GROUP BY 1
),
ce AS (
    SELECT a.cluster_id,
           avg(e.jaccard)    AS jaccard,
           sum(e.n_coevents) AS coevents,
           count(*)          AS n_edges
    FROM ed e
    JOIN cl a ON a.author_id = e.u
    JOIN cl b ON b.author_id = e.v AND b.cluster_id = a.cluster_id
    GROUP BY 1
)
SELECT cs.cluster_id, cs.sz,
       round(ce.jaccard, 3)                                   AS jaccard,
       ce.coevents,
       round(2.0 * ce.n_edges / (cs.sz * (cs.sz - 1)), 3)     AS density
FROM cs JOIN ce USING (cluster_id)
WHERE cs.sz >= ?
ORDER BY ce.jaccard DESC
LIMIT ?
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--min-size", type=int, default=8)
    ap.add_argument("--lang", default=C.PRIMARY_LANG)
    ap.add_argument("--targets", type=int, default=6, help="handles shown per cluster")
    args = ap.parse_args()

    if not (C.GRAPH / "clusters.parquet").exists():
        sys.exit("no clusters yet -- run ./run.sh coordinate first")

    con = duckdb.connect()
    con.execute(f"SET threads={C.THREADS}")
    con.execute(f"CREATE VIEW cl AS SELECT * FROM '{C.GRAPH}/clusters.parquet'")
    con.execute(f"CREATE VIEW ed AS SELECT * FROM '{C.GRAPH}/edges.parquet'")
    con.execute(f"CREATE VIEW ev AS SELECT * FROM read_parquet('{C.EVENTS}/*.parquet')")

    top = con.execute(SUMMARY, [args.min_size, args.top]).df()
    if top.empty:
        sys.exit(f"no clusters with >= {args.min_size} accounts")

    print("jaccard = share of amplification targets two accounts hold in common")
    print("density = fraction of possible pairs inside the cluster that are linked\n")
    print(top.to_string(index=False))
    print()

    lang_filter = f"AND lang = '{args.lang}'" if args.lang and args.lang != "all" else ""
    for cid in top["cluster_id"]:
        rows = con.execute(f"""
            SELECT rt_handle, count(*) AS n FROM ev
            WHERE kind = 'retweet' {lang_filter}
              AND author_id IN (SELECT author_id FROM cl WHERE cluster_id = {cid})
            GROUP BY 1 ORDER BY n DESC LIMIT {args.targets}
        """).fetchall()
        sz = int(top.loc[top.cluster_id == cid, "sz"].iloc[0])
        targets = ", ".join(f"@{h}({n})" for h, n in rows if h)
        print(f"cluster {cid:>5} (n={sz:>3}): {targets}")


if __name__ == "__main__":
    main()
