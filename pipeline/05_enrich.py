#!/usr/bin/env python3
"""Put back what the export dropped: the cut text and the engagement counts.

Two separate losses, both repaired from the same scan of the firehose.

1. TEXT. A retweet body arrives clipped to 140 characters with an ellipsis
   glued on, so most variants read "...join the Akatsuki and…". Step 4 already
   folds a clipped copy into its full form *when it has one in tokens.parquet*
   -- but that table starts at MIN_COPIES=2, and the tweet being retweeted was
   typically posted exactly once. The full wording sits in the firehose at
   n_copies=1, one join below the threshold, and never reaches the export.

2. COUNTS. The firehose carries like_count, reply_count, retweet_count,
   quote_count, views_count and bookmarks_count. They are real, and on an
   original tweet they are populated -- 99.6% of non-retweet rows carry a view
   count. normalize() keeps only the text, so the export has none of them.
   A retweet row does not accumulate its own likes, which is why these have to
   come from the original rather than from the amplification.

3. RESPONSES -- moved out of this stage. This used to attach a collapsed
   reply group per wording. Stage 6 supersedes it and does better on both
   counts: it reads quote tweets as well as replies (39.6M against 10.1M, and
   36x the reach across the export's wordings), and it clusters and scores
   them instead of only counting. The join here was also a disjunction over
   10M reply rows, which plans as a nested loop. Nothing reads the old `rsp`
   field any more -- see pipeline/06_comments.py.

These are display repairs. Nothing here feeds the clustering, the distances,
the tree or the spread figures, which is why it runs after step 4 rather than
inside it: re-deriving the phylogeny to fix a caption is the wrong trade.

    ./run.sh enrich

Safe to re-run: a second pass finds nothing left to repair.

What this cannot do: recover a tweet that is not in the crawl. The firehose
has no retweeted_status and no embed payload (that column is null throughout),
and a retweet's conversation_id points at itself rather than at its original,
so there is no id to follow -- text is the only join available. Where the
original was never sampled, the clipped body is all anyone has.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C  # noqa: E402

FIREHOSE = C.ROOT / "twitter-firehose" / "*.parquet"
EXPORT = C.DATA / "export" / "phylo"

CUT = "…"
KEY = 40        # prefix bucket; a clipped body runs ~119 chars, so this is safe
MAX_TEXT = 560  # twice a standard tweet: a guard against a pathological row
SHOW = 5

# Which count goes in which slot of the post card.
METRICS = ["like_count", "reply_count", "retweet_count",
           "quote_count", "views_count", "bookmarks_count"]
SHORT = {"like_count": "like", "reply_count": "reply", "retweet_count": "rt",
         "quote_count": "quote", "views_count": "views", "bookmarks_count": "save"}


def stem_of(text: str) -> str | None:
    """The part of a clipped body that is a verbatim prefix of the original.

    The platform cuts at a character boundary and appends the ellipsis, so
    everything before that last ellipsis is exactly what the author wrote --
    including any ellipsis they typed themselves, which is why this splits on
    the last one rather than the first.
    """
    t = text.rstrip()
    if not t.endswith(CUT):
        return None
    stem = t[: t.rindex(CUT)]
    return stem if len(stem) >= KEY else None


def load_export(d: Path):
    idx = json.loads((d / "index.json").read_text())
    trees = [(p, json.loads(p.read_text())) for p in sorted(d.glob("trees-*.json"))]
    return idx, trees


def wanted(idx: dict, trees: list) -> list[tuple[str, str, str, bool]]:
    """Every distinct string in the export, with the key it matches on."""
    seen: dict[str, tuple] = {}
    def add(t):
        if not isinstance(t, str) or t in seen or len(t) < KEY:
            return
        stem = stem_of(t)
        if stem:
            seen[t] = (t, stem, stem[:KEY], True)
        else:
            seen[t] = (t, t, t[:KEY], False)
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                add(n.get("txt"))
    for f in idx["families"]:
        add(f.get("root"))
        add(f.get("top"))
    return list(seen.values())


def resolve(con, want: list) -> dict[str, dict]:
    """Match each variant to its original row in the firehose.

    Bucketing on a fixed prefix makes this one hash join over the corpus rather
    than a scan per variant; `starts_with` over the whole observed body is what
    actually decides a match, so a shared opening line cannot pull in a
    different tweet.

    The row chosen is the longest body, tie-broken by views -- longest because
    that is the complete tweet, views because identical text posted by many
    accounts should report the instance that actually travelled. Text and
    counts come from that one row, so a card never mixes two tweets together.
    """
    con.execute("CREATE OR REPLACE TEMP TABLE want "
                "(txt VARCHAR, stem VARCHAR, k VARCHAR, clipped BOOLEAN)")
    con.executemany("INSERT INTO want VALUES (?, ?, ?, ?)", want)

    picks = ", ".join(f"f.{m}" for m in METRICS)
    rows = con.execute(f"""
        SELECT w.txt,
               arg_max(struct_pack(body := f.body, tweet_id := f.id,
                                   conversation_id := f.conversation_id, {picks}),
                       length(f.body) * 1000000000
                       + least(coalesce(f.views_count, 0), 999999999)) AS best
        FROM want w
        JOIN read_parquet('{FIREHOSE}') f
          ON substr(f.body, 1, {KEY}) = w.k
        WHERE f.body NOT LIKE 'RT @%'
          AND f.body NOT LIKE '%{CUT}'
          AND length(f.body) <= {MAX_TEXT}
          AND ( (w.clipped     AND starts_with(f.body, w.stem))
             OR (NOT w.clipped AND f.body = w.txt) )
        GROUP BY w.txt
    """).fetchall()
    return {txt: best for txt, best in rows}


def apply(idx: dict, trees: list, hit: dict[str, dict]) -> dict[str, int]:
    """Rewrite in place. index.json keeps its own 180-char display slice."""
    c = {"txt": 0, "met": 0, "root": 0, "top": 0}
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                source_txt = n.get("txt")
                b = hit.get(source_txt)
                if not b:
                    continue
                if len(b["body"]) > len(n["txt"]):
                    n["txt"] = b["body"]
                    c["txt"] += 1
                n["tweet"] = str(b["tweet_id"])
                met = {SHORT[m]: int(b[m] or 0) for m in METRICS}
                if any(met.values()):
                    n["met"] = met
                    c["met"] += 1
    for f in idx["families"]:
        for field in ("root", "top"):
            b = hit.get(f.get(field))
            if b and len(b["body"]) > len(f[field]):
                f[field] = b["body"][:180]
                c[field] += 1
    return c


def write(d: Path, idx: dict, trees: list) -> None:
    def put(p: Path, obj) -> None:
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
        tmp.replace(p)
    put(d / "index.json", idx)
    for p, tree in trees:
        put(p, tree)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--export", default=str(EXPORT), help="phylo export directory")
    ap.add_argument("--dry-run", action="store_true", help="report, change nothing")
    a = ap.parse_args()

    d = Path(a.export)
    if not (d / "index.json").exists():
        sys.exit(f"no export at {d} - run ./run.sh evolution first")
    if not list(Path(str(FIREHOSE)).parent.glob("*.parquet")):
        sys.exit("no firehose shards - run ./run.sh download first")

    t = time.time()
    idx, trees = load_export(d)
    want = wanted(idx, trees)
    clipped = sum(1 for w in want if w[3])
    total = sum(len(f["nodes"]) for _, tr in trees for f in tr.values())
    print(f"  {total:,} variants, {len(want):,} distinct strings, "
          f"{clipped:,} clipped by the platform")

    hit = resolve(duckdb.connect(), want)
    got_text = sum(1 for w in want if w[3] and w[0] in hit)
    print(f"  matched {len(hit):,} to an original tweet in the firehose")
    print(f"  full wording recovered for {got_text:,} of {clipped:,} clipped "
          f"({100 * got_text / max(1, clipped):.0f}%)")

    for w in [w for w in want if w[3] and w[0] in hit][:SHOW]:
        b = hit[w[0]]
        print(f"    {len(w[0]):>3} -> {len(b['body']):>3} ch  "
              f"{b['views_count'] or 0:>9,} views  {b['body'][:64]!r}")

    if a.dry_run:
        print("  --dry-run: nothing written")
        return

    c = apply(idx, trees, hit)
    write(d, idx, trees)
    print(f"  rewrote {c['txt']:,} texts, attached counts to {c['met']:,} variants, "
          f"fixed {c['root']:,} roots and {c['top']:,} captions  ({time.time() - t:.0f}s)")
    print("  responses now come from pipeline/06_comments.py - run ./run.sh comments")
    left = clipped - got_text
    if left:
        print(f"  {left:,} stay clipped: the tweet they amplify is not in the crawl")
    print("  run ./run.sh dist to publish")


if __name__ == "__main__":
    main()
