#!/usr/bin/env python3
"""Restore the text the platform cut off, for display only.

A retweet body arrives clipped to 140 characters with an ellipsis glued on the
end, so most variants in the export read "...join the Akatsuki and…". Step 4
already folds a clipped copy into its full form *when it has one in
tokens.parquet* -- but that table starts at MIN_COPIES=2, and the tweet being
retweeted was typically posted exactly once. The full wording is therefore
sitting in content_global at n_copies=1, one join away, and simply never
reached the export.

This reads the export back, finds the full form of every clipped variant, and
rewrites the text in place. It touches nothing the analysis depends on -- not
the clustering, the distances, the tree, the counts. Only the string shown to
a reader changes, which is why it runs after step 4 instead of inside it:
re-deriving the phylogeny to fix a caption would be the wrong trade.

    ./run.sh untruncate

Safe to re-run: a second pass finds nothing left to repair.
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

CONTENT_GLOBAL = C.DATA / "normalized" / "content_global.parquet"
EXPORT = C.DATA / "export" / "phylo"

CUT = "…"
KEY = 40        # prefix used to bucket candidates; a clipped body runs ~119 chars
MAX_TEXT = 560  # twice a standard tweet: a guard against a pathological row
SHOW = 6


def stem_of(text: str) -> str | None:
    """The part of a clipped body that is a verbatim prefix of the original.

    The platform cuts at a character boundary and appends the ellipsis, so
    everything before that last ellipsis is exactly what the author wrote --
    including any ellipsis they typed themselves, which is why this splits on
    the last one rather than the first.
    """
    if not text.rstrip().endswith(CUT):
        return None
    stem = text.rstrip()[: text.rstrip().rindex(CUT)]
    return stem if len(stem) >= KEY else None


def load_export(d: Path) -> tuple[dict, list[tuple[Path, dict]]]:
    idx = json.loads((d / "index.json").read_text())
    trees = [(p, json.loads(p.read_text())) for p in sorted(d.glob("trees-*.json"))]
    return idx, trees


def collect_clipped(idx: dict, trees: list) -> dict[str, str]:
    """Every clipped string in the export, mapped to the stem we match on."""
    out: dict[str, str] = {}
    def add(t):
        if isinstance(t, str) and t not in out:
            s = stem_of(t)
            if s:
                out[t] = s
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                add(n.get("txt"))
    for f in idx["families"]:
        add(f.get("root"))
        add(f.get("top"))
    return out


def resolve(con, clipped: dict[str, str]) -> dict[str, str]:
    """Longest full text in the corpus that the clipped body is a prefix of.

    Bucketing on a fixed prefix turns this into one hash join over the corpus
    instead of a scan per variant; `starts_with` on the whole stem is what
    actually decides it, so a shared opening line cannot pull in the wrong
    tweet.
    """
    con.execute("CREATE OR REPLACE TEMP TABLE clipped (txt VARCHAR, stem VARCHAR, k VARCHAR)")
    con.executemany(
        "INSERT INTO clipped VALUES (?, ?, ?)",
        [(t, s, s[:KEY]) for t, s in clipped.items()],
    )
    rows = con.execute(
        f"""
        SELECT c.txt, arg_max(g.text, length(g.text)) AS full
        FROM clipped c
        JOIN '{CONTENT_GLOBAL}' g
          ON substr(g.text, 1, {KEY}) = c.k
        WHERE g.text NOT LIKE '%{CUT}'
          AND length(g.text) > length(c.txt)
          AND length(g.text) <= {MAX_TEXT}
          AND starts_with(g.text, c.stem)
        GROUP BY c.txt
        """
    ).fetchall()
    return {t: full for t, full in rows}


def apply(idx: dict, trees: list, fix: dict[str, str]) -> dict[str, int]:
    """Rewrite in place. index.json keeps its own 180-char display slice."""
    hits = {"txt": 0, "root": 0, "top": 0}
    for _, tree in trees:
        for fam in tree.values():
            for n in fam["nodes"]:
                full = fix.get(n.get("txt"))
                if full:
                    n["txt"] = full
                    hits["txt"] += 1
    for f in idx["families"]:
        for field in ("root", "top"):
            full = fix.get(f.get(field))
            if full:
                f[field] = full[:180]
                hits[field] += 1
    return hits


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

    t = time.time()
    idx, trees = load_export(d)
    clipped = collect_clipped(idx, trees)
    total = sum(len(f["nodes"]) for _, tr in trees for f in tr.values())
    print(f"  {total:,} variants, {len(clipped):,} clipped by the platform")
    if not clipped:
        print("  nothing to repair")
        return

    con = duckdb.connect()
    fix = resolve(con, clipped)
    print(f"  {len(fix):,} full wordings recovered from the corpus "
          f"({100 * len(fix) / len(clipped):.0f}% of the clipped ones)")

    for t_, full in list(fix.items())[:SHOW]:
        print(f"    {len(t_):>3} -> {len(full):>3}  {full[:88]!r}")

    if a.dry_run:
        print("  --dry-run: nothing written")
        return

    hits = apply(idx, trees, fix)
    write(d, idx, trees)
    print(f"  rewrote {hits['txt']:,} variant texts, "
          f"{hits['root']:,} roots, {hits['top']:,} list captions  ({time.time() - t:.0f}s)")
    left = len(clipped) - len(fix)
    if left:
        print(f"  {left:,} stay clipped: the tweet they amplify is not in the crawl")
    print("  run ./run.sh dist to publish")


if __name__ == "__main__":
    main()
