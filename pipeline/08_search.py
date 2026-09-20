"""
Stage 8 — full-text search index for the dashboard.

The problem this fixes
----------------------
Search used to run against three fields per lineage: `q`, `top` and `root`.
`root` and `top` are one variant each, and `q` was
`" ".join(sorted(tokens))[:320]` — which fails twice over:

  * **Alphabetical order destroys phrases.** A sorted token bag cannot match
    "charlie kirk"; the two words are only adjacent by accident.
  * **The cut is alphabetical too.** 2,247 lineages (21%) overflowed 320 chars,
    and in those everything from roughly "p" onward was simply gone. A search
    for "ycombinator" reached 74 of them.

So a term could be sitting in a dozen variants and return nothing. Measured
against node text: `h1b` went from 1 lineage to 16, `tariff` 4 to 10,
`immigration` 18 to 40.

What this writes
----------------
`data/export/phylo/search.json` — `{fid: blob}` over all 142,175 variant
texts, lowercased, URLs stripped, deduplicated within a lineage, word order
intact. The dashboard fetches it without awaiting it, exactly as it already
does with comments.json, and falls back to the index fields until it lands.

This is an additive post-pass in the shape stage 3/5/6/7 use: it reads the
export and writes one new file beside it. It never touches clustering,
distances or trees, so a failed run cannot take the demo down — the board just
keeps searching the way it did before.

    ./run.sh search
    ./run.sh search --cap 4000
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config as C
from pipeline.textnorm import search_blob

PHYLO = C.EXPORT / "phylo"

# 6000 chars per lineage holds 99.3% of what an uncapped blob would match
# while leaving headroom under the 16 MB per-file ceiling an Artifact imposes
# (uncapped is 14.1 MB, which publishes but leaves nothing spare). Only 249
# lineages are cut at all, and each loses its least-emitted wordings first.
DEFAULT_CAP = 6000


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=int, default=DEFAULT_CAP,
                    help="max search characters per lineage")
    args = ap.parse_args()

    t0 = time.time()
    print("search index — full variant text, phrase-preserving")

    idx_path = PHYLO / "index.json"
    if not idx_path.exists():
        sys.exit(f"no export at {idx_path} — run ./run.sh evolution first")
    index = json.loads(idx_path.read_text())

    tree_files = sorted(glob.glob(str(PHYLO / "trees-*.json")))
    if not tree_files:
        sys.exit(f"no tree files in {PHYLO} — run ./run.sh evolution first")

    blobs: dict[str, str] = {}
    n_nodes = 0
    for fp in tree_files:
        for fid, fam in json.loads(Path(fp).read_text()).items():
            nodes = fam["nodes"]
            n_nodes += len(nodes)
            # Most-emitted first: the cap cuts the tail, and a lineage forced
            # to drop wordings should drop the ones fewest people ever sent.
            ordered = sorted(nodes, key=lambda nd: -nd.get("n", 0))
            blobs[fid] = search_blob((nd.get("txt") or "" for nd in ordered),
                                     args.cap)
    print(f"  [1] {n_nodes:,} variant texts over {len(blobs):,} lineages "
          f"({time.time() - t0:.0f}s)")

    # Keep the index's own fallback honest: a lineage the trees do not carry
    # would otherwise silently drop out of search once the blob file loads.
    missing = [f["id"] for f in index["families"] if str(f["id"]) not in blobs]
    if missing:
        print(f"      warning: {len(missing):,} indexed lineages have no tree "
              f"(search falls back to root/top for these)")

    out = PHYLO / "search.json"
    payload = {
        "meta": {
            "cap": args.cap,
            "n_lineages": len(blobs),
            "n_variants": n_nodes,
            "truncated": sum(1 for b in blobs.values() if len(b) >= args.cap - 280),
        },
        "blobs": blobs,
    }
    out.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    mb = out.stat().st_size / 1e6
    print(f"  [2] wrote {out.name} — {mb:.1f} MB, "
          f"{payload['meta']['truncated']:,} lineages at the cap")
    if mb > 15:
        print("      warning: approaching the 16 MB Artifact file ceiling; "
              "re-run with a lower --cap before publishing")
    print(f"done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
