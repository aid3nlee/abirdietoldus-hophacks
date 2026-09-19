#!/bin/sh
# Entry point for the whole pipeline. Every step is resumable and safe to
# re-run: work already on disk is skipped, so if something dies halfway just
# run it again.
#
#   ./run.sh status      what's done so far
#   ./run.sh setup       install python deps
#   ./run.sh download    pull the 396 shards (~25 min, resumable)
#   ./run.sh normalize   raw shards -> events/content/trajectory
#   ./run.sh coordinate  co-retweet graph -> clusters
#   ./run.sh inspect     print the top clusters and what they amplify
#   ./run.sh evolution   variant families, mutation trees, dashboard export
#   ./run.sh enrich      put back the cut text and the platform's own counts
#   ./run.sh dist        assemble the static site into dist/
#   ./run.sh serve       live dev server on :8000 (npm run dev)
#   ./run.sh verify     check every shard opens as valid parquet
#   ./run.sh all         setup + download + normalize + coordinate
#
# Anything after the subcommand is passed through, e.g.
#   ./run.sh coordinate --window 30 --min-coevents 5

set -e
cd "$(dirname "$0")"

BASE="https://calcifer-hot.s3.us-east-2.amazonaws.com/hopkins-hackathon-2026/twitter-firehose-last-month"
NSHARDS=395          # shards are 000000..000395
PARALLEL=10          # concurrent downloads

cmd="${1:-status}"
[ $# -gt 0 ] && shift

case "$cmd" in

setup)
    echo "installing python dependencies..."
    python3 -m pip install --quiet -r requirements.txt
    python3 -c "import duckdb,pyarrow,pandas; print('core ok')"
    python3 -c "import igraph,leidenalg; print('leiden ok')" 2>/dev/null \
        || echo "note: leidenalg missing - clustering falls back to connected components"
    ;;

download)
    mkdir -p twitter-firehose
    echo "downloading shards 000000..$(printf '%06d' $NSHARDS) with $PARALLEL workers"
    echo "(already-downloaded files are skipped; safe to interrupt and re-run)"
    # Download to .tmp and move into place only on success. Without this an
    # interrupted transfer leaves a non-empty partial file that the next run
    # would skip, silently corrupting a shard.
    seq -f "%06g" 0 $NSHARDS | xargs -P $PARALLEL -I{} sh -c \
        'f="tweets-{}.parquet"
         [ -s "twitter-firehose/$f" ] && exit 0
         if curl -sf -o "twitter-firehose/.$f.tmp" "'"$BASE"'/$f"; then
             mv "twitter-firehose/.$f.tmp" "twitter-firehose/$f"
         else
             rm -f "twitter-firehose/.$f.tmp"; echo "FAILED $f"
         fi'
    echo "done: $(ls twitter-firehose/*.parquet 2>/dev/null | wc -l | tr -d ' ') shards"
    ;;

verify)
    # Parquet keeps its footer at the end of the file, so a truncated shard
    # fails to open. Cheapest possible integrity check.
    echo "checking every shard opens as valid parquet..."
    python3 - <<'PY'
import glob, duckdb, sys
con, bad = duckdb.connect(), []
files = sorted(glob.glob("twitter-firehose/*.parquet"))
for f in files:
    try:
        con.execute(f"SELECT 1 FROM read_parquet('{f}') LIMIT 1").fetchone()
    except Exception:
        bad.append(f)
print(f"{len(files) - len(bad)}/{len(files)} shards valid")
for f in bad:
    print("  CORRUPT:", f)
if bad:
    print("\ndelete those files and re-run ./run.sh download")
    sys.exit(1)
PY
    ;;

normalize)
    python3 pipeline/01_normalize.py --workers 5 "$@"
    ;;

coordinate)
    python3 pipeline/02_coordinate.py "$@"
    ;;

inspect)
    python3 pipeline/inspect_clusters.py "$@"
    ;;

evolution)
    python3 pipeline/04_evolution.py "$@"
    ;;

enrich)
    # Display repair, not analysis. Two things the export loses and this puts
    # back from the firehose: the text beyond the 140-char retweet cut, and the
    # like/reply/retweet/quote/view/bookmark counts, which normalize drops and
    # which only an original row carries. Re-run after any re-run of evolution,
    # or the site shows stumps with no counters.
    python3 pipeline/05_enrich.py "$@"
    ;;

dist)
    # The site is the page plus the JSON it fetches, and nothing else: no build
    # step, no server, no API. Anything that serves static files can host it.
    if [ ! -f data/export/phylo/index.json ]; then
        echo "no export yet - run ./run.sh evolution first" >&2
        exit 1
    fi
    clipped=$(python3 -c "
import json, glob
n = sum(1 for f in glob.glob('data/export/phylo/trees-*.json')
          for fam in json.load(open(f)).values()
          for v in fam['nodes'] if v['txt'].rstrip().endswith('\u2026'))
print(n)" 2>/dev/null || echo 0)
    if [ "$clipped" -gt 8000 ]; then
        echo "note: $clipped variants still cut at 140 chars - run ./run.sh enrich first"
    fi
    rm -rf dist
    mkdir -p dist/phylo
    cp dashboard/index.html dist/index.html
    cp data/export/phylo/*.json dist/phylo/
    echo "dist/ ready: $(du -sh dist | cut -f1), $(ls dist/phylo | wc -l | tr -d ' ') data files"
    echo "deploy the folder to any static host, or run ./run.sh serve"
    ;;

serve)
    # A dev server, not a build: .dev/ symlinks the real page and the real
    # export, so editing dashboard/index.html only needs a browser refresh.
    if [ ! -f data/export/phylo/index.json ]; then
        echo "no export yet - run ./run.sh evolution first" >&2
        exit 1
    fi
    port="${1:-8000}"
    rm -rf .dev
    mkdir -p .dev
    ln -s ../dashboard/index.html .dev/index.html
    ln -s ../data/export/phylo .dev/phylo
    echo "serving dashboard/index.html live - edit it and refresh"
    echo
    echo "  http://localhost:$port"
    echo
    python3 -m http.server "$port" --directory .dev
    ;;

all)
    ./run.sh setup
    ./run.sh download
    ./run.sh normalize
    ./run.sh coordinate "$@"
    ./run.sh inspect
    ./run.sh evolution
    ./run.sh enrich
    ;;

status)
    raw=$(ls twitter-firehose/*.parquet 2>/dev/null | wc -l | tr -d ' ')
    nor=$(ls data/normalized/events/*.parquet 2>/dev/null | wc -l | tr -d ' ')
    echo "raw shards       $raw / 396   $(du -sh twitter-firehose 2>/dev/null | cut -f1)"
    echo "normalized       $nor / $raw"
    if [ -f data/graph/clusters.parquet ]; then
        python3 - <<'PY'
import duckdb
c = duckdb.connect()
n = c.execute("SELECT count(DISTINCT cluster_id), count(*) FROM 'data/graph/clusters.parquet'").fetchone()
e = c.execute("SELECT count(*) FROM 'data/graph/edges.parquet'").fetchone()[0]
print(f"graph            {e:,} edges, {n[0]:,} clusters, {n[1]:,} accounts")
PY
    else
        echo "graph            not built yet"
    fi
    if [ -f data/export/phylo/index.json ]; then
        python3 - <<'PHYLO'
import json
m = json.load(open('data/export/phylo/index.json'))['meta']
print(f"phylogeny        {m['n_families']:,} lineages, {m['n_variants']:,} variants, "
      f"{m['n_emissions']:,} retweets traced")
PHYLO
    else
        echo "phylogeny        not built yet"
    fi
    if [ "$raw" -lt 396 ]; then
        echo
        echo "note: $((396 - raw)) shards still missing - run ./run.sh download"
    elif [ "$nor" -lt "$raw" ]; then
        echo
        echo "note: $((raw - nor)) shards not yet normalized - run ./run.sh normalize"
    fi
    ;;

*)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
