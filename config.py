"""Shared configuration. Every stage imports from here so paths stay consistent."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SHARDS = ROOT / "twitter-firehose"          # 396 raw parquet shards (gitignored)
DATA = ROOT / "data"

EVENTS = DATA / "normalized" / "events"     # one row per distinct tweet
CONTENT = DATA / "normalized" / "content"   # deduped text, keyed by content_key
TRAJECTORY = DATA / "normalized" / "trajectory"  # engagement curves
GRAPH = DATA / "graph"
LABELS = DATA / "labels"
EXPORT = DATA / "export"                    # JSON the dashboard reads

for p in (EVENTS, CONTENT, TRAJECTORY, GRAPH, LABELS, EXPORT):
    p.mkdir(parents=True, exist_ok=True)

# --- DuckDB resources -------------------------------------------------------
MEMORY_LIMIT = "32GB"
THREADS = 12
TMP_DIR = "/private/tmp/claude-501/duckdb-spill"

# --- Analysis knobs (tune these; they are the main levers) ------------------
PRIMARY_LANG = "en"       # None = all languages
CORETWEET_WINDOW_S = 60   # two accounts retweeting the same content within this = one co-event
MIN_COEVENTS = 3          # edge kept only if a pair co-retweets this many times
MAX_GROUP_FANOUT = 400    # cap pairs per viral content group to stop O(n^2) blowup
MIN_CLUSTER_SIZE = 5
