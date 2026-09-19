"""
Stage 1 — normalize the raw firehose into three compact tables.

Why this stage exists: the raw corpus is 52 GB because the text of every viral
tweet is stored once per retweet. Splitting content out and keying it by hash
collapses that redundancy, and what remains is small enough to iterate on fast.

Shards are *mostly* partitioned by `created_at`, but ~3.65% of tweets straddle
a shard boundary and appear in two files. So this stage dedups within a shard
(cheap, parallel) and stage 01b does a global pass partitioned by day -- which
is exact, because every copy of a tweet shares its `created_at` and therefore
lands in the same day.

Outputs (the contracts other stages code against):

  events/events-NNNNNN.parquet
      One row per distinct tweet.
      id, author_id, created_at, version, lang, kind, rt_handle, content_key,
      reply_to_status_id, reply_to_user_id, conversation_id, quoting_id, is_truncated,
      like_count, retweet_count, reply_count, quote_count, views_count

  content/content-NNNNNN.parquet
      Deduped text. content_key, kind, rt_handle, text, n_copies, first_seen

  trajectory/trajectory-NNNNNN.parquet
      Only tweets observed more than once — the engagement curves.
      id, version, like_count, retweet_count, views_count

Run:  python3 pipeline/01_normalize.py [--limit N] [--workers K]
"""
import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import config as C

# A retweet body is literally "RT @handle: <text>", truncated by the platform at
# 140 chars. The truncation is deterministic, so every retweeter of the same
# tweet yields a byte-identical string -- which is what makes content_key a
# reliable join key for co-retweet detection.
RT_PREFIX = r'^RT @([A-Za-z0-9_]{1,15}): '

SQL = f"""
WITH src AS (
    SELECT * FROM read_parquet(?)
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY version DESC) AS rn
    FROM src
),
latest AS (
    SELECT * FROM ranked WHERE rn = 1
),
typed AS (
    SELECT
        id,
        author_id,
        created_at,
        version,
        lang,
        CASE
            WHEN regexp_matches(body, '{RT_PREFIX}') THEN 'retweet'
            WHEN reply_to_status_id IS NOT NULL      THEN 'reply'
            WHEN quoting_id IS NOT NULL              THEN 'quote'
            ELSE 'original'
        END AS kind,
        nullif(regexp_extract(body, '{RT_PREFIX}', 1), '') AS rt_handle,
        CASE
            WHEN regexp_matches(body, '{RT_PREFIX}')
            THEN regexp_replace(body, '{RT_PREFIX}', '')
            ELSE body
        END AS text,
        reply_to_status_id,
        reply_to_user_id,
        conversation_id,
        quoting_id,
        like_count, retweet_count, reply_count, quote_count, views_count
    FROM latest
)
SELECT
    id, author_id, created_at, version, lang, kind, rt_handle,
    -- key the ORIGINAL content, not this copy of it: handle + text identifies
    -- the upstream tweet even though the schema has no retweeted_status_id.
    md5(coalesce(rt_handle, '') || '|' || text) AS content_key,
    text,
    reply_to_status_id, reply_to_user_id, conversation_id, quoting_id,
    (text LIKE '%…') AS is_truncated,
    like_count, retweet_count, reply_count, quote_count, views_count
FROM typed
"""


def connect():
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute(f"SET threads={max(1, C.THREADS // 2)}")
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    return con


def process_shard(path_str: str) -> tuple[str, int, float]:
    """Normalize one shard into the three output tables."""
    from pathlib import Path

    t0 = time.time()
    path = Path(path_str)
    tag = path.stem.replace("tweets-", "")
    con = connect()

    con.execute(f"CREATE TEMP TABLE norm AS {SQL}", [path_str])
    n = con.execute("SELECT count(*) FROM norm").fetchone()[0]

    # --- events: everything except the bulky text -------------------------
    con.execute(
        f"""COPY (
            SELECT id, author_id, created_at, version, lang, kind, rt_handle, content_key,
                   reply_to_status_id, reply_to_user_id, conversation_id, quoting_id, is_truncated,
                   like_count, retweet_count, reply_count, quote_count, views_count
            FROM norm
        ) TO '{C.EVENTS}/events-{tag}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )

    # --- content: one row per distinct piece of text ----------------------
    con.execute(
        f"""COPY (
            SELECT content_key,
                   any_value(kind)       AS kind,
                   any_value(rt_handle)  AS rt_handle,
                   any_value(text)       AS text,
                   any_value(lang)       AS lang,
                   count(*)              AS n_copies,
                   min(created_at)       AS first_seen
            FROM norm GROUP BY content_key
        ) TO '{C.CONTENT}/content-{tag}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )

    # --- trajectory: engagement curves for re-observed tweets -------------
    con.execute(
        f"""COPY (
            SELECT id, version, like_count, retweet_count, views_count
            FROM read_parquet('{path_str}')
            WHERE id IN (
                SELECT id FROM read_parquet('{path_str}')
                GROUP BY id HAVING count(*) > 1
            )
        ) TO '{C.TRAJECTORY}/trajectory-{tag}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )

    con.close()
    return tag, n, time.time() - t0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="only first N shards")
    ap.add_argument("--workers", type=int, default=5)
    args = ap.parse_args()

    shards = sorted(C.SHARDS.glob("tweets-*.parquet"))
    if args.limit:
        shards = shards[: args.limit]
    if not shards:
        sys.exit(f"no shards found in {C.SHARDS}")

    # Skip shards already normalized so this is resumable after a crash.
    todo = [s for s in shards if not (C.EVENTS / f"events-{s.stem.replace('tweets-', '')}.parquet").exists()]
    print(f"{len(shards)} shards present, {len(todo)} to process, {args.workers} workers")

    t0, done, rows = time.time(), 0, 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_shard, str(s)): s for s in todo}
        for f in as_completed(futs):
            tag, n, dt = f.result()
            done += 1
            rows += n
            print(f"  [{done}/{len(todo)}] shard {tag}  {n:,} tweets  {dt:.1f}s", flush=True)

    print(f"\ndone: {rows:,} distinct tweets in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
