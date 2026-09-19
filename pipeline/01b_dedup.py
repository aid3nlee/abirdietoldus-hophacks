"""
Stage 1b — repartition events by day (and verify deduplication).

Stage 1 dedups within each shard. That turns out to be globally correct: shards
partition cleanly on `created_at`, and measuring this stage confirms zero
cross-shard duplicates. (An earlier `approx_count_distinct` suggested 3.65% of
rows were duplicates -- that was HyperLogLog error, not real duplication. The
dataset README's "~363.5M distinct" figure appears to be the same approximation;
the exact count is 377,271,528.)

This stage therefore exists for a different reason: repartitioning by day makes
every time-sliced query dramatically faster, because DuckDB skips whole files
via the Hive partition key. The dedup it performs is a cheap ongoing assertion
that the shard-local assumption still holds.

We also exploit the fact that shards are time-ordered: each day touches only a
handful of adjacent shards, so we build a shard -> time-range index once and
read only the overlapping shards per day rather than rescanning all 396.

Repartitioning by day is useful in its own right -- most downstream questions
are time-sliced, and DuckDB skips whole files via the Hive partition key.

Input:   data/normalized/events/events-NNNNNN.parquet   (shard-partitioned)
Output:  data/normalized/events_daily/dt=YYYY-MM-DD/data.parquet

Run:  python3 pipeline/01b_dedup.py [--workers 4]
"""
import argparse
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

EVENTS_DAILY = C.DATA / "normalized" / "events_daily"


def connect(threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")
    con.execute(f"SET threads={threads}")
    con.execute(f"SET temp_directory='{C.TMP_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


def build_index(con) -> list[tuple[str, str, str]]:
    """Min/max created_at per shard, so each day reads only overlapping files."""
    rows = con.execute(f"""
        SELECT filename,
               strftime(min(created_at), '%Y-%m-%d') AS d0,
               strftime(max(created_at), '%Y-%m-%d') AS d1
        FROM read_parquet('{C.EVENTS}/*.parquet', filename = true)
        GROUP BY filename
    """).fetchall()
    return rows


def process_day(day: str, files: list[str], threads: int) -> tuple[str, int, int]:
    con = connect(threads)
    out = EVENTS_DAILY / f"dt={day}"
    out.mkdir(parents=True, exist_ok=True)

    flist = "[" + ",".join(f"'{f}'" for f in files) + "]"
    before = con.execute(
        f"SELECT count(*) FROM read_parquet({flist}) "
        f"WHERE strftime(created_at, '%Y-%m-%d') = '{day}'"
    ).fetchone()[0]

    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY id ORDER BY version DESC) AS rn
                FROM read_parquet({flist})
                WHERE strftime(created_at, '%Y-%m-%d') = '{day}'
            ) WHERE rn = 1
        ) TO '{out}/data.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    after = con.execute(f"SELECT count(*) FROM '{out}/data.parquet'").fetchone()[0]
    con.close()
    return day, before, after


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()

    if EVENTS_DAILY.exists():
        shutil.rmtree(EVENTS_DAILY)
    EVENTS_DAILY.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con = connect(C.THREADS)
    print("indexing shard time ranges...")
    idx = build_index(con)
    con.close()

    # Enumerate days from the data, not from shard endpoints: a day that falls
    # entirely inside a single shard's range appears as neither a d0 nor a d1,
    # and deriving the list from endpoints silently drops it.
    con = connect(C.THREADS)
    days = [r[0] for r in con.execute(f"""
        SELECT DISTINCT strftime(created_at, '%Y-%m-%d') AS d
        FROM read_parquet('{C.EVENTS}/*.parquet')
        WHERE created_at IS NOT NULL
        ORDER BY d
    """).fetchall()]
    con.close()
    # A shard contributes to a day if the day falls inside its [d0, d1] range.
    per_day = {
        day: sorted(f for f, d0, d1 in idx if d0 <= day <= d1)
        for day in days
    }
    print(f"{len(days)} days, {len(idx)} shards "
          f"(median {sorted(len(v) for v in per_day.values())[len(days)//2]} shards/day)")

    tot_b = tot_a = 0
    threads = max(2, C.THREADS // args.workers)
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_day, d, per_day[d], threads): d for d in days}
        for i, f in enumerate(as_completed(futs), 1):
            day, b, a = f.result()
            tot_b += b
            tot_a += a
            print(f"  [{i}/{len(days)}] {day}  {b:,} -> {a:,}  (-{b - a:,})", flush=True)

    print(f"\n{tot_b:,} rows -> {tot_a:,} distinct tweets")
    print(f"removed {tot_b - tot_a:,} cross-shard duplicates "
          f"({100 * (tot_b - tot_a) / tot_b:.2f}%)")
    print(f"done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
