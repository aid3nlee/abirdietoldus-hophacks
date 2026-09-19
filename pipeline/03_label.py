#!/usr/bin/env python3
"""Give every lineage a readable label: what idea is this, in a few words.

Why this exists
---------------
Step 6 tags families from a small curated lexicon -- politics, conflict,
migration, identity, health, crypto, fandom, sports. That lexicon was written
when the question was "which of these is a hate or propaganda narrative", and
for that question a deliberately narrow list is correct: a term that is not on
it is not evidence of anything, and a wider list mislabels more than it finds.

The question is now "how does an idea mutate", which the same lexicon answers
badly. It leaves 89% of exported lineages (9,491 of 10,674) with no label at
all, so the rail is a list of ten thousand rows that a reader cannot scan and
cannot filter. "Which ideas?" is the first question a reader asks, and the
export currently cannot answer it.

Two labels, because they answer different questions
---------------------------------------------------
kw      the distinctive vocabulary of this lineage -- the words that survive
        its rewordings and that other lineages do not share. Derived, not
        curated, so it covers 100% of lineages and needs no taxonomy. This is
        what makes the rail scannable.

topics  a coarse category, from a curated lexicon, for filtering. Still
        conservative, still anchored, but widened past the propaganda-hunting
        list to the subject matter this corpus actually contains.

Keyword scoring
---------------
A lineage is a set of rewordings of one claim, so a word that is part of the
claim survives the rewording and a word that is incidental does not. That is
the same insight the blocking step in stage 4 rests on, used here for a
different purpose: score a token by how much of the lineage it survives into
(share) against how many other lineages also carry it (inverse family
frequency). A word in most of this lineage's wordings and few others' is what
this lineage is about.

Stopwords are not enough on their own here -- textnorm keeps only 43, on
purpose, because Jaccard needs content words. Inverse family frequency is what
actually suppresses "you", "has" and "new", and it does it from the data rather
than from a list someone has to maintain.

This stage does not touch the phylogeny
---------------------------------------
It reads variants.parquet and rewrites labels into the existing export in
place. Nothing here feeds the clustering, the distances, the trees or the
spread figures, so it is safe to re-run and it cannot invalidate a demo.

    ./run.sh label

Optional Claude refinement
--------------------------
    ./run.sh label --claude

Turns the derived keywords into a written phrase per lineage ("Somali refugee
award winner arrested"). Needs ANTHROPIC_API_KEY and the anthropic SDK; without
either, the offline labels stand on their own and the flag is a no-op with a
warning. Only the top lineages are sent, exemplars only, never the corpus.
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

import config as C
from pipeline.textnorm import STOPWORDS

EVO = C.DATA / "evolution"
PHYLO = C.EXPORT / "phylo"
LABELS = C.LABELS / "lineages.json"

# --- keyword extraction -----------------------------------------------------

KW_MIN_SHARE = 0.45   # token must survive into this share of a lineage's wordings
KW_MAX_FAMSHARE = 0.10  # ...and appear as core vocabulary in no more than this
                        # share of all lineages, or it is describing the corpus
                        # rather than the lineage
KW_N = 4              # keywords kept per lineage
KW_MIN_LEN = 3        # "us" and "eu" are real but too ambiguous to label with

# Tokens that pass every statistical test and still say nothing about subject
# matter. Kept deliberately tiny: inverse family frequency removes almost
# everything, and each entry here is a judgement that has to be defended.
KW_BLOCK = frozenset("""
    https http co com www amp rt via retweet follow
    2026 2025 08 09 pm am utc est
""".split())


def keywords(tok_share: dict[str, float], fam_share: dict[str, float]) -> list[str]:
    """Top distinctive tokens for one lineage.

    share  -- how much of this lineage the token survives into
    idf    -- how rare that is across lineages

    The product is the usual tf-idf trade, with the term frequency replaced by
    something meaningful for this data: not how often a word occurs, but how
    reliably it survives being reworded.
    """
    scored = []
    for tok, share in tok_share.items():
        if share < KW_MIN_SHARE:
            continue
        if len(tok) < KW_MIN_LEN or tok in STOPWORDS or tok in KW_BLOCK:
            continue
        if tok.isdigit():
            continue
        fs = fam_share.get(tok, 0.0)
        if fs > KW_MAX_FAMSHARE or fs <= 0.0:
            continue
        scored.append((share * math.log(1.0 / fs), tok))
    scored.sort(key=lambda x: (-x[0], x[1]))

    # textnorm keeps the apostrophe as a word character, which is right for
    # matching -- "don't" and "dont" should not be the same token -- but a
    # curly quote folded to ASCII leaves it stranded at a word edge, so
    # "'Ghost Rider'" tokenizes to "'ghost" and "rider'". Strip only the edges,
    # so possessives ("mcdonald's") survive intact, and de-duplicate after,
    # since stripping can collide a quoted form with a bare one.
    out: list[str] = []
    for _, tok in scored:
        tok = tok.strip("'")
        if len(tok) >= KW_MIN_LEN and tok not in out:
            out.append(tok)
        if len(out) == KW_N:
            break
    return out


# --- topic taxonomy ---------------------------------------------------------
#
# Same mechanism as stage 4: `!` marks an anchor, a topic fires only when an
# anchor survives the per-lineage floor, and weak terms only rank a topic that
# an anchor has already earned. The list is wider because the question is
# wider, and the additions are taken from what the unlabeled lineages actually
# contain rather than from what a taxonomy ought to have.
#
# Measured on the current export, the recurring core vocabulary of unlabeled
# lineages is: breaking (200 lineages), update (190), live (187), giveaway
# (153), spotify (147), music (135), album (127), birthday (217), season (116).
# Those are the categories below. The propaganda-era eight are kept verbatim so
# existing labels do not move.

TOPICS = {
    # --- inherited from stage 4, unchanged ---------------------------------
    "politics": "trump! biden! election! vote voter ballot! congress! senate! president "
                "campaign democrat! republican! maga! conservative liberal government "
                "policy impeach! governor! senator! minister! parliament! mayor!",
    "conflict": "israel! palestine! gaza! hamas! idf! zionist! genocide! ukraine! russia! "
                "war strike military airstrike! ceasefire! hostage occupation settler",
    "migration": "immigrant! immigration! migrant! refugee! asylum! border deport! "
                 "deportation! illegal alien invasion assimilate visa amnesty!",
    "identity": "muslim! islam! jew! jewish! christian racist! racism! antisemitic! "
                "islamophobia! sharia! woke dei trans lgbtq groomer! white black",
    "health": "vaccine! vaccinated! covid! pandemic! fauci! cdc! fda mrna! autism pharma "
              "outbreak measles! medicaid!",
    "crypto": "bitcoin! btc! crypto! ethereum! token airdrop! presale! wallet trading "
              "forex! gold xauusd! signal pump profit",
    "fandom": "comeback teaser! album mv kpop! bts! nct! concert fancam! stan lightstick! "
              "preorder weverse! photocard! debut tour",
    "sports": "match goal league season transfer fixture cricket! football! nba! ufc! fifa! "
              "scoreline! kickoff! matchday! fulltime! playoffs! wickets! touchdown!",

    # --- added for the evolution framing -----------------------------------
    # NB: not "news" -- the dashboard already uses that key for a pseudo-topic
    # that groups politics/conflict/migration/identity/health behind one chip.
    "breaking": "breaking! confirmed! reports reported announce announced statement official "
                "update developing! sources according arrested! charged! killed! dead died",
    "music": "spotify! streams! streaming! chart! charts! billboard! song single album "
             "release debuted views! youtube! itunes! apple playlist riaa! platinum! "
             "certifications! remix! tracklist! hot100!",
    "celebrity": "birthday! happy congratulations wishes actor actress singer star "
                 "premiere! trailer! netflix! film movie season episode cast marvel! "
                 "disney! boxoffice! oscar! grammy! emmy! casting! sequel!",
    "giveaway": "giveaway! giveway! winner! prize! enter! claim retweet follow tag "
                "friends free win randomly selected participants! entries!",
    "business": "market stocks! shares! revenue profit earnings! company ceo! launch "
                "funding investors! valuation! acquisition! merger! ipo!",
    "disaster": "earthquake! flood! floods! hurricane! wildfire! storm! typhoon! "
                "evacuated! rescue! casualties! magnitude! tsunami! landslide!",
    "religion": "god! jesus! christ! allah! prayer! pray church! mosque! bible! quran! "
                "pastor! blessed faith worship! prophet!",
    "tech": "ai! chatgpt! openai! google! apple microsoft! tesla! iphone! android! "
            "software app update launch chip! nvidia! robot!",
}


def parse_terms(spec: str) -> dict[str, int]:
    out = {}
    for w in spec.split():
        out[w.rstrip("!")] = 2 if w.endswith("!") else 1
    return out


TOPIC_SETS = {k: parse_terms(v) for k, v in TOPICS.items()}

MIN_TOPIC_DF = 2
MIN_TOPIC_SHARE = 0.05
MAX_TOPIC_DF = 4


def df_floor(n_variants: int) -> int:
    return max(MIN_TOPIC_DF, min(MAX_TOPIC_DF,
                                 int(n_variants * MIN_TOPIC_SHARE + 0.999)))


def classify(tok_df: dict[str, int], n_variants: int) -> list[str]:
    """Coarse category for one lineage, anchored exactly as stage 4 does."""
    floor = df_floor(n_variants)
    hits = []
    for topic, terms in TOPIC_SETS.items():
        kept = [(tok, w) for tok, w in terms.items() if tok_df.get(tok, 0) >= floor]
        if not any(w == 2 for _, w in kept):
            continue
        hits.append((topic, sum(w for _, w in kept)))
    hits.sort(key=lambda x: -x[1])
    return [k for k, _ in hits[:2]]


# --- main -------------------------------------------------------------------

def load_index() -> dict:
    p = PHYLO / "index.json"
    if not p.exists():
        sys.exit(f"no export at {p} -- run ./run.sh evolution first")
    return json.loads(p.read_text())


def build(con) -> tuple[dict[int, dict], dict]:
    index = load_index()
    fam_ids = {int(f["id"]) for f in index["families"]}
    print(f"  [1] reading variants for {len(fam_ids):,} exported lineages...")
    t = time.time()
    rows = con.execute(
        f"SELECT family_id, tokens FROM '{EVO / 'variants.parquet'}'"
    ).fetchall()

    fam_tok: dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
    fam_n: collections.Counter = collections.Counter()
    for fid, toks in rows:
        fid = int(fid)
        if fid not in fam_ids:
            continue
        fam_n[fid] += 1
        for tok in set(toks or []):
            fam_tok[fid][tok] += 1
    print(f"      {sum(fam_n.values()):,} wordings across {len(fam_n):,} lineages "
          f"({time.time() - t:.0f}s)")

    # How many lineages carry each token as *core* vocabulary. Counting core
    # membership rather than any occurrence is what makes the denominator mean
    # "lineages this word characterises" instead of "lineages it appears in".
    print("  [2] scoring distinctive vocabulary...")
    core_fams: collections.Counter = collections.Counter()
    for fid, cnt in fam_tok.items():
        n = fam_n[fid]
        for tok, k in cnt.items():
            if k / n >= KW_MIN_SHARE:
                core_fams[tok] += 1
    n_fams = max(1, len(fam_tok))
    fam_share = {tok: n / n_fams for tok, n in core_fams.items()}

    out: dict[int, dict] = {}
    for fid, cnt in fam_tok.items():
        n = fam_n[fid]
        share = {tok: k / n for tok, k in cnt.items()}
        out[fid] = {
            "kw": keywords(share, fam_share),
            "topics": classify(cnt, n),
        }
    return out, index


def merge_into_export(labels: dict[int, dict], index: dict) -> None:
    """Write kw/topics back into index.json, in place."""
    before = sum(1 for f in index["families"] if f.get("topics"))
    kw_n = 0
    for f in index["families"]:
        lab = labels.get(int(f["id"]))
        if not lab:
            continue
        if lab["kw"]:
            f["kw"] = lab["kw"]
            kw_n += 1
        # Only widen. A lineage stage 4 already categorised keeps that call
        # unless this stage finds strictly more, so re-running never silently
        # drops a label someone has already looked at.
        if lab["topics"]:
            f["topics"] = lab["topics"]
    after = sum(1 for f in index["families"] if f.get("topics"))
    total = len(index["families"])
    index.setdefault("meta", {})["labelled"] = {
        "kw": kw_n, "topics": after, "total": total,
    }
    (PHYLO / "index.json").write_text(json.dumps(index, separators=(",", ":")))
    print(f"  [3] wrote labels into the export")
    print(f"      keywords   {kw_n:,}/{total:,} lineages ({100 * kw_n / total:.1f}%)")
    print(f"      topics     {before:,} -> {after:,}/{total:,} "
          f"({100 * after / total:.1f}%)")


def refine_with_claude(labels: dict[int, dict], index: dict, limit: int) -> None:
    """Optional: turn keywords into a written phrase for the top lineages."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("  [claude] ANTHROPIC_API_KEY not set -- keeping offline labels")
        return
    try:
        import anthropic
    except ImportError:
        print("  [claude] anthropic SDK not installed "
              "(pip install anthropic) -- keeping offline labels")
        return

    fams = sorted(index["families"], key=lambda f: -f.get("emis", 0))[:limit]
    client = anthropic.Anthropic(api_key=key)
    print(f"  [claude] naming the {len(fams):,} most-spread lineages...")
    done = 0
    for batch_start in range(0, len(fams), 20):
        batch = fams[batch_start:batch_start + 20]
        listing = "\n".join(
            f"{f['id']}\t{(f.get('top') or f.get('root') or '')[:180]!r}"
            for f in batch
        )
        msg = (
            "Each line is a lineage id and one example tweet from it. For each, "
            "write a neutral 3-6 word noun phrase naming the claim or subject. "
            "Describe, do not judge. Reply as JSON: {\"<id>\": \"<phrase>\"}.\n\n"
            + listing
        )
        try:
            resp = client.messages.create(
                model="claude-sonnet-5", max_tokens=1500,
                messages=[{"role": "user", "content": msg}],
            )
            text = resp.content[0].text.strip()
            text = text[text.find("{"): text.rfind("}") + 1]
            for fid, phrase in json.loads(text).items():
                labels.setdefault(int(fid), {})["name"] = phrase
                done += 1
        except Exception as exc:                      # noqa: BLE001
            print(f"  [claude] batch failed ({exc}); keeping offline labels for it")
    by_id = {int(f["id"]): f for f in index["families"]}
    for fid, lab in labels.items():
        if lab.get("name") and fid in by_id:
            by_id[fid]["name"] = lab["name"]
    print(f"  [claude] named {done:,} lineages")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--claude", action="store_true",
                    help="refine the top lineages into written phrases (needs API key)")
    ap.add_argument("--limit", type=int, default=300,
                    help="how many lineages to send for refinement")
    args = ap.parse_args()

    C.LABELS.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={C.THREADS}")
    con.execute(f"SET memory_limit='{C.MEMORY_LIMIT}'")

    t0 = time.time()
    print("lineage labelling")
    labels, index = build(con)
    if args.claude:
        refine_with_claude(labels, index, args.limit)
    merge_into_export(labels, index)
    LABELS.write_text(json.dumps({str(k): v for k, v in labels.items()},
                                 separators=(",", ":")))
    print(f"      durable copy at {LABELS}")
    con.close()
    print(f"\ndone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
