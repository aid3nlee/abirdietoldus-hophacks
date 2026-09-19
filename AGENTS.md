# AGENTS.md

Repository-wide rules for automated and human contributors.

The offline pipeline (`pipeline/`, `config.py`, `run.sh`) and the static
dashboard (`dashboard/index.html`) are existing, working implementations.
Preserve them. Read `README.md` for what the corpus is and what it cannot
support, and `docs/consumer-extension-plan.md` for the consumer-extension work
in progress.

## Scope

Active work on `feature/consumer-extension` is limited to three features:
coordination evidence beside visible X posts, earlier context and wording
comparisons, and user-controlled muting of repeated versions of a story.
Anything else is out of scope for that branch. Do not expand it without the
orchestrator's approval.

## Evidence rules

These exist because the corpus does not support the claims a reader will
naturally assume we are making. They are not stylistic.

1. **State the evidence's actual scope.** Coordination evidence in this project
   is *historical, narrative-level, and measured over a fixed corpus window
   (2026-08-17 → 2026-09-17)*. Never present it as live, as current, or as a
   property of the account whose post is on screen. Always report the
   denominator a figure was computed against.

2. **Never attribute automation, nationality, or actor identity from text
   similarity.** We can show that wordings recur and that accounts co-amplified.
   We cannot show "bot", "foreign", "state-backed", or "inauthentic actor", and
   the corpus has no account metadata that could. No bot probability, no
   country flag, no truth score reaches a user.

3. **`rt_handle` is the amplified account, not the amplifier.** It identifies
   who was retweeted. Do not render it as the author of the post being examined,
   and do not treat it as evidence about the retweeter.

4. **A narrative's history is not its current author's record.** Matching a
   post's wording to a coordinated family says something about the wording, not
   about the person who just posted it. Author-level evidence is `unknown`
   unless a reliable author-id join exists — and for this corpus it does not.

5. **Earliest observed, never proven origin.** The root of a family is the
   earliest phrasing *sampled in this corpus*. Label it "earliest observed".
   Never "origin", "patient zero", "source of the claim". Equal timestamps do
   not imply direction. An unknown or unsupported parent is a permitted,
   displayable outcome.

6. **Similarity is not confidence.** Report `exact | similar | ambiguous | none`
   with the method that produced it. Do not render a Jaccard score or any
   similarity number as a probability or a percentage confidence.

7. **Missing evidence is shown, not hidden.** Unmatched, unassessable
   (out of corpus window), and unmeasured are three different states and must be
   visibly different. A post we know nothing about stays explicitly unassessed —
   never silently clean. Fixture data is labelled as fixture, everywhere it
   surfaces.

8. **Keep the collection-gap disclosure.** Crawl volume drops 15.3× on
   2026-09-01. Anything that compares counts across that date must carry the
   warning already in `meta.cliff_note`.

## Feature rules

9. **Muting is reversible and deliberate.** It requires an explicit user action,
   expires on a documented schedule, offers Show once / Undo / Clear all, and
   collapses posts with an accessible placeholder rather than deleting native
   DOM nodes. Restore on expiry or undo.

10. **Preserve quotes, rebuttals, corrections, and new information.** Matching
    must abstain when negation, stance, quotation, numbers, dates, entities, or
    materially new material could change the claim. When uncertain, show the
    post. Never mute a whole offline family just because it shares a
    `family_id`; those families can contain weak links and opposing statements.

11. **Do not present an LLM's interpretation as source evidence.** These three
    features require no paid API and no model in the serving path. Diffs come
    from the actual full texts.

12. **Never invent a source URL.** Link only to records we actually matched, and
    distinguish a sampled event's permalink from an identified upstream source.
    Where the original was never sampled, say so.

## Data and identity rules

13. **Tweet and account ids are strings** everywhere — JSON, Python, JS, tests.
    They exceed 2^53.

14. **Do not join records by truncated display text.** `node.txt` is a 280-char
    slice, `family.root`/`family.top` are 180-char slices, and `05_enrich.py`
    rewrites some of them afterwards. Join on canonical keys.

15. **Version the exports.** Every export carries a dataset version id, and
    every cache key and deep link includes it. Do not silently rewrite archived
    trees; version a changed export and document the change.

## Repository hygiene

16. **No secrets and no bulk datasets in Git.** `twitter-firehose/`, `data/`,
    `*.parquet`, `dist/`, `.dev/` stay gitignored. Never commit credentials,
    tokens, or a `.env`. Fixtures must be small and clearly labelled.

17. **Preserve `main` and teammates' work.** Work on the task branch. Never
    force-push, never reset shared history, never stage unrelated changes.
    Fetch before branching — `main` moves.

18. **Report verification honestly.** Distinguish code inspection, locally
    executed tests, CI results, and manual browser testing. Do not describe
    something as tested if it was not run. Record failures rather than omitting
    them.
