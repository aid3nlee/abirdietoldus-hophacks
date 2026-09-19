"""Stance and polarity for a comment, with no model download.

Two scores that answer different questions, kept separate on purpose:

  STANCE   what the comment is *doing* to the claim it answers -- agreeing,
           contradicting, ridiculing, asking, or farming engagement off it.
  POLARITY how positive or negative the wording is, from VADER.

They are not interchangeable and neither substitutes for the other. VADER
scores "source? this is debunked" at 0.00 because it carries no affect words,
and "lmao the cope is real" at +0.60 because it reads the laughter and misses
the contempt. Polarity cannot tell agreement from disagreement, which is the
one thing this project needs from a reply. So stance is decided by lexicon and
polarity is carried alongside it as a shade, never as the label.

Why a lexicon rather than a classifier: the honest alternative is a
transformer, and a transformer that has not been calibrated on this corpus
would be a black box producing numbers nobody in the room can defend. Every
decision here is a visible rule that can be read off the table below, argued
with, and corrected. That is worth more than a few points of accuracy on a
task where the failure mode is a confident wrong label in front of a judge.

Measured on this corpus, 83% of comments carry no explicit stance cue at all.
That is mostly not a failure of the lexicon: a quote tweet typically uses the
tweet it quotes as a springboard rather than arguing with it, so "takes no
position" is the honest label for most of them. Those comments keep the
`none` stance and are read on the tone axis instead, which is why tone exists
as a separate field rather than being folded into the stance label.

`attack` is hostility directed at a target, not hate speech. Calling someone
a bigot is condemnation and lands in `attack` exactly as a slur would; this
lexicon deliberately does not try to tell those apart, and nothing downstream
should present an `attack` count as a hate-speech count.

Cues were pruned after reading what they actually caught. A bare "wrong"
classified "God keep taking the wrong white people" as a *dispute* -- it is
not a contradiction of anything, and it was the most-liked comment on the
busiest node in the corpus, so it would have been on screen. Bare "sick" is
positive slang, "actually" and "context" appear in ordinary prose, and "hack",
"bot" and "gross" are ordinary nouns. All are gone; the phrase forms that
carry the same meaning unambiguously ("that s wrong", "you re wrong") stay.

Known limits, stated rather than hidden:
  - Sarcasm defeats it. "great reporting as always" scores endorse.
  - Quoting the claim in order to dunk on it looks like endorse unless a
    dispute, mock or attack cue appears alongside.
  - It is English-only, and reads a non-English comment as `none`.
  - Tone is VADER's compound score bucketed at +/-0.35. It reads laughter as
    positive, so a mocking comment often scores positive tone; tone is a
    reading of the wording, never of the intent.
"""
from __future__ import annotations

import re

# --- cue lexicons -----------------------------------------------------------
# Each entry is matched on the normalized (lowercased, punctuation-spaced)
# body. Multi-word cues are matched as phrases, so "no evidence" does not fire
# on a comment that happens to contain both words far apart.

_ENDORSE = [
    "exactly", "this is true", "so true", "100%", "1000%", "facts",
    "well said", "agreed", "i agree", "agree", "spot on",
    "preach", "based", "absolutely", "couldn t agree", "could not agree",
    "say it louder", "thank you for saying", "this needs", "amen", "yes sir",
    "real talk", "nailed it", "perfectly said", "finally someone", "bingo",
    "this right here", "say it", "you re right", "youre right", "he s right",
    "love this", "beautiful", "gorgeous", "incredible", "goat", "legend",
    "king", "queen", "icon", "masterpiece", "deserved", "rest in peace",
    "rip", "condolences", "proud of", "congrats", "congratulations",
    "💯", "🔥", "🙏", "❤️",
]
_DISPUTE = [
    "false", "fake", "not true", "untrue", "incorrect", "lie", "lying",
    "lies", "debunked", "misleading", "misinformation", "disinformation",
    "no evidence", "source", "citation", "fact check",
    "factcheck", "that s not", "thats not", "this is not", "nonsense",
    "propaganda", "hoax", "bullshit", "nope", "wrong again", "you re wrong",
    "that s wrong", "thats wrong", "just wrong", "flat out wrong",
    "youre wrong", "never happened", "out of context", "prove it", "proof",
    "doesn t say", "does not say", "misrepresent", "citation needed",
    "that s false", "thats false", "not what", "didn t happen", "did not happen",
    "where s the", "wheres the", "ai generated", "ai slop", "photoshop",
    "old video", "old news", "🧢",
]
_MOCK = [
    "lol", "lmao", "lmfao", "rofl", "haha", "hahaha", "clown", "clownish",
    "cope", "copium", "ratio", "l take", "bad take", "touch grass", "cringe",
    "embarrassing", "pathetic", "delusional", "braindead", "brain dead",
    "you re joking", "youre joking", "is this satire", "parody", "self own",
    "self report", "the irony", "ironic", "sure buddy", "ok buddy", "yeah right",
    "😂", "🤣", "💀", "🤡",
]
_ATTACK = [
    "idiot", "idiots", "moron", "morons", "stupid", "dumb", "dumbass",
    "trash", "garbage", "shut up", "shut the", "fuck you", "fuck off",
    "fuk u", "fuck u", "stfu", "loser", "losers", "scum", "scumbag", "evil",
    "disgrace", "disgusting", "disgusted", "vile", "monster", "coward",
    "grifter", "shill", "bootlicker", "traitor",
    "piece of shit", "asshole", "pos", "creep", "weirdo", "psycho",
    "racist", "bigot", "nazi", "fascist", "terrorist",
    "brainwash", "brainwashed", "sheeple", "npc", "shameful", "shame on",
    "rot in", "get a life", "nobody asked", "who asked",
]
_PROMO = [
    "giveaway", "give away", "follow me", "follow back", "followback",
    "retweet to enter", "rt to enter", "like and rt", "like rt", "drop your",
    "drop a", "check out my", "dm me", "dm for", "link in bio", "promo",
    "airdrop", "whitelist", "mint", "join my", "subscribe", "my username",
    "username", "my id", "make it rain", "rain on me", "pick me", "count me in",
    "let s gooo", "entered", "entering", "🆔",
]

# Structural promo: an entry-form reply is mostly an identifier plus tags, and
# says nothing about the claim. Catching the shape catches variants the word
# list misses.
_ID_FORM = re.compile(r"\b(?:id|username|user|tag|handle)\s*[:\-]", re.I)

_NORM = re.compile(r"[^a-z0-9À-￿]+")
_URL = re.compile(r"https?://\S+|\bt\.co/\S*")
_MENTION_LEAD = re.compile(r"^(?:\s*@[A-Za-z0-9_]{1,15})+")

STANCES = ("endorse", "dispute", "mock", "attack", "question", "promo", "none")
TONES = ("pos", "neu", "neg")

# VADER compound is bucketed rather than shown raw: the exact value implies a
# precision the lexicon behind it does not have.
TONE_CUT = 0.35

_analyzer = None


def _vader():
    global _analyzer
    if _analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


def _prep(body: str) -> tuple[str, str]:
    """Return (stripped body for VADER, normalized string for cue matching).

    The leading @handles of a reply are addressing, not content, and leaving
    them in makes every reply in a thread share a token. URLs are dropped for
    the same reason.
    """
    b = _MENTION_LEAD.sub(" ", body or "")
    b = _URL.sub(" ", b)
    flat = " " + _NORM.sub(" ", b.lower()).strip() + " "
    return b.strip(), flat


def _count(flat: str, cues: list[str]) -> int:
    n = 0
    for c in cues:
        # Emoji and other non-word cues cannot be space-delimited.
        if c.isascii() and c.replace(" ", "").isalnum():
            if f" {c} " in flat:
                n += 1
        elif c in flat:
            n += 1
    return n


def score(body: str) -> dict:
    """Classify one comment. Returns stance, the cue counts behind it, tone."""
    raw, flat = _prep(body)
    if not raw:
        return {"stance": "none", "pol": 0.0, "tone": "neu", "cues": {}}

    c = {
        "promo": _count(flat, _PROMO) + (1 if _ID_FORM.search(raw) else 0),
        "dispute": _count(flat, _DISPUTE),
        "attack": _count(flat, _ATTACK),
        "mock": _count(flat, _MOCK),
        "endorse": _count(flat, _ENDORSE),
    }
    q = raw.count("?")

    # Precedence, and the reasoning for it:
    #  promo first -- an entry form is not a position on the claim at all, so
    #    letting "thank you" inside it score endorse would be wrong.
    #  dispute over attack -- a comment that both contradicts and insults is
    #    reported as the contradiction, because that is the part that engages
    #    with the claim rather than with the person.
    #  attack and mock over endorse for the same reason: hostility and
    #    ridicule are not agreement, whatever warm words sit beside them.
    #  question only when nothing else fired, since most cued comments that
    #    contain a "?" are rhetorical.
    if c["promo"] >= 2 or (c["promo"] and not any(v for k, v in c.items() if k != "promo")):
        stance = "promo"
    elif c["dispute"]:
        stance = "dispute"
    elif c["attack"]:
        stance = "attack"
    elif c["mock"]:
        stance = "mock"
    elif c["endorse"]:
        stance = "endorse"
    elif q:
        stance = "question"
    else:
        stance = "none"

    try:
        pol = float(_vader().polarity_scores(raw)["compound"])
    except Exception:
        pol = 0.0
    tone = "pos" if pol >= TONE_CUT else "neg" if pol <= -TONE_CUT else "neu"
    return {"stance": stance, "pol": round(pol, 3), "tone": tone,
            "cues": {k: v for k, v in c.items() if v}}


def score_many(bodies) -> list[dict]:
    return [score(b) for b in bodies]
