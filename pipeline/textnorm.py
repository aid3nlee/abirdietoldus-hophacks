"""
Shared text normalization for variant detection.

Two jobs, and they pull in opposite directions:

  1. Collapse cosmetic differences so that two postings of the same message
     land on the same token set -- otherwise every retweet of a slogan looks
     like a fresh mutation.
  2. *Measure* the cosmetic differences rather than silently discarding them,
     because deliberate obfuscation (Cyrillic homoglyphs, math-bold styling,
     emoji standing in for a word) is itself the signal we are looking for:
     it is what a narrative does under keyword-filter pressure.

So normalization is lossy on purpose, but every lossy step increments a
counter that survives into the output as an evasion feature.

The confusable map is generated, not hand-written. Unicode's NFKC folding
already knows that MATHEMATICAL BOLD CAPITAL R is an R; we walk the ranges
where that is true and emit a `translate()` pair for each. Homoglyphs from
other scripts (Cyrillic а, Greek ο) are *not* NFKC-foldable -- they are
genuinely different letters -- so those are listed explicitly.
"""
from __future__ import annotations

import html
import re
import unicodedata

# --- generated confusables --------------------------------------------------

# Ranges where NFKC folds a styled codepoint down to a plain ASCII alnum.
_NFKC_RANGES = [
    (0x00A0, 0x024F),    # latin-1 + extended-A/B (accented forms)
    (0x1D400, 0x1D7FF),  # mathematical alphanumeric symbols (bold/script/fraktur/...)
    (0xFF01, 0xFF5E),    # fullwidth forms
    (0x2460, 0x24FF),    # enclosed alphanumerics (circled letters/digits)
    (0x2100, 0x214F),    # letterlike symbols
    (0x1D2C, 0x1D6A),    # phonetic extensions (superscript/small-cap letters)
    (0x2070, 0x209F),    # super/subscripts
]

# Cross-script lookalikes. NFKC will never fold these, which is exactly why
# they are the preferred evasion vehicle.
_HOMOGLYPHS = {
    # Cyrillic
    "а": "a", "б": "b", "в": "b", "г": "r", "е": "e", "ѕ": "s", "і": "i",
    "ј": "j", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c",
    "т": "t", "у": "y", "х": "x", "ч": "4", "һ": "h", "ԁ": "d", "ԛ": "q",
    "ԝ": "w", "ѡ": "w", "ѵ": "v", "ё": "e",
    "А": "A", "В": "B", "Е": "E", "З": "3", "К": "K", "М": "M", "Н": "H",
    "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X", "Ј": "J",
    # Greek
    "α": "a", "β": "b", "γ": "y", "ε": "e", "ζ": "z", "η": "n", "ι": "i",
    "κ": "k", "μ": "u", "ν": "v", "ο": "o", "ρ": "p", "σ": "o", "τ": "t",
    "υ": "u", "χ": "x", "Α": "A", "Β": "B", "Ε": "E", "Ζ": "Z", "Η": "H",
    "Ι": "I", "Κ": "K", "Μ": "M", "Ν": "N", "Ο": "O", "Ρ": "P", "Τ": "T",
    "Υ": "Y", "Χ": "X",
    # Armenian / Cherokee / other frequent offenders
    "ո": "n", "օ": "o", "ս": "u", "Ꭺ": "A", "Ꭼ": "E", "Ꮃ": "W", "Ꮋ": "H",
}

# Typographic punctuation. These fold for the same reason as everything above
# -- they break tokenization -- but they are NOT evidence of anything. Smart
# quotes come from phone keyboards and the ellipsis is what Twitter itself
# appends when it truncates a retweet, so counting them as obfuscation made
# the evasion signal a measure of "was this tweet cut off". Folded like the
# rest, excluded from the evasion counters below.
_PUNCT = {
    "‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-", "―": "-",
    "‘": "'", "’": "'", "‚": "'", "‛": "'", "＇": "'",
    "“": '"', "”": '"', "„": '"', "‟": '"',
    "․": ".", "‥": ".", "…": ".", "⁄": "/", "∕": "/", "％": "%",
}


def _styled_map() -> dict:
    """Codepoints that NFKC folds to a plain ASCII alphanumeric.

    Mathematical bold/script/fraktur, fullwidth forms, circled letters. Using
    these is a deliberate styling choice, and one that defeats a naive keyword
    filter, but it is also just how a lot of promo copy is written -- so it is
    a weaker signal than a cross-script homoglyph and is counted separately.
    """
    out = {}
    for lo, hi in _NFKC_RANGES:
        for cp in range(lo, hi + 1):
            ch = chr(cp)
            if ch in out:
                continue
            folded = unicodedata.normalize("NFKC", ch)
            # Only single-char ASCII alnum folds are safe for a 1:1 translate.
            if len(folded) == 1 and folded.isascii() and folded.isalnum() and folded != ch:
                out[ch] = folded
    return out


_STYLED = _styled_map()


def _join(*maps: dict) -> tuple[str, str]:
    src, dst, seen = [], [], set()
    for m in maps:
        for ch, folded in m.items():
            if ch not in seen:
                src.append(ch)
                dst.append(folded)
                seen.add(ch)
    return "".join(src), "".join(dst)


# Everything that gets folded before tokenization. Punctuation included: two
# postings that differ only in apostrophe style are the same variant.
CONF_FROM, CONF_TO = _join(_STYLED, _HOMOGLYPHS, _PUNCT)

# What we *count* as evasion. Punctuation is deliberately absent; see _PUNCT.
STYLED_FROM = "".join(_STYLED)
HOMOGLYPH_FROM = "".join(_HOMOGLYPHS)
EVASION_FROM = STYLED_FROM + HOMOGLYPH_FROM

_HOMO_SET = set(_HOMOGLYPHS)
_STYLED_SET = set(_STYLED)
_LATIN_WORD = re.compile(r"[A-Za-z\u0370-\u058F]{2,}")


def evasion_profile(text: str) -> dict:
    """Measure deliberate character substitution in one message.

    Three counters, weakest to strongest:

      styled  -- math-bold and friends. Common in ordinary promo copy.
      homo    -- cross-script lookalikes anywhere in the message.
      mixed   -- words that are *part* Latin and part lookalike. This is the
                 one that is hard to explain innocently: a fully Cyrillic word
                 is just Russian, but "gеnocide" carrying a single Cyrillic е
                 exists to survive a keyword match and nothing else.
    """
    t = text or ""
    styled = sum(1 for ch in t if ch in _STYLED_SET)
    homo = sum(1 for ch in t if ch in _HOMO_SET)
    mixed = 0
    if homo:
        for w in _LATIN_WORD.findall(t):
            has_h = any(ch in _HOMO_SET for ch in w)
            has_l = any(ch.isascii() and ch.isalpha() for ch in w)
            if has_h and has_l:
                mixed += 1
    return {"styled": styled, "homo": homo, "mixed": mixed}

# --- script detection -------------------------------------------------------

# Twitter's lang field is the only language signal in the firehose and it is
# unreliable for hashtag-heavy posts: a Thai fancam caption whose hashtags and
# artist names are Latin gets tagged "en". Tokenization then strips to
# [a-z0-9'], so the non-Latin body contributes no tokens at all and the
# lineage is built entirely from its hashtag block. Measuring the script mix
# lets a caller filter on what the text actually is rather than on what the
# platform guessed.
_SCRIPT_RANGES = [
    (0x0E00, 0x0E7F),    # Thai
    (0x1100, 0x11FF),    # Hangul Jamo
    (0x3040, 0x30FF),    # Hiragana + Katakana
    (0x3400, 0x4DBF),    # CJK ext A
    (0x4E00, 0x9FFF),    # CJK unified
    (0xAC00, 0xD7AF),    # Hangul syllables
    (0x0600, 0x06FF),    # Arabic
    (0x0590, 0x05FF),    # Hebrew
    (0x0900, 0x097F),    # Devanagari
]


def _is_other_script(ch: str) -> bool:
    cp = ord(ch)
    return any(lo <= cp <= hi for lo, hi in _SCRIPT_RANGES)


def nonlatin_share(text: str) -> float:
    """Share of a message's letters that belong to a non-Latin script.

    Counts letters only, so URLs, hashtag punctuation, digits and emoji do not
    dilute the result.
    """
    t = text or ""
    letters = [ch for ch in t if ch.isalpha()]
    if not letters:
        return 0.0
    return sum(1 for ch in letters if _is_other_script(ch)) / len(letters)


# Above this share of non-Latin letters, treating the text as English prose is
# not defensible. Set low deliberately: these posts are mostly Latin hashtags
# wrapped around a non-Latin sentence, so the share is diluted by design.
NONLATIN_THRESHOLD = 0.15


# --- stopwords --------------------------------------------------------------

# Deliberately short. An aggressive stopword list would erase the function-word
# swaps ("will be" -> "is going to be") that are some of the most telling
# paraphrase mutations.
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "if", "of", "to", "in", "on", "at",
    "for", "with", "is", "are", "was", "were", "be", "been", "am", "it", "its",
    "this", "that", "these", "those", "as", "by", "from", "so", "than", "then",
    "too", "very", "can", "just", "rt", "amp", "http", "https", "co", "t",
}

# --- emoji ------------------------------------------------------------------

# RE2 syntax (DuckDB); pictographs, dingbats, symbols, variation selectors,
# skin-tone modifiers and regional indicators.
EMOJI_RE2 = (
    r"[\x{1F000}-\x{1FAFF}\x{2600}-\x{27BF}\x{2B00}-\x{2BFF}"
    r"\x{2190}-\x{21FF}\x{2300}-\x{23FF}\x{FE0F}\x{1F3FB}-\x{1F3FF}]"
)
_EMOJI_PY = re.compile(
    "[\U0001F000-\U0001FAFF☀-➿⬀-⯿"
    "←-⇿⌀-⏿️\U0001F3FB-\U0001F3FF]"
)

_URL = re.compile(r"https?://\S+|\bt\.co/\S*|\bht(?:t(?:p(?:s)?)?)?…")
_MENTION = re.compile(r"@[A-Za-z0-9_]{1,15}")
_HASHTAG = re.compile(r"#(\w+)")
_NONWORD = re.compile(r"[^a-z0-9']+")
_REPEAT = re.compile(r"(.)\1{2,}")


def normalize(text: str) -> dict:
    """Python-side reference implementation, mirrored by the SQL in 04_evolution.

    Used for exemplars and diffing in the export step, where per-row Python is
    affordable. The bulk pass runs the same logic as DuckDB expressions.
    """
    raw = text or ""
    t = html.unescape(raw)
    emoji = _EMOJI_PY.findall(t)
    mentions = [m.lower() for m in _MENTION.findall(t)]
    hashtags = [h.lower() for h in _HASHTAG.findall(t)]
    obf = sum(1 for ch in t if ch in CONF_FROM)
    t = t.translate(str.maketrans(CONF_FROM, CONF_TO))
    t = _URL.sub(" ", t)
    t = _MENTION.sub(" ", t)
    t = t.replace("#", " ")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = _NONWORD.sub(" ", t.lower())
    t = _REPEAT.sub(r"\1\1", t)          # sooooo -> soo
    norm = " ".join(t.split())
    toks = [w for w in norm.split() if len(w) >= 2 and w not in STOPWORDS]
    return {
        "norm": norm,
        "tokens": sorted(set(toks)),
        "emoji": emoji,
        "mentions": mentions,
        "hashtags": hashtags,
        "obf_chars": obf,
    }


# --- search blobs -----------------------------------------------------------
# The dashboard's search box needs something quite different from the token
# sets above. Those are deliberately order-free and stopword-stripped, which is
# right for measuring similarity and wrong for finding a phrase: a sorted token
# bag cannot match "charlie kirk" unless those two words happen to be adjacent
# in the alphabet. So search gets its own representation, built from the raw
# variant text with word order intact.
#
# The old blob was `" ".join(sorted(tokens))[:320]`, which had both faults at
# once -- alphabetical order destroyed phrases, and the cut then removed every
# token from roughly "p" onward for the 21% of lineages that overflowed it.
# A term like "ycombinator" was effectively unsearchable.

_SB_URL = re.compile(r"https?://\S+|\bt\.co/\S*")
_SB_WS = re.compile(r"\s+")
_SB_WORD = re.compile(r"[a-z0-9#@']+")


def search_blob(texts, cap: int = 6000, min_new: int = 1) -> str:
    """Phrase-preserving, lowercased search text for one lineage.

    `texts` is an iterable of variant strings already ordered by importance --
    most-emitted first -- because `cap` cuts from the end, and if a lineage has
    to lose wordings it should lose the rarest ones.

    Variants within a lineage are near-duplicates by construction, so a
    variant contributing fewer than `min_new` words the blob does not already
    hold is skipped. That is what keeps the corpus a sane size: the saving
    comes from redundancy between variants, not from throwing away vocabulary.
    URLs go too -- a t.co link is 23 bytes nobody searches for.

    `min_new` is the size/recall dial, and it is a far better one than `cap`
    once a corpus is wide rather than deep: at 17,263 lineages the total is
    driven by how many lineages there are, not by the few hundred that overrun
    the cap, so cutting `cap` from 6000 to 3500 saves barely 6% while 1 -> 3
    here saves 21% for 1.9% of recall.
    """
    seen: set[str] = set()
    out: list[str] = []
    n = 0
    for t in texts:
        if not t:
            continue
        t = _SB_WS.sub(" ", _SB_URL.sub(" ", t)).strip().lower()
        if not t:
            continue
        words = set(_SB_WORD.findall(t))
        if words and len(words - seen) < min_new:
            continue
        seen |= words
        if n + len(t) + 1 > cap:
            continue
        out.append(t)
        n += len(t) + 1
    return " ".join(out)


def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    i = len(a & b)
    return i / (len(a) + len(b) - i)


if __name__ == "__main__":
    print(f"confusable map: {len(CONF_FROM)} codepoints")
    for s in [
        "𝐑𝐞𝐬𝐮𝐫𝐫𝐞𝐜𝐭𝐢𝐨𝐧 ІІ &lt;test&gt;",
        "Тhеу аrе rерlасіng us 🔥🔥 https://t.co/abc",
        "sooooo TRUE!!! #WakeUp @someone",
    ]:
        r = normalize(s)
        print(f"\n{s!r}\n  norm={r['norm']!r}\n  toks={r['tokens']}\n  obf={r['obf_chars']} emoji={r['emoji']}")
