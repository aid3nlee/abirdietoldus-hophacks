"""Static dashboard server with a server-side Gemini analysis endpoint."""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
MAX_BODY = 32 * 1024


def load_env_file() -> None:
    """Read KEY=value lines from a gitignored .env so the key need not be exported.

    A real environment variable always wins, so `GEMINI_API_KEY=... ./run.sh serve`
    still overrides the file.
    """
    try:
        text = (ROOT / ".env").read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def build_ssl_context() -> ssl.SSLContext:
    """Verify TLS even where the interpreter has no usable CA store.

    A python.org macOS build points at an openssl cert.pem that does not exist
    until `Install Certificates.command` is run, so an unpatched laptop fails
    every HTTPS call with CERTIFICATE_VERIFY_FAILED. Falling back to certifi's
    bundle keeps verification on rather than turning it off.
    """
    context = ssl.create_default_context()
    if context.cert_store_stats()["x509_ca"]:
        return context
    try:
        import certifi
    except ImportError:
        return context
    return ssl.create_default_context(cafile=certifi.where())


load_env_file()
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
SSL_CONTEXT = build_ssl_context()


# Closed vocabularies for the classification fields. The model is asked for one
# of these exact strings; anything else is coerced to the fallback rather than
# rendered, for the same reason the analysis is truncated -- the inspector is a
# fixed slot and an invented label would either break the layout or, worse,
# quietly read as a verdict the pipeline never produced.
TONES = ("reportorial", "alarmed", "outraged", "mocking", "celebratory",
         "earnest", "sardonic", "promotional", "neutral")
SOURCING = ("attributed", "hedged", "flat-assertion", "opinion", "unclear")
SHIFTS = ("stripped", "added", "unchanged", "n/a")


def one_of(value: object, allowed: tuple, fallback: str) -> str:
    """Coerce a model-supplied label into a known vocabulary."""
    candidate = str(value or "").strip().lower()
    return candidate if candidate in allowed else fallback


def clip_words(text: object, limit: int = 12) -> str:
    """Cap a free-text justification so it cannot run past its one line."""
    return " ".join(str(text or "").split()[:limit])


def unit(value: object) -> float:
    """Clamp a confidence to 0..1; the panel renders it as a percentage."""
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def two_sentences(text: str) -> str:
    """Hard-cap the analysis at two sentences.

    The prompt asks for two and usually gets two, but the panel it renders into
    is a fixed slot next to the tree -- a model having a verbose day must not be
    able to push the rest of the inspector off screen. Splits on sentence-ending
    punctuation followed by a space, so a decimal or an abbreviation mid-sentence
    does not count as a break.
    """
    parts = re.split(r"(?<=[.!?])\s+", str(text).strip())
    return " ".join(p for p in parts[:2] if p)


def gemini_review(payload: dict) -> dict:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    prompt = {
        "role": "user",
        "parts": [{"text": """You are the analyst in a narrative phylogeny tool. The pipeline traces how a phrasing spreads on Twitter and mutates as people reword it while copying. You are reading one wording from one lineage, along with the wording it descends from and the tokens that were added or dropped in between.

Say what this wording claims and what the reword changed about it. The supplied figures describe spread and reception, never intent: coordination, volume and hostile replies tell you how a phrasing travelled and how it landed, not that anyone posted it in bad faith. Do not invent facts, sources, intent or context, and do not fact-check the underlying claim -- you have no way to verify it. Keep what the text literally says separate from what a reader might infer.

Return JSON only with exactly these keys:
{
  "analysis": "at most two sentences",
  "tone": "one label",
  "sourcing": "one label",
  "sourcing_basis": "at most 12 words",
  "sourcing_shift": "one label",
  "confidence": 0.0
}

`analysis` is read off a dashboard, so it is hard-capped at TWO SENTENCES. Sentence one: what the wording says, and what the mutation from its parent changed about its meaning or force. Sentence two: what the spread and reception figures show about how it travelled. No preamble, no bullet points, no quoting the wording back verbatim.

The remaining fields classify the WORDING ITSELF. They describe how the text is written, never whether its claim is true -- you still cannot verify that and must not try. An unsourced claim is not thereby a false one, and nothing you return may imply that it is.

`tone` -- the register the wording is written in. Exactly one of:
reportorial | alarmed | outraged | mocking | celebratory | earnest | sardonic | promotional | neutral

`sourcing` -- whether the wording shows its work. Exactly one of:
attributed     -- names a source, outlet, document or speaker
hedged         -- asserts with explicit uncertainty: reportedly, appears, may
flat-assertion -- states a checkable factual claim with no source and no hedge
opinion        -- a value judgement or reaction, making no factual claim
unclear        -- too short or too fragmentary to tell

`sourcing_basis` -- at most 12 words naming the feature in the text that decided `sourcing`. Quote the words that did it.

`sourcing_shift` -- how the reword changed that grounding relative to the parent wording. Exactly one of:
stripped | added | unchanged | n/a
Use n/a when no parent wording was supplied. This is the field the tool cares about most: attribution and hedges are expensive to carry, so watch for them being dropped as a phrasing replicates.

Use a confidence from 0 to 1 for your reading of the text, not for the truth of the underlying claim."""},
        {"text": "\nSUPPLIED MATERIAL:\n" + json.dumps(payload, ensure_ascii=True)},
    ]}
    body = json.dumps({
        "contents": [prompt],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }).encode("utf-8")
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + quote(MODEL, safe="") + ":generateContent?key=" + quote(api_key, safe="")
    )
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=45, context=SSL_CONTEXT) as response:
            result = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError("Gemini API returned HTTP " + str(exc.code) + ": " + detail) from exc
    except URLError as exc:
        raise RuntimeError("Gemini API request failed: " + str(exc.reason)) from exc

    try:
        text = result["candidates"][0]["content"]["parts"][0]["text"]
        review = json.loads(text)
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Gemini returned an unexpected response") from exc
    has_parent = bool(payload.get("parent_wording"))
    shift = one_of(review.get("sourcing_shift"), SHIFTS, "n/a")
    return {
        "analysis": two_sentences(review.get("analysis", "")),
        "tone": one_of(review.get("tone"), TONES, "neutral"),
        "sourcing": one_of(review.get("sourcing"), SOURCING, "unclear"),
        "sourcing_basis": clip_words(review.get("sourcing_basis")),
        # A root wording has nothing to have shifted from, so the model does not
        # get to report one -- it will occasionally claim "unchanged" anyway.
        "sourcing_shift": shift if has_parent else "n/a",
        "confidence": unit(review.get("confidence")),
    }


class Handler(SimpleHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/api/analyze":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > MAX_BODY:
            self.send_json(413, {"error": "request is empty or too large"})
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            review = gemini_review(payload)
            self.send_json(200, review)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_json(400, {"error": "request must be valid JSON"})
        except RuntimeError as exc:
            self.send_json(503, {"error": str(exc)})

    def send_json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("dashboard: " + (format % args) + "\n")


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    directory = sys.argv[2] if len(sys.argv) > 2 else str(ROOT / ".dev")
    request_handler = partial(Handler, directory=directory)
    server = ThreadingHTTPServer(("127.0.0.1", port), request_handler)
    print("serving dashboard with Gemini review at http://localhost:" + str(port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()