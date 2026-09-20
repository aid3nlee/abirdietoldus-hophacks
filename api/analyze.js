// Production twin of the /api/analyze endpoint in gemini_server.py.
//
// The local dev server is Python because the rest of the pipeline is; this is
// JavaScript because a Python function on Vercel would install the root
// requirements.txt -- duckdb, pyarrow, pandas, scipy -- into the lambda and
// blow the bundle limit for the sake of one HTTPS POST. The prompt, the
// two-sentence cap and the error codes below are kept byte-for-byte in step
// with gemini_server.py; change one and change the other, or the deployed
// panel stops matching the one the demo was rehearsed against.

const MAX_BODY = 32 * 1024;
const MODEL = process.env.GEMINI_MODEL || "gemini-3.6-flash";

const PROMPT = `You are the analyst in a narrative phylogeny tool. The pipeline traces how a phrasing spreads on Twitter and mutates as people reword it while copying. You are reading one wording from one lineage, along with the wording it descends from and the tokens that were added or dropped in between.

Say what this wording claims and what the reword changed about it. The supplied figures describe spread and reception, never intent: coordination, volume and hostile replies tell you how a phrasing travelled and how it landed, not that anyone posted it in bad faith. Do not invent facts, sources, intent or context, and do not fact-check the underlying claim -- you have no way to verify it. Keep what the text literally says separate from what a reader might infer.

Return JSON only with exactly these keys:
{
  "analysis": "at most two sentences",
  "confidence": 0.0
}

\`analysis\` is read off a dashboard, so it is hard-capped at TWO SENTENCES. Sentence one: what the wording says, and what the mutation from its parent changed about its meaning or force. Sentence two: what the spread and reception figures show about how it travelled. No preamble, no bullet points, no quoting the wording back verbatim.

Use a confidence from 0 to 1 for your reading of the text, not for the truth of the underlying claim.`;

// Hard-cap the analysis at two sentences. The prompt asks for two and usually
// gets two, but the panel it renders into is a fixed slot next to the tree --
// a model having a verbose day must not be able to push the rest of the
// inspector off screen. Splits on sentence-ending punctuation followed by a
// space, so a decimal or an abbreviation mid-sentence does not count as a break.
function twoSentences(text) {
  return String(text == null ? "" : text)
    .trim()
    .split(/(?<=[.!?])\s+/)
    .slice(0, 2)
    .filter(Boolean)
    .join(" ");
}

async function geminiReview(payload) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error("GEMINI_API_KEY is not set");

  const url =
    "https://generativelanguage.googleapis.com/v1beta/models/" +
    encodeURIComponent(MODEL) +
    ":generateContent?key=" +
    encodeURIComponent(apiKey);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 45000);
  let response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              { text: PROMPT },
              { text: "\nSUPPLIED MATERIAL:\n" + JSON.stringify(payload) },
            ],
          },
        ],
        generationConfig: { temperature: 0.2, responseMimeType: "application/json" },
      }),
    });
  } catch (error) {
    throw new Error("Gemini API request failed: " + (error.name === "AbortError" ? "timed out" : error.message));
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500);
    throw new Error("Gemini API returned HTTP " + response.status + ": " + detail);
  }

  let review;
  try {
    const result = await response.json();
    review = JSON.parse(result.candidates[0].content.parts[0].text);
  } catch (error) {
    throw new Error("Gemini returned an unexpected response");
  }
  review.analysis = twoSentences(review.analysis);
  return review;
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  if (req.method !== "POST") {
    res.status(405).json({ error: "POST only" });
    return;
  }
  const length = Number(req.headers["content-length"] || 0);
  if (length > MAX_BODY) {
    res.status(413).json({ error: "request is empty or too large" });
    return;
  }
  // Vercel parses application/json for us; a body that failed to parse arrives
  // as a string or undefined rather than throwing, so check rather than catch.
  const payload = req.body;
  if (!payload || typeof payload !== "object") {
    res.status(400).json({ error: "request must be valid JSON" });
    return;
  }
  try {
    res.status(200).json(await geminiReview(payload));
  } catch (error) {
    res.status(503).json({ error: error.message });
  }
};
