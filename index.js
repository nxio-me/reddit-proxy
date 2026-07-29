// reddit-proxy — RSS/Atom proxy for the nxio.me pipeline.
//
// Why this exists: Cloudflare Workers IPs are blocked by Reddit, so the
// pipeline fetches subreddit feeds through this tiny service instead.
// Since 2026-06 Reddit also blocks/rate-limits this server's datacenter IP
// for anonymous RSS (six weeks of upstream 403/429 -> 502 for every sub).
//
// Modes (checked per request, no restart needed beyond env changes):
//   1. OAuth (preferred): set REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET
//      (script app from https://www.reddit.com/prefs/apps). Uses the
//      official API via oauth.reddit.com (works from datacenter IPs,
//      100 QPM free tier) and converts JSON -> Atom so the pipeline's
//      feed parser keeps working unchanged.
//   2. Anonymous fallback: original www.reddit.com RSS fetch, hardened
//      with one jittered retry and serve-stale-on-error from cache.
//
// Contract with the pipeline stays identical: GET /r/{sub}/hot.rss with
// X-API-Key, response is an Atom feed (Reddit's native RSS is Atom too).

const http = require("http");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "";
const CACHE_TTL = parseInt(process.env.CACHE_TTL || "1800", 10); // fresh: 30min
const STALE_TTL = parseInt(process.env.STALE_TTL || "86400", 10); // stale fallback: 24h
const OAUTH_ID = process.env.REDDIT_CLIENT_ID || "";
const OAUTH_SECRET = process.env.REDDIT_CLIENT_SECRET || "";
const UA = "nxio-research/1.0 (feed aggregator; contact https://nxio.me)";

const cache = new Map(); // sub -> { data, ts }
let oauthToken = null; // { token, expiresAt }
let lastUpstreamError = null; // { ts, sub, message } for /health diagnostics

function xmlEscape(s) {
  return String(s)
    // strip XML-1.0-illegal control chars and lone surrogates first
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\uFFFE\uFFFF]/g, "")
    .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// slice on codepoint boundaries so we never split a surrogate pair
function safeSlice(s, n) {
  return Array.from(String(s)).slice(0, n).join("");
}

async function getOauthToken() {
  if (oauthToken && Date.now() < oauthToken.expiresAt) return oauthToken.token;
  const res = await fetch("https://www.reddit.com/api/v1/access_token", {
    method: "POST",
    headers: {
      Authorization: "Basic " + Buffer.from(`${OAUTH_ID}:${OAUTH_SECRET}`).toString("base64"),
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": UA,
    },
    body: "grant_type=client_credentials",
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`Reddit OAuth token request returned ${res.status}`);
  const body = await res.json();
  if (!body.access_token) throw new Error("Reddit OAuth response had no access_token");
  oauthToken = {
    token: body.access_token,
    expiresAt: Date.now() + Math.max((body.expires_in || 3600) - 60, 60) * 1000,
  };
  return oauthToken.token;
}

// Official API path: JSON listing -> minimal Atom feed (same shape the
// pipeline already parses from Reddit's native RSS).
async function fetchViaOauth(subreddit) {
  const token = await getOauthToken();
  const res = await fetch(
    `https://oauth.reddit.com/r/${subreddit}/hot.json?limit=25&raw_json=1`,
    {
      headers: { Authorization: `Bearer ${token}`, "User-Agent": UA },
      signal: AbortSignal.timeout(10000),
    },
  );
  if (res.status === 401) {
    oauthToken = null; // token invalidated server-side; next call re-auths
    throw new Error("Reddit OAuth returned 401 (token refreshed for next attempt)");
  }
  if (!res.ok) throw new Error(`Reddit OAuth API returned ${res.status}`);
  const json = await res.json();
  const posts = (json.data && json.data.children) || [];
  const entries = posts
    .filter((p) => p && p.kind === "t3" && p.data)
    .map((p) => {
      const d = p.data;
      const link = `https://www.reddit.com${d.permalink}`;
      const published = new Date((d.created_utc || 0) * 1000).toISOString();
      const content = d.selftext
        ? xmlEscape(safeSlice(d.selftext, 4000))
        : xmlEscape(d.url || link);
      return [
        "  <entry>",
        `    <title>${xmlEscape(d.title || "")}</title>`,
        `    <link href="${xmlEscape(link)}"/>`,
        `    <id>${xmlEscape(d.name || link)}</id>`,
        `    <updated>${published}</updated>`,
        `    <published>${published}</published>`,
        `    <author><name>/u/${xmlEscape(d.author || "unknown")}</name></author>`,
        `    <content type="html">${content}</content>`,
        "  </entry>",
      ].join("\n");
    });
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<feed xmlns="http://www.w3.org/2005/Atom">',
    `  <title>/r/${xmlEscape(subreddit)} — hot</title>`,
    `  <id>https://www.reddit.com/r/${xmlEscape(subreddit)}/hot</id>`,
    `  <updated>${new Date().toISOString()}</updated>`,
    entries.join("\n"),
    "</feed>",
  ].join("\n");
}

// Anonymous fallback: Reddit's native Atom-RSS. Blocked for this server's
// IP since 2026-06, but kept as fallback and for local/residential use.
async function fetchAnonymous(subreddit) {
  const res = await fetch(`https://www.reddit.com/r/${subreddit}/hot/.rss`, {
    headers: { "User-Agent": UA },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`Reddit returned ${res.status}`);
  return res.text();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchFeed(subreddit) {
  const attempt = () => (OAUTH_ID && OAUTH_SECRET ? fetchViaOauth(subreddit) : fetchAnonymous(subreddit));
  try {
    return await attempt();
  } catch (firstErr) {
    await sleep(1500 + Math.floor(Math.random() * 2000)); // jittered single retry
    try {
      return await attempt();
    } catch (retryErr) {
      retryErr.firstMessage = firstErr.message;
      throw retryErr;
    }
  }
}

const server = http.createServer(async (req, res) => {
  // Health check (root + /health both return 200 for Coolify)
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        mode: OAUTH_ID && OAUTH_SECRET ? "oauth" : "anonymous",
        cached: cache.size,
        has_upstream_error: Boolean(lastUpstreamError),
        last_error_ts: lastUpstreamError ? lastUpstreamError.ts : null,
      }),
    );
    return;
  }

  // Auth check
  if (API_KEY && req.headers["x-api-key"] !== API_KEY) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Unauthorized" }));
    return;
  }

  // Route: /r/{subreddit}/hot.rss
  const match = req.url.match(/^\/r\/([a-zA-Z0-9_]+)\/hot\.rss$/);
  if (!match) {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Use /r/{subreddit}/hot.rss" }));
    return;
  }

  const subreddit = match[1];
  const cacheKey = subreddit.toLowerCase();
  const cached = cache.get(cacheKey);

  // Fresh cache hit
  if (cached && Date.now() - cached.ts < CACHE_TTL * 1000) {
    res.writeHead(200, {
      "Content-Type": "application/rss+xml; charset=utf-8",
      "X-Cache": "HIT",
    });
    res.end(cached.data);
    return;
  }

  try {
    const feed = await fetchFeed(subreddit);
    cache.set(cacheKey, { data: feed, ts: Date.now() });
    res.writeHead(200, {
      "Content-Type": "application/rss+xml; charset=utf-8",
      "X-Cache": "MISS",
    });
    res.end(feed);
  } catch (err) {
    lastUpstreamError = { ts: new Date().toISOString(), sub: subreddit, message: err.message };
    // Serve-stale: an expired-but-recent cache entry beats a 502. Bridges
    // intermittent upstream blocks without the pipeline pausing the source.
    if (cached && Date.now() - cached.ts < STALE_TTL * 1000) {
      res.writeHead(200, {
        "Content-Type": "application/rss+xml; charset=utf-8",
        "X-Cache": "STALE",
        "X-Upstream-Error": String(err.message).replace(/[\r\n]/g, " ").slice(0, 120),
      });
      res.end(cached.data);
      return;
    }
    res.writeHead(502, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: err.message, first_attempt: err.firstMessage }));
  }
});

// Cleanup cache entries past stale window every 10min
setInterval(() => {
  const now = Date.now();
  for (const [key, val] of cache) {
    if (now - val.ts > STALE_TTL * 1000) cache.delete(key);
  }
}, 600000).unref();

server.listen(PORT, () => {
  console.log(`reddit-proxy listening on :${PORT} (mode: ${OAUTH_ID && OAUTH_SECRET ? "oauth" : "anonymous"})`);
});
