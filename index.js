const http = require("http");
const https = require("https");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "";
const CACHE_TTL = parseInt(process.env.CACHE_TTL || "1800", 10); // 30min

const cache = new Map();

function fetchReddit(path) {
  return new Promise((resolve, reject) => {
    const url = `https://www.reddit.com${path}`;
    https.get(url, {
      headers: { "User-Agent": "reddit-proxy/1.0 (nxio.me feed aggregator)" },
      timeout: 10000,
    }, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Reddit returned ${res.statusCode}`));
        res.resume();
        return;
      }
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => resolve(data));
    }).on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  // Health check
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", cached: cache.size }));
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

  // Check cache
  const cached = cache.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL * 1000) {
    res.writeHead(200, {
      "Content-Type": "application/rss+xml; charset=utf-8",
      "X-Cache": "HIT",
    });
    res.end(cached.data);
    return;
  }

  try {
    const rss = await fetchReddit(`/r/${subreddit}/hot/.rss`);
    cache.set(cacheKey, { data: rss, ts: Date.now() });

    res.writeHead(200, {
      "Content-Type": "application/rss+xml; charset=utf-8",
      "X-Cache": "MISS",
    });
    res.end(rss);
  } catch (err) {
    res.writeHead(502, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: err.message }));
  }
});

// Cleanup stale cache entries every 10min
setInterval(() => {
  const now = Date.now();
  for (const [key, val] of cache) {
    if (now - val.ts > CACHE_TTL * 2000) cache.delete(key);
  }
}, 600000);

server.listen(PORT, () => {
  console.log(`reddit-proxy listening on :${PORT}`);
});
