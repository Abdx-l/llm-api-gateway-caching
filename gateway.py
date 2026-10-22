"""Authenticated model API proxy with in-memory cache and sliding limits."""
import hashlib
import json
import os
import time
import urllib.request
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Gateway:
    def __init__(self, keys, upstream, clock=time.monotonic, limit=20, window=60, ttl=120):
        self.keys = set(keys)
        self.upstream = upstream
        self.clock, self.limit, self.window, self.ttl = clock, limit, window, ttl
        self.hits = defaultdict(deque)
        self.cache = {}
        self.requests = self.cache_hits = 0
        self.latencies = []

    def handle(self, key, body):
        start = self.clock()
        self.requests += 1
        if key not in self.keys:
            return 401, {"error": "invalid key"}, "application/json"
        times = self.hits[key]
        while times and times[0] <= start - self.window:
            times.popleft()
        if len(times) >= self.limit:
            return 429, {"error": "rate limit"}, "application/json"
        times.append(start)
        if not isinstance(body, dict):
            return 400, {"error": "JSON object required"}, "application/json"
        streaming = body.get("stream") is True
        digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        cached = self.cache.get(digest)
        if not streaming and cached and cached[0] > start:
            self.cache_hits += 1
            return 200, cached[1], "application/json"
        try:
            content, media = self.upstream(body)
        except Exception as exc:
            return 502, {"error": "upstream unavailable"}, "application/json"
        self.latencies.append(self.clock() - start)
        if not streaming and media == "application/json":
            self.cache[digest] = (self.clock() + self.ttl, content)
        return 200, content, media

    def metrics(self):
        thresholds = (0.1, 0.5, 1, 5)
        lines = [f"gateway_requests_total {self.requests}", f"gateway_cache_hits_total {self.cache_hits}",
                 f"gateway_latency_seconds_sum {sum(self.latencies):.6f}", f"gateway_latency_seconds_count {len(self.latencies)}"]
        lines += [f'gateway_latency_seconds_bucket{{le="{limit}"}} {sum(v <= limit for v in self.latencies)}' for limit in thresholds]
        lines.append(f'gateway_latency_seconds_bucket{{le="+Inf"}} {len(self.latencies)}')
        return "\n".join(lines) + "\n"


def remote(body):
    url = os.environ["UPSTREAM_URL"]
    raw = json.dumps(body).encode()
    req = urllib.request.Request(url, data=raw, headers={"Content-Type":"application/json", "Authorization":"Bearer " + os.environ.get("UPSTREAM_KEY", "")}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as response:
        media = response.headers.get_content_type()
        data = response.read(5_000_000)
    return (data.decode("utf-8") if media == "text/event-stream" else json.loads(data)), media


def serve(gateway, port=8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            raw = gateway.metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            if self.path != "/chat":
                self.send_error(404)
                return
            size = int(self.headers.get("Content-Length", 0))
            if size <= 0 or size > 1_000_000:
                self.send_error(413)
                return
            try:
                body = json.loads(self.rfile.read(size))
            except (ValueError, UnicodeDecodeError):
                self.send_error(400)
                return
            authorization = self.headers.get("Authorization", "")
            key = authorization[7:] if authorization.startswith("Bearer ") else ""
            status, payload, media = gateway.handle(key, body)
            raw = payload.encode() if media == "text/event-stream" else json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", media)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    keys = [key for key in os.environ.get("CLIENT_KEYS", "").split(",") if key]
    if not keys or not os.environ.get("UPSTREAM_URL"):
        raise SystemExit("Set CLIENT_KEYS and UPSTREAM_URL")
    serve(Gateway(keys, remote), int(os.environ.get("PORT", "8080")))

