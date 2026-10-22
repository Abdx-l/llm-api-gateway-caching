# LLM API Gateway with Caching

A local HTTP gateway for JSON chat requests. It validates client API keys, applies a per-key sliding-window limit, caches identical non-streaming payloads by SHA-256 and TTL, and exposes request, cache-hit, and latency metrics. An injectable upstream makes behavior testable without a model account.

```sh
CLIENT_KEYS=demo-key UPSTREAM_URL=https://your-provider.example/chat UPSTREAM_KEY=... python gateway.py
python -m unittest discover -s tests -v
```

POST `/chat` with `Authorization: Bearer demo-key` and a JSON body. GET `/metrics` returns Prometheus text. The default upstream forwards JSON with `urllib`; SSE responses bypass the cache but are buffered before delivery. Configure keys and upstream URL through environment variables. This is a small single-process gateway with in-memory state.

