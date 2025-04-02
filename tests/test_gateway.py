import unittest
from gateway import Gateway


class GatewayTests(unittest.TestCase):
    def test_cache_ttl_and_rate_limit(self):
        now = [0]
        calls = []
        def upstream(body):
            calls.append(body)
            return {"reply": "ok"}, "application/json"
        g = Gateway(["k"], upstream, lambda: now[0], limit=3, ttl=5)
        self.assertEqual(g.handle("bad", {"prompt":"hi"})[0], 401)
        self.assertEqual(g.handle("k", {"prompt":"hi"})[0], 200)
        self.assertEqual(g.handle("k", {"prompt":"hi"})[0], 200)
        self.assertEqual(g.cache_hits, 1)
        self.assertEqual(g.handle("k", {"prompt":"hi"})[0], 200)
        self.assertEqual(g.handle("k", {"prompt":"hi"})[0], 429)
        now[0] = 61
        self.assertEqual(g.handle("k", {"prompt":"hi"})[0], 200)
        self.assertEqual(len(calls), 2)

    def test_streaming_bypasses_cache(self):
        g = Gateway(["k"], lambda body: ("data: hello\n\n", "text/event-stream"))
        self.assertEqual(g.handle("k", {"stream":True})[2], "text/event-stream")
        self.assertEqual(g.cache_hits, 0)
        self.assertIn("gateway_requests_total 1", g.metrics())

