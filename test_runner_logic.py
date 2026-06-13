#!/usr/bin/env python3
import unittest

import runner


class RunnerLogicTests(unittest.TestCase):
    def test_consensus_requires_two_point_edge(self):
        provider_results = [
            {"provider": "gemini", "signal": "SHORT", "sources": []},
            {"provider": "openai", "signal": "NO_TRADE", "sources": []},
            {"provider": "claude", "signal": "NO_TRADE", "sources": []},
            {"provider": "perplexity", "signal": "NO_TRADE", "sources": []},
            {"provider": "grok", "signal": "NO_TRADE", "sources": []},
        ]
        consensus = runner.compute_consensus(provider_results, {"value": "", "classification": ""})
        self.assertEqual(consensus["score"], 1)
        self.assertEqual(consensus["signal"], "NO_TRADE")

    def test_consensus_short_with_two_point_edge(self):
        provider_results = [
            {"provider": "gemini", "signal": "SHORT", "sources": []},
            {"provider": "openai", "signal": "SHORT", "sources": []},
            {"provider": "claude", "signal": "NO_TRADE", "sources": []},
            {"provider": "perplexity", "signal": "NO_TRADE", "sources": []},
            {"provider": "grok", "signal": "LONG", "sources": []},
        ]
        consensus = runner.compute_consensus(provider_results, {"value": "", "classification": ""})
        self.assertEqual(consensus["score"], 1)
        self.assertEqual(consensus["signal"], "NO_TRADE")

        provider_results[-1]["signal"] = "NO_TRADE"
        consensus = runner.compute_consensus(provider_results, {"value": "", "classification": ""})
        self.assertEqual(consensus["score"], 2)
        self.assertEqual(consensus["signal"], "SHORT")

    def test_signal_return_pct_applies_round_trip_fees(self):
        self.assertAlmostEqual(runner.signal_return_pct("LONG", 100.0, 101.0), 0.97, places=6)
        self.assertAlmostEqual(runner.signal_return_pct("SHORT", 100.0, 99.0), 0.97, places=6)
        self.assertEqual(runner.signal_return_pct("NO_TRADE", 100.0, 101.0), 0.0)

    def test_format_commit_ts_formats_utc(self):
        self.assertEqual(
            runner.format_commit_ts("2026-06-13T12:34:56+00:00"),
            "2026-06-13 12:34 UTC",
        )


if __name__ == "__main__":
    unittest.main()
