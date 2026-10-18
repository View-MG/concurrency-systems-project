"""Measurement boundaries must include the time spent delivering acceptance."""
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import experiments
from experiments import fairness
from support import traces


class MeasurementTests(unittest.TestCase):
    def test_equal_slowdowns_are_balanced_but_can_both_be_slow(self):
        self.assertEqual(fairness([1, 1]), 1)
        self.assertEqual(fairness([100, 100]), 1)

    def test_unequal_slowdowns_use_inverse_service(self):
        self.assertAlmostEqual(fairness([1, 4]), 25 / 34)
        self.assertAlmostEqual(fairness([4, 1]), 25 / 34)
        with self.assertRaises(AssertionError):
            fairness([0, 1])

    def test_live_trace_waits_for_a_complete_json_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.jsonl"
            path.write_bytes(b'{"event":"first"}\n{"event":')
            self.assertEqual(traces(Path(directory), "broker"), [{"event": "first"}])
            with path.open("ab") as stream:
                stream.write(b'"second"}\n')
            self.assertEqual([e["event"] for e in traces(Path(directory), "broker")], ["first", "second"])

    def test_delayed_acceptance_reply_counts_towards_admission_latency(self):
        def event(kind, tick, detail=""):
            return dict(event=kind, qpc_ticks=tick, qpc_frequency=1000,
                        request_id=1, job_id=1, chunk_id=1, worker_id=1, detail=detail)

        traces = {
            "client": [event("command_started", 1000, "SUBMIT"),
                       event("response_received", 1500, "ACCEPTED"),
                       event("response_received", 1600, "COMPLETED")],
            "broker": [event("job_accepted", 1100), event("task_assigned", 1200),
                       event("result_received", 1300), event("job_completed", 1400)],
        }
        cluster = SimpleNamespace(directory=None, processes={"client": None, "broker": None, "worker": None})
        jobs = {"A": dict(job=1, request=1, total_chunks=1, mode="route", summary_hash="fixture",
                          summary=dict(valid_records=7, malformed_records=0), compute_us=100000)}
        before, after = dict(ipc_messages=0, ipc_bytes=0), dict(ipc_messages=10, ipc_bytes=1000)
        with patch.object(experiments, "traces", side_effect=lambda directory, role: traces[role]):
            measured, _ = experiments.measures(cluster, jobs, before, after)
        self.assertEqual(measured["A"]["admission_ms"], 500)
        self.assertEqual(measured["A"]["queue_ms"], 100)
        self.assertEqual(measured["A"]["turnaround_ms"], 300)
        self.assertEqual(measured["A"]["client_response_ms"], 600)


if __name__ == "__main__":
    unittest.main()
