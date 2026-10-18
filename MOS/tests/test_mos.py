from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from run import run_experiment
from validate_trace import validate


class CourseMosTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="mos-integration-")
        cls.output = Path(cls.directory.name)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_actual_simulator_matrix(self):
        # K=1 exercises the source one-slot boundary; K=3 repeatedly wraps.
        # Multiple readers exercise the same full/mutex waiting queues.
        for capacity in (1, 3):
            for consumers in (1, 2):
                for timer in (80, 400, 1600):
                    with self.subTest(capacity=capacity, consumers=consumers, timer=timer):
                        output = self.output / f"k{capacity}-c{consumers}-t{timer}"
                        report = run_experiment(capacity, 12, consumers, timer, output)
                        self.assertTrue(report["passed"])
                        self.assertEqual(report["produced"], 12)
                        self.assertEqual(report["consumed"], 12)
                        self.assertGreater(report["event_counts"].get("timer_interrupt", 0), 0)
                        baseline = (ROOT / "course" / "mos-rz2.txt").read_text()
                        generated = (output / "buffer.txt").read_text()
                        start, end = "#noframe\nint0()", "\nboot()"
                        self.assertEqual(baseline.split(start)[1].split(end)[0], generated.split(start)[1].split(end)[0])

    def test_original_scheduler_regression_is_detected(self):
        output = self.output / "original-scheduler"
        with self.assertRaises(ValueError):
            run_experiment(4, 24, 1, 400, output, max_clocks=100_000, original_scheduler=True)
        failure = json.loads((output / "validation.json").read_text())
        self.assertFalse(failure["passed"])

    def test_validator_rejects_corrupted_trace(self):
        output = self.output / "corruption"
        run_experiment(3, 12, 2, 400, output)
        records = [json.loads(line) for line in (output / "trace.jsonl").read_text().splitlines()]
        get = next(record for record in records if record["event"] == "get")
        get["value"] += 1
        corrupted = output / "corrupted.jsonl"
        corrupted.write_text("\n".join(json.dumps(record) for record in records))
        config = json.loads((output / "manifest.json").read_text())["config"]
        with self.assertRaisesRegex(ValueError, "FIFO buffer contents"):
            validate(corrupted, config)


if __name__ == "__main__":
    unittest.main(verbosity=2)
