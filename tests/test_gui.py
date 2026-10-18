"""Headless checks for GUI state and incremental real-trace ingestion."""
import json
import os
from pathlib import Path
import queue
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "Client"))
import gui


class ModelTests(unittest.TestCase):

    def test_snapshot_keeps_results_and_reports_only_validated_progress(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "STATUS", "monitor": {
            "policy": "rr", "ready_jobs": [4], "jobs": [
                {"job_id": 4, "state": "Active", "total_chunks": 5,
                 "completed_chunks": 2, "in_flight": 3}],
            "workers": [{"worker_id": 1, "state": "Busy", "job_id": 4, "chunk_id": 3}]}})
        self.assertEqual(model.progress(model.jobs[4]), (2, 5, 40))
        self.assertEqual(model.ready_jobs, [4])
        self.assertEqual(model.workers[1]["chunk_id"], 3)
        summary = {"valid_records": 7, "malformed_records": 3, "groups": {"/a": {"errors": 2, "4xx": 1, "5xx": 1}}}
        model.apply_response({"type": "COMPLETED", "job_id": 4, "summary": summary})
        model.apply_response({"type": "STATUS", "monitor": {"jobs": [{"job_id": 4, "state": "Completed"}]}})
        self.assertEqual(model.results[4], summary)
        self.assertEqual(model.progress(model.jobs[4]), (5, 5, 100))
        self.assertEqual(model.jobs[4]["valid_records"], 7)

    def test_timeline_uses_qpc_and_matches_out_of_order_events_by_identity(self):
        model = gui.MonitorModel()

        def event(kind, chunk, tick):
            model.apply_trace({"event": kind, "qpc_ticks": tick, "qpc_frequency": 1000,
                               "job_id": 9, "chunk_id": chunk, "worker_id": chunk, "component": "broker"})

        event("task_assigned", 1, 1000)
        event("task_assigned", 2, 1100)
        event("result_received", 2, 1500)
        event("compute_started", 1, 1200)  # Files may be tailed in a different order.
        event("compute_finished", 1, 1800)
        event("result_received", 1, 2000)
        event("result_received", 1, 9000)  # Do not overwrite the first result boundary.
        self.assertEqual(model.spans[(9, 1, 1)].assigned, 1)
        self.assertEqual(model.spans[(9, 1, 1)].returned, 2)
        self.assertEqual(model.spans[(9, 2, 2)].returned, 1.5)
        self.assertEqual(model.spans[(9, 1, 1)].compute_finished, 1.8)
        self.assertEqual(model.origin, 1)

    def test_missing_time_does_not_fabricate_a_timeline(self):
        model = gui.MonitorModel()
        model.apply_trace({"event": "task_assigned", "job_id": 1, "chunk_id": 1, "worker_id": 1})
        self.assertFalse(model.spans)
        self.assertIsNone(model.origin)

    def test_cancelled_job_is_not_counted_as_successful(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "CANCELLED", "job_id": 3})
        self.assertEqual(model.jobs[3]["state"], "Cancelled")
        self.assertNotIn(3, model.results)

    def test_failed_job_has_no_dispatch_eligible_chunks(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "PROGRESS", "job_id": 1, "job_state": "Active", "total_chunks": 5,
                              "completed_chunks": 1, "in_flight": 2, "pending_chunks": 2,
                              "monitor": {"ready_jobs": [1, 2]}})
        model.apply_response({"type": "FAILED", "job_id": 1, "reason": "Worker disconnected"})
        self.assertEqual(model.jobs[1]["state"], "Failed")
        self.assertEqual(model.jobs[1]["pending_chunks"], 0)
        self.assertEqual(model.jobs[1]["in_flight"], 2)  # Other assignments still need settlement.
        self.assertEqual(model.ready_jobs, [2])
        self.assertNotIn(1, model.results)

    def test_progress_and_cancel_ack_work_without_a_snapshot(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "ACCEPTED", "job_id": 8, "total_chunks": 10})
        model.apply_response({"type": "PROGRESS", "job_id": 8, "job_state": "Active", "total_chunks": 10,
                              "completed_chunks": 3, "in_flight": 2, "pending_chunks": 5,
                              "monitor": {"ready_jobs": [8, 9]}})
        self.assertEqual(model.progress(model.jobs[8]), (3, 10, 30))
        model.apply_response({"type": "CANCEL_ACK", "job_id": 8, "job_state": "Cancelling", "in_flight": 2})
        self.assertEqual(model.jobs[8]["state"], "Cancelling")
        self.assertEqual(model.jobs[8]["pending_chunks"], 0)
        self.assertEqual(model.ready_jobs, [9])

    def test_authoritative_snapshot_removes_evicted_history_and_results(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "COMPLETED", "job_id": 1, "summary": {"valid_records": 1}})
        model.apply_response({"type": "STATUS", "monitor": {"jobs": [{"job_id": 2, "state": "Queued"}]}})
        self.assertEqual(set(model.jobs), {2})
        self.assertFalse(model.results)

    def test_history_evicts_terminal_jobs_and_keeps_the_oldest_active_job(self):
        model = gui.MonitorModel()
        model.apply_response({"type": "ACCEPTED", "job_id": 1, "total_chunks": 10})
        model.apply_response({"type": "PROGRESS", "job_id": 1, "job_state": "Active"})
        for jid in range(2, 258):
            model.apply_response({"type": "COMPLETED", "job_id": jid, "summary": {"valid_records": 1}})
        self.assertEqual(len(model.jobs), 256)
        self.assertEqual(model.jobs[1]["state"], "Active")
        self.assertNotIn(2, model.jobs)
        self.assertNotIn(2, model.results)

    def test_worker_disconnect_settles_assignment_without_fabricating_compute_end(self):
        model = gui.MonitorModel()
        for event, tick in (("task_assigned", 1000), ("compute_started", 1200), ("worker_disconnected", 2000)):
            model.apply_trace({"event": event, "qpc_ticks": tick, "qpc_frequency": 1000,
                               "job_id": 1, "chunk_id": 2, "worker_id": 3})
        span = model.spans[(1, 2, 3)]
        self.assertEqual(span.disconnected, 2)
        self.assertIsNone(span.returned)
        self.assertIsNone(span.compute_finished)


class TailTests(unittest.TestCase):
    def test_large_backlog_is_read_in_batches_without_losing_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "broker.jsonl"
            count = 6000
            path.write_text("".join(json.dumps({"seq": i, "detail": "x" * 100}) + "\n"
                                    for i in range(count)), encoding="utf-8")
            emitted = []
            tail = gui.TraceTailer(temporary, emitted.append)
            tail.poll()
            self.assertTrue(0 < len(emitted) < count)
            for _ in range(10):
                tail.poll()
            self.assertEqual([item["payload"]["seq"] for item in emitted], list(range(count)))

    def test_partial_line_append_and_truncation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "worker2.jsonl"
            emitted = []
            tail = gui.TraceTailer(temporary, emitted.append)
            data = json.dumps({"event": "compute_started", "worker_id": 2}).encode()
            path.write_bytes(data[:10])
            tail.poll()
            self.assertFalse(emitted)
            with path.open("ab") as stream:
                stream.write(data[10:] + b"\n")
            tail.poll()
            tail.poll()
            self.assertEqual(len(emitted), 1)
            self.assertEqual(emitted[0]["payload"]["worker_id"], 2)
            path.write_bytes(b'{"event":"new"}\n')
            tail.poll()
            self.assertEqual(emitted[-1]["payload"]["event"], "new")

    def test_invalid_line_is_reported_and_next_record_survives(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "broker.jsonl"
            path.write_bytes(b'bad json\n{"event":"job_accepted"}\n')
            emitted = []
            gui.TraceTailer(temporary, emitted.append).poll()
            self.assertEqual([v["kind"] for v in emitted], ["log", "trace"])


@unittest.skipUnless(os.name == "nt" and os.environ.get("LGA_GUI_PROCESS_TESTS") == "1",
                     "Set LGA_GUI_PROCESS_TESTS=1 to run real Windows GUI-controller processes")
class ProcessTests(unittest.TestCase):
    def test_client_loss_allows_broker_to_settle_before_stopping_workers(self):
        controller = gui.ClusterController(queue.Queue())
        spawn = controller._spawn

        def delayed_worker(role, args, client=False):
            if role.startswith("worker"):
                args = [*args, "--delay-ms", "600"]
            return spawn(role, args, client=client)

        try:
            with patch.object(controller, "_spawn", side_effect=delayed_worker):
                controller.start(workers=3, policy="rr")
            self.assertFalse(controller.stopped.is_set())
            controller.send(f'submit route "{gui.ROOT / "data/small.csv"}"')
            deadline = time.monotonic() + 12
            while True:
                self.assertLess(time.monotonic(), deadline, "Worker never started; inspect session traces")
                try:
                    item = controller.events.get(timeout=0.1)
                except queue.Empty:
                    continue
                if item["kind"] == "error":
                    self.fail(item["text"])
                if item["kind"] == "trace" and item["payload"].get("event") == "test_delay_started":
                    break
            controller.processes["client"].terminate()
            controller.processes["client"].wait(timeout=5)
            controller.stop((1,))
            exits = {role: process.poll() for role, process in controller.processes.items()}
            settled = all(code == 0 for role, code in exits.items() if role != "client")
            self.assertTrue(settled, f"Client loss must allow chunk settlement: {exits}")
            events = [json.loads(line) for line in (controller.directory / "broker.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(event["event"] == "job_cancelled" for event in events))
            self.assertTrue(any(event["event"] == "broker_stopped" for event in events))
        finally:
            if not controller.stopped.is_set():
                controller.stop((1,))

    def test_controller_prepares_submits_collects_real_traces_and_stops_owned_children(self):
        events = queue.Queue()
        controller = gui.ClusterController(events)
        model = gui.MonitorModel()
        messages = []

        def pump_until(predicate, timeout=12):
            deadline = time.monotonic() + timeout
            while not predicate():
                if time.monotonic() >= deadline:
                    self.fail(f"GUI controller timeout; traces: {controller.directory}")
                try:
                    item = events.get(timeout=0.1)
                except queue.Empty:
                    continue
                if item["kind"] == "error":
                    self.fail(item["text"])
                if item["kind"] == "response":
                    messages.append(item["payload"])
                    model.apply_response(item["payload"])
                elif item["kind"] == "trace":
                    model.apply_trace(item["payload"], item.get("filename", ""))

        with tempfile.TemporaryDirectory(prefix="gui-input-", dir=gui.ROOT / "build") as temporary:
            sample = Path(temporary) / "gui sample.csv"
            shutil.copyfile(gui.ROOT / "data/small.csv", sample)
            try:
                controller.start(workers=3, policy="rr", capacity=32)
                self.assertFalse(controller.stopped.is_set(), "Cluster startup failed")
                controller.send("status")
                pump_until(lambda: len(model.workers) == 3)
                for mode in ("route", "time"):
                    previous = len(model.results)
                    controller.prepare_submit(sample, mode, 64)
                    pump_until(lambda: len(model.results) > previous)
                    completed = next(m for m in reversed(messages) if m.get("type") == "COMPLETED")
                    expected = json.loads((gui.ROOT / "data" / f"expected-{mode}.json").read_text())
                    self.assertEqual(completed["summary"], expected)
                    jid = completed["job_id"]
                    self.assertGreater(model.jobs[jid]["total_chunks"], 1)
                controller.send("status")
                pump_until(lambda: len(model.spans) == sum(j.get("total_chunks", 0) for j in model.jobs.values())
                           and len(model.spans) > 2
                           and all(s.compute_finished is not None and s.returned is not None for s in model.spans.values()))
            finally:
                controller.stop()
            self.assertTrue(controller.stopped.is_set())
            self.assertTrue(all(p.poll() == 0 for p in controller.processes.values()),
                            "GUI-owned processes must stop cleanly")
            self.assertTrue(controller.processes["client"].stdin.closed)


@unittest.skipUnless(os.name == "nt" and os.environ.get("LGA_GUI_PROCESS_TESTS") == "1", "Enable Windows GUI checks")
class NativeWindowTests(unittest.TestCase):
    def setUp(self):
        self.root = gui.tk.Tk()
        self.root.withdraw()
        self.window = gui.MonitorWindow(self.root)

    def tearDown(self):
        for callback in self.root.tk.call("after", "info"):
            self.root.after_cancel(callback)
        self.root.destroy()


    def test_result_view_refreshes_when_selected_job_completes(self):
        window = self.window
        window.model.apply_response(dict(type="ACCEPTED", job_id=1, total_chunks=1))
        window._render()
        window.jobs.selection_set("1")
        window._render()
        self.assertFalse(window.result_tree.get_children())
        window.model.apply_response(dict(type="COMPLETED", job_id=1, summary={
            "valid_records": 2, "malformed_records": 0, "groups": {"/a": {"errors": 2, "4xx": 1, "5xx": 1}}}))
        window._render()
        window._render()  # An unchanged repaint must preserve the displayed result.
        rows = [window.result_tree.item(item, "values") for item in window.result_tree.get_children()]
        self.assertEqual(rows, [("/a", "2", "1", "1")])
        window.model.apply_response(dict(type="STATUS", monitor={"jobs": []}))
        window._render()
        self.assertFalse(window.result_tree.get_children())


if __name__ == "__main__":
    unittest.main()
