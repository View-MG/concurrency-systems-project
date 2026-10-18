"""Real Windows process tests: transport, concurrency and bounded resources."""
import hashlib
import itertools
import json
import math
import os
import struct
import subprocess
import sys
import time

from support import BUILD, ROOT, Cluster, RawWorker, check, traces, prepare, oracle, wait_event, process_usage
sys.path.insert(0, str(ROOT / "scripts"))
from generate import generate


def input_limits(expected):
    for size in (8192, 8193):
        for ending in ("\n", "\r\n", ""):
            with Cluster("stdin-limit", delay=100) as c:
                c.send('submit route "data/small.csv"')
                c.receive("ACCEPTED", 1)
                # Write exact bytes: TextIOWrapper would translate LF on Windows.
                client = c.processes["client"]
                client.stdin.buffer.write(("status" + " " * (size - 6) + ending).encode())
                client.stdin.close()
                if size == 8192:
                    c.receive("STATUS", 2)
                check(c.receive("COMPLETED", 1)["summary"] == expected, "Input failure must drain admitted work")
                check(all(p.wait(timeout=5) == 0 for p in c.processes.values()), "Input EOF/failure did not settle")
                c.reader.join(timeout=2)
                log = (c.directory / "client.console.txt").read_text(encoding="utf-8")
                check(("input error: Command exceeds 8192 bytes" in log) == (size > 8192), "Command limit must report its error")
                sent = [e for e in traces(c.directory, "client") if e["event"] == "command_sent"]
                check(len(sent) == (2 if size == 8192 else 1), "Oversized command reached Broker")
    # Exceptional test exits must close the owned cluster too.
    try:
        with Cluster("cleanup-on-error") as c:
            raise ValueError("deliberate test failure")
    except ValueError:
        check(all(p.poll() is not None for p in c.processes.values()), "Context exit leaked a child process")
        check(c.processes["client"].stdin.closed, "Context exit leaked Client stdin")


def transport():
    subprocess.run([str(BUILD / "common_tests.exe")], cwd=ROOT, check=True)
    expected = {mode: json.loads((ROOT / "data" / f"expected-{mode}.json").read_text()) for mode in ("route", "time")}
    for mode in ("route", "time"):
        reference = subprocess.check_output([str(BUILD / "reference.exe"), "--file", "data/small.csv", "--analysis", mode], cwd=ROOT, text=True)
        check(json.loads(reference) == expected[mode], f"Hand-counted {mode} reference mismatch")

    with Cluster("roundtrip", delay=1200) as c:
        c.send('submit route "data/small.csv"')
        accepted = c.receive("ACCEPTED", 1)
        job = accepted["job_id"]
        wait_event(c, "worker", "test_delay_started")
        c.send(f"status {job}")
        status = c.receive("STATUS", 2)
        check(status["job_state"] == "Active" and status["worker_state"] == "Busy", "Status while Worker computes")
        check(not any(e["event"] == "job_completed" for e in traces(c.directory, "broker")), "Status did not precede completion")
        c.send('submit time "data/small.csv"')
        accepted2 = c.receive("ACCEPTED", 3)
        check(accepted2["job_id"] != job, "Jobs need distinct identities")
        c.send("shutdown")
        check("outstanding" in c.receive("REJECTED", 4)["reason"], "Shutdown must reject while busy")
        check(c.receive("COMPLETED", 1)["summary"] == expected["route"], "Route end-to-end mismatch")
        check(c.receive("COMPLETED", 3)["summary"] == expected["time"], "Time end-to-end mismatch")
        c.send("status 999")
        c.receive("REJECTED", 5)
        c.send('submit route "data/does-not-exist.csv"')
        c.receive("REJECTED", 6)
        c.shutdown(7)
        broker = traces(c.directory, "broker")
        for jid in (job, accepted2["job_id"]):
            events = [e["event"] for e in broker if e["job_id"] == jid]
            ordered = ["job_accepted", "task_assigned", "result_received", "job_completed"]
            indexes = [events.index(event) for event in ordered]
            check(indexes == sorted(indexes), "Trace lifecycle order")

    inputs = BUILD / "test-input"
    inputs.mkdir(exist_ok=True)
    large = inputs / "many-routes.csv"
    with large.open("w", encoding="utf-8", newline="") as f:
        f.write("timestamp,route,status\n")
        for i in range(1000):
            f.write(f"2026-10-11T10:00:00Z,/route-{i:04d},500\n")
    unicode_file = inputs / "ตัวอย่าง log.csv"
    unicode_file.write_bytes((ROOT / "data" / "small.csv").read_bytes())
    bad = inputs / "bad-header.csv"
    bad.write_text("incorrect,header\n", encoding="utf-8")
    with Cluster("fragmented-result") as c:
        c.send(f'submit route "{large}"')
        c.receive("ACCEPTED", 1)
        summary = c.receive("COMPLETED", 1)["summary"]
        check(summary["valid_records"] == 1000 and summary["malformed_records"] == 0 and len(summary["groups"]) == 1000, "Large multi-read result")
        check(all(v == {"errors": 1, "4xx": 0, "5xx": 1} for v in summary["groups"].values()), "Large result key counts")
        c.send(f'submit route "{bad}"')
        c.receive("ACCEPTED", 2)
        c.receive("FAILED", 2)
        c.send('submit time "data/small.csv"')
        c.receive("ACCEPTED", 3)
        check(c.receive("COMPLETED", 3)["summary"] == expected["time"], "Worker reused after analyzer failure")
        c.send(f'submit route "{unicode_file}"')
        c.receive("ACCEPTED", 4)
        check(c.receive("COMPLETED", 4)["summary"] == expected["route"], "UTF-8 path with space/backslashes")
        c.shutdown(5)

    with Cluster("worker-loss", delay=1500) as c:
        c.send('submit route "data/small.csv"')
        c.receive("ACCEPTED", 1)
        wait_event(c, "worker", "test_delay_started")
        c.processes["worker"].terminate()
        c.processes["worker"].wait(timeout=5)
        c.receive("FAILED", 1)
        c.send('submit route "data/small.csv"')
        c.receive("REJECTED", 2)
        c.send("shutdown")
        c.receive("SHUTDOWN_ACK", 3)
        check(c.processes["broker"].wait(timeout=5) == 0, "Broker exit after Worker loss")
        check(c.processes["client"].wait(timeout=5) == 0, "Client exit after Worker loss")

    c = Cluster("wrong-assignment", real_worker=False)
    peer = None
    try:
        peer = RawWorker(c.session)
        c.send('submit route "data/small.csv"')
        c.receive("ACCEPTED", 1)
        request, job, chunk, worker = peer.task()
        # All fields parse, but the chunk ID is not the assigned one.
        payload = struct.pack("<QQQQB", request, job, chunk + 1, worker, 1)
        payload += struct.pack("<IQQQI", 0, 0, 0, 0, 0)
        peer.send(6, payload)
        failed = c.receive("FAILED", 1)
        check("assignment" in failed["reason"], "Wrong assignment must not complete a Job")
        peer.close()
        c.shutdown(2)
    finally:
        if peer:
            peer.close()
        c.close()

    with Cluster("stdin-eof") as c:
        c.send('submit route "data/small.csv"')
        c.processes["client"].stdin.close()
        c.receive("ACCEPTED", 1)
        check(c.receive("COMPLETED", 1)["summary"] == expected["route"], "EOF must drain submitted job")
        for role, process in c.processes.items():
            check(process.wait(timeout=5) == 0, f"{role} exit after Client EOF")

    input_limits(expected["route"])
    print("PASS: real three-process IPC, hand-counted route/time results, status during work, request correlation, >4 KiB result, rejection/failure, reassignment and shutdown")


def result_payload(identity, valid=0):
    return struct.pack("<QQQQB", *identity, 1) + struct.pack("<IQQQI", 0, 0, valid, 0, 0)


def concurrency():
    inputs = BUILD / "concurrency-input"
    inputs.mkdir(exist_ok=True)
    source = inputs / "random.csv"
    metadata = generate(source, records=1234, seed=773, malformed_ratio=0.08)
    repeated = inputs / "repeat.csv"
    generate(repeated, records=1234, seed=773, malformed_ratio=0.08)
    check(hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(repeated.read_bytes()).digest(), "Generator must reproduce bytes")
    check(metadata["realized"]["valid_records"] + metadata["realized"]["malformed_records"] == 1234, "Generator accounting")
    prepare(source, 1300)
    for count in (1, 2, 3):
        with Cluster(f"multi-{count}", workers=count, worker_delays={1: 70, 2: 2, 3: 10}) as c:
            for request, mode in enumerate(("route", "time"), 1):
                c.send(f'submit {mode} "{source}"')
                accepted = c.receive("ACCEPTED", request)
                check(accepted["total_chunks"] > 3, "Prepared multi-chunk index must be used")
            for request, mode in enumerate(("route", "time"), 1):
                result = c.receive("COMPLETED", request, timeout=20)
                expected = oracle(source, mode)
                reference = json.loads(subprocess.check_output([str(BUILD / "reference.exe"), "--file", str(source), "--analysis", mode], cwd=ROOT, text=True))
                check(result["summary"] == reference == expected, f"{count} Workers/{mode}: independent reference agreement")
            c.send("status")
            monitor = c.receive("STATUS", 3)["monitor"]
            check(len(monitor["workers"]) == count and all(j["completed_chunks"] == j["total_chunks"] for j in monitor["jobs"]), "Snapshot tracks real progress")
            c.shutdown(4)
            broker = traces(c.directory, "broker")
            check(all(e["job_state"] is None for e in broker if e["event"] == "task_assigned"), "Assignments must not report an unknown Broker state as Queued")
            check(all(e["job_state"] == "Completed" for e in traces(c.directory, "client")
                      if e["event"] == "response_received" and e["detail"] == "COMPLETED"), "Terminal trace state must agree with its reply")
            for job in (1, 2):
                chunks = [e["chunk_id"] for e in broker if e["event"] == "result_received" and e["job_id"] == job]
                check(len(chunks) == len(set(chunks)), "No duplicate merge")
                if count > 1:
                    check(chunks != sorted(chunks), "Controlled delay must cause out-of-order results")

    # Admit all jobs before the first Worker connects to make dispatch order exact.
    paths = []
    for label, records in (("A", 3), ("B", 2), ("C", 1)):
        path = inputs / f"{label}.csv"
        path.write_text("timestamp,route,status\n" + "2026-10-11T00:00:00Z,/a,500\n" * records, encoding="utf-8")
        prepare(path, 1)
        paths.append(path)
    for policy, expected in (("fifo", [1, 1, 1, 2, 2, 3]), ("rr", [1, 2, 3, 1, 2, 1])):
        with Cluster(f"order-{policy}", real_worker=False, policy=policy) as c:
            for request, path in enumerate(paths, 1):
                c.send(f'submit route "{path}"')
                c.receive("ACCEPTED", request)
            c.start("worker", ["--id", "1", "--delay-ms", "15"])
            for request in (1, 2, 3):
                c.receive("COMPLETED", request)
            c.shutdown(4)
            actual = [e["job_id"] for e in traces(c.directory, "broker") if e["event"] == "task_assigned"]
            check(actual == expected, f"{policy}: {actual} != {expected}")

    with Cluster("cancel", real_worker=False) as c:
        for request, path in enumerate(paths[:2], 1):
            c.send(f'submit route "{path}"')
            c.receive("ACCEPTED", request)
        c.send("cancel 2")
        c.receive("CANCEL_ACK", 3)
        check(c.receive("CANCELLED", 2)["in_flight"] == 0, "Queued cancel must settle directly")
        c.start("worker", ["--id", "1", "--delay-ms", "300"])
        wait_event(c, "worker", "test_delay_started")
        c.send("cancel 1")
        ack = c.receive("CANCEL_ACK", 4)
        check(ack["job_state"] == "Cancelling" and ack["in_flight"] == 1, "Active cancel retains actual assignment")
        check(c.receive("CANCELLED", 1)["in_flight"] == 0, "Cancelled only after settlement")
        c.send('submit time "data/small.csv"')
        c.receive("ACCEPTED", 5)
        c.receive("COMPLETED", 5)
        c.shutdown(6)
        events = traces(c.directory, "broker")
        check(not any(e["event"] == "job_completed" and e["job_id"] in (1, 2) for e in events), "Late results cannot resurrect cancellation")
        check(sum(e["event"] == "task_assigned" and e["job_id"] == 1 for e in events) == 1, "No new dispatch after Cancel")
        check(any(e["event"] == "result_discarded" for e in events), "Running cancelled result discarded after identity validation")

    with Cluster("capacity", delay=200, capacity=1) as c:
        c.send('submit route "data/small.csv"')
        c.receive("ACCEPTED", 1)
        c.send('submit time "data/small.csv"')
        check("capacity" in c.receive("REJECTED", 2)["reason"], "Capacity rejection reason")
        c.receive("COMPLETED", 1)
        c.send('submit time "data/small.csv"')
        c.receive("ACCEPTED", 3)
        c.receive("COMPLETED", 3)
        c.shutdown(4)

    with Cluster("isolate-loss", workers=2, delay=450) as c:
        c.send(f'submit route "{source}"')
        c.receive("ACCEPTED", 1)
        wait_event(c, "worker", "test_delay_started")
        wait_event(c, "worker2", "test_delay_started")
        c.processes["worker"].terminate()
        c.processes["worker"].wait(timeout=5)
        c.receive("FAILED", 1)
        c.send('submit time "data/small.csv"')
        c.receive("ACCEPTED", 2)
        check(c.receive("COMPLETED", 2)["summary"] == json.loads((ROOT / "data/expected-time.json").read_text()), "Remaining Worker continues other jobs")
        # Terminated test Worker intentionally has a nonzero exit status.
        c.processes.pop("worker")
        c.shutdown(3)
        events = traces(c.directory, "broker")
        check(any(e["event"] == "result_discarded" and e["job_id"] == 1 for e in events), "Failed job late assignment settled")
        check(not any(e["event"] == "job_completed" and e["job_id"] == 1 for e in events), "Failed job stays failed")

    with Cluster("client-loss", workers=2, delay=200) as c:
        c.send(f'submit route "{source}"')
        c.receive("ACCEPTED", 1)
        wait_event(c, "worker", "test_delay_started")
        c.processes["client"].terminate()
        c.processes["client"].wait(timeout=5)
        for role in ("broker", "worker", "worker2"):
            check(c.processes[role].wait(timeout=8) == 0, f"Clean settlement after Client loss: {role}")
        events = traces(c.directory, "broker")
        check(any(e["event"] == "job_cancelled" for e in events), "Client loss cancels owned job")

    # Replay a once-accepted completion while the Worker holds another assignment.
    c = Cluster("duplicate", real_worker=False)
    peer = None
    try:
        peer = RawWorker(c.session)
        c.send(f'submit route "{paths[0]}"')
        c.receive("ACCEPTED", 1)
        first = peer.task()
        peer.send(6, result_payload(first, valid=1))
        second = peer.task()
        check(second[2] == first[2] + 1, "Next assignment after valid result")
        peer.send(6, result_payload(first, valid=1))
        c.receive("FAILED", 1)
        peer.close()
        c.shutdown(2)
        check(sum(e["event"] == "chunk_merged" for e in traces(c.directory, "broker")) == 1, "Duplicate merged at most once and cannot release unrelated assignment")
    finally:
        if peer:
            peer.close()
        c.close()

    # Index made stale by same-size edit: metadata mismatch must reject admission.
    stale = inputs / "stale.csv"
    stale.write_bytes(paths[0].read_bytes())
    prepare(stale, 10)
    time.sleep(0.02)
    stale.write_bytes(stale.read_bytes().replace(b"500", b"400"))
    with Cluster("stale") as c:
        c.send(f'submit route "{stale}"')
        check("Stale" in c.receive("REJECTED", 1)["reason"], "Reject same-size edited source")
        c.shutdown(2)
    unicode_file = inputs / "ตัวอย่าง หลายชังก์.csv"
    unicode_file.write_bytes(paths[0].read_bytes())
    prepare(unicode_file, 1)
    unicode_reference = json.loads(subprocess.check_output([
        str(BUILD / "reference.exe"), "--file", str(unicode_file), "--analysis", "route"
    ], cwd=ROOT, text=True, encoding="utf-8"))
    with Cluster("unicode-prepare", workers=3) as c:
        c.send(f'submit route "{unicode_file}"')
        check(c.receive("ACCEPTED", 1)["total_chunks"] == 3, "Unicode CLI index must be used")
        check(c.receive("COMPLETED", 1)["summary"] == unicode_reference, "Unicode Prepare/Reference/Submit agree")
        c.shutdown(2)
    for _ in range(10):
        with Cluster("idle-shutdown") as c:
            c.shutdown(1)
    with Cluster("repeated-shutdown", real_worker=False) as c:
        # One pipe write makes both requests available before the shutdown ACK.
        c.send("shutdown\nshutdown")
        check("stopping" in c.receive("REJECTED", 2)["reason"], "Repeated shutdown must preserve the first request")
        c.receive("SHUTDOWN_ACK", 1)
        check(all(p.wait(timeout=5) == 0 for p in c.processes.values()), "No unanswered request after repeated shutdown")

    for executable, args in (("worker", ["--id", "-18446744073709551615"]),
                             ("worker", ["--delay-ms", "+0"]),
                             ("prepare", ["--file", str(source), "--chunk-bytes", "-18446744073709551615"]),
                             ("broker", ["--workers", "+1"])):
        result = subprocess.run([str(BUILD / f"{executable}.exe"), *args], cwd=ROOT, capture_output=True, timeout=5)
        check(result.returncode != 0 and b"Invalid --" in result.stderr, "Unsigned options must reject signs before opening resources")
    print("PASS: concurrency multi-worker/oracle, out-of-order, FIFO/RR, queued/active cancellation, capacity reuse, Worker/Client loss, duplicate result, stale input")


def snapshot(c, requests):
    request = next(requests)
    c.send("status")
    return c.receive("STATUS", request)["monitor"]


def settled(monitor):
    check(not monitor["ready_jobs"], "Dispatch queue still contains jobs")
    check(all(j["state"] in ("Completed", "Cancelled", "Failed") and j["in_flight"] == 0 and j["pending_chunks"] == 0 for j in monitor["jobs"]), "Job/assignment did not settle")
    check(all(w["state"] != "Busy" for w in monitor["workers"]), "Worker remains Busy after settlement")


def correctness(directory):
    boundary = directory / "boundary.csv"
    rows = [b"2024-02-29T23:59:58Z,/a,400", b"2024-02-29T23:59:59Z,/a,500",
            b"2024-02-29T23:59:59Z,/quiet,200", b"", b"\xff", b"2024-02-30T00:00:00Z,/a,500",
            b"2026-10-17T00:00:00Z,/a,600", b"x" * 200000, b"2026-10-17T00:00:00Z,/b,599"]
    boundary.write_bytes(b"\xef\xbb\xbftimestamp,route,status\r\n" + b"\r\n".join(rows))
    counts = {"/a": {"errors": 2, "4xx": 1, "5xx": 1}, "/b": {"errors": 1, "4xx": 0, "5xx": 1}}
    expected = {"route": dict(valid_records=4, malformed_records=5, groups=counts),
                "time": dict(valid_records=4, malformed_records=5, groups={
                    "2024-02-29T23:59:00Z": counts["/a"], "2026-10-17T00:00:00Z": counts["/b"]})}
    quiet = directory / "no-errors.csv"
    quiet.write_bytes(b"timestamp,route,status\n" + b"2026-10-17T00:00:00Z,/quiet,200\n" * 2000)
    header = directory / "header-only.csv"
    header.write_bytes(b"timestamp,route,status")
    with Cluster("resource-boundaries", workers=3, worker_delays={1: 20, 2: 1, 3: 5}) as c:
        requests = itertools.count(1)
        for path, wanted, targets in ((boundary, expected, (1, 64, 65536)),
                                     (quiet, {mode: dict(valid_records=2000, malformed_records=0, groups={}) for mode in expected}, (32, 65536)),
                                     (header, {mode: dict(valid_records=0, malformed_records=0, groups={}) for mode in expected}, (1,))):
            for target in targets:
                prepare(path, target)
                for mode in ("route", "time"):
                    serial = json.loads(subprocess.check_output([str(BUILD / "reference.exe"), "--file", str(path), "--analysis", mode], encoding="utf-8"))
                    request = next(requests)
                    c.send(f'submit {mode} "{path}"')
                    c.receive("ACCEPTED", request)
                    result = c.receive("COMPLETED", request, timeout=30)["summary"]
                    check(result == serial == wanted[mode], f"Fixture/serial/multiprocess mismatch: {path.name}/{target}/{mode}")
        settled(snapshot(c, requests))
        c.shutdown(next(requests))
        events = traces(c.directory, "broker")
        merged = [(e["job_id"], e["chunk_id"]) for e in events if e["event"] == "chunk_merged"]
        check(all(chunk for _, chunk in merged) and len(merged) == len(set(merged)), "Validated merge evidence must identify unique chunks")


def continuous(*, saturate):
    source = ROOT / "data/small.csv"
    expected = {mode: json.loads((ROOT / "data" / f"expected-{mode}.json").read_text()) for mode in ("route", "time")}
    with Cluster("resource-saturation" if saturate else "resource-history", workers=3, capacity=6 if saturate else 32, delay=40 if saturate else 0) as c:
        requests = itertools.count(1)
        accepted_count = rejected_count = 0
        for wid in range(1, 4):
            wait_event(c, "broker", "worker_ready", lambda e, wid=wid: e["worker_id"] == wid)
        initial = {role: process_usage(p)["handles"] for role, p in c.processes.items() if role != "client"}
        for wave in range(4 if saturate else 10):
            submitted = []
            for index in range(32):
                request = next(requests)
                mode = "route" if index % 2 else "time"
                c.send(f'submit {mode} "{source}"')
                submitted.append((request, mode))
            admitted = []
            for request, mode in submitted:
                response = c.receive(("ACCEPTED", "REJECTED"), request)
                if response["type"] == "ACCEPTED":
                    admitted.append((request, mode))
                else:
                    check(saturate and "capacity" in response["reason"], "Unexpected rejection during sustained arrivals")
                    rejected_count += 1
            for request, mode in admitted:
                check(c.receive("COMPLETED", request, timeout=20)["summary"] == expected[mode], "Continuous-job result mismatch")
            accepted_count += len(admitted)
            monitor = snapshot(c, requests)
            settled(monitor)
            usage = {role: process_usage(c.processes[role])["handles"] for role in initial}
            check(all(usage[role] <= count + 2 for role, count in initial.items()), f"Handles accumulate after settlement: {usage}/{initial}")
        if saturate:
            check(accepted_count > 0 and rejected_count > 0, "Did not exercise both admission and rejection")
        else:
            check(accepted_count == 320 and len(monitor["jobs"]) == 256, "History bound/continuous admission mismatch")
            request = next(requests)
            c.send("status 1")
            check("evicted" in c.receive("REJECTED", request)["reason"], "Evicted job must not appear current")
        c.shutdown(next(requests))


def cancellation_loss(directory):
    source = directory / "cancel.csv"
    source.write_bytes(b"timestamp,route,status\n" + b"2026-10-17T00:00:00Z,/a,500\n" * 1000)
    prepare(source, 1)
    with Cluster("resource-cancel-loss", workers=3, delay=300) as c:
        c.send(f'submit route "{source}"')
        c.receive("ACCEPTED", 1)
        for role in ("worker", "worker2", "worker3"):
            wait_event(c, role, "test_delay_started")
        c.send("cancel 1")
        ack = c.receive("CANCEL_ACK", 2)
        check(ack["job_state"] == "Cancelling" and ack["in_flight"] == 3, "Cancellation released actual assignments too early")
        c.processes["worker"].terminate()
        c.processes["worker"].wait(timeout=5)
        c.processes.pop("worker")
        check(c.receive("CANCELLED", 1)["in_flight"] == 0, "Cancellation did not settle after Worker loss")
        c.send('submit route "data/small.csv"')
        c.receive("ACCEPTED", 3)
        check(c.receive("COMPLETED", 3)["summary"] == json.loads((ROOT / "data/expected-route.json").read_text()), "Remaining Workers could not run the next job")
        c.send("status")
        settled(c.receive("STATUS", 4)["monitor"])
        c.shutdown(5)


def progress_bound(directory):
    source = directory / "progress.csv"
    source.write_bytes(b"timestamp,route,status\n" + b"2026-10-17T00:00:00Z,/a,500\n" * 1000)
    prepare(source, 1)
    with Cluster("resource-progress", workers=3) as c:
        c.send(f'submit route "{source}"')
        accepted = c.receive("ACCEPTED", 1)
        result = c.receive("COMPLETED", 1, timeout=30)
        check(result["summary"]["groups"]["/a"]["errors"] == 1000, "Throttled progress lost results")
        c.shutdown(2)
        events = traces(c.directory, "broker")
        first = next(e for e in events if e["event"] == "job_accepted")
        last = next(e for e in events if e["event"] == "job_completed")
        elapsed = (last["qpc_ticks"] - first["qpc_ticks"]) / first["qpc_frequency"]
        messages = sum(e["event"] == "response_queued" and e["detail"] == "PROGRESS" for e in events)
        check(0 < messages < accepted["total_chunks"] - 1 and messages <= 2 + math.ceil(elapsed / 0.05), "Progress was not bounded independently of chunk count")


def resources():
    directory = BUILD / "resource-input"
    directory.mkdir(exist_ok=True)
    for name, action in (("record_edges_no_errors", correctness), ("continuous_saturation", lambda d: continuous(saturate=True)),
                         ("continuous_history_handles", lambda d: continuous(saturate=False)),
                         ("cancel_with_worker_loss", cancellation_loss), ("bounded_progress", progress_bound)):
        action(directory)
        print("PASS", name, flush=True)


if __name__ == "__main__":
    check(os.name == "nt", "Process tests require Windows")
    for suite in (transport, concurrency, resources):
        suite()
        print("PASS", suite.__name__, flush=True)
