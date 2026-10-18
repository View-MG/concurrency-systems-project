"""Reproducible real-process measurements; synthetic-delay evidence is separate."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import itertools
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from support import BUILD, Cluster, check, traces, oracle, wait_event, prepare, process_usage
from generate import generate


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def distribution(values):
    ordered = sorted(values)
    return {"n": len(ordered), "min": ordered[0], "median": statistics.median(ordered),
            "max": ordered[-1], "p95_nearest_rank": ordered[math.ceil(0.95 * len(ordered)) - 1]}


def reference(path, mode, timing_path):
    value = subprocess.check_output([str(BUILD / "reference.exe"), "--file", str(path),
                                    "--analysis", mode, "--timing-file", str(timing_path)],
                                   cwd=ROOT, text=True, encoding="utf-8")
    return json.loads(value), json.loads(timing_path.read_text(encoding="utf-8"))


def one(events, event, job=None, request=None, kind=None):
    matches = [e for e in events if e["event"] == event and (job is None or e["job_id"] == job)
               and (request is None or e["request_id"] == request)
               and (kind is None or e["detail"] == kind)]
    check(len(matches) == 1, f"Expected exactly one {event}/{job}/{request}/{kind}: {len(matches)}")
    return matches[0]


def measures(c, jobs, before, after):
    broker, client = traces(c.directory, "broker"), traces(c.directory, "client")
    frequency = broker[0]["qpc_frequency"]
    check(all(e["qpc_frequency"] == frequency for e in broker + client), "QPC frequencies disagree")
    result, accepted_ticks, terminal_ticks = {}, [], []
    for label, description in jobs.items():
        job, request = description["job"], description["request"]
        command = one(client, "command_started", request=request, kind="SUBMIT")
        accepted = one(broker, "job_accepted", job=job)
        admitted = one(client, "response_received", request=request, kind="ACCEPTED")
        terminal = one(broker, "job_completed", job=job)
        final = one(client, "response_received", request=request, kind="COMPLETED")
        assignments = [e for e in broker if e["event"] == "task_assigned" and e["job_id"] == job]
        returns = [e for e in broker if e["event"] == "result_received" and e["job_id"] == job]
        check(len(assignments) == len(returns) == description["total_chunks"], "Incomplete assignment evidence")
        elapsed = (terminal["qpc_ticks"] - accepted["qpc_ticks"]) / frequency
        check(elapsed > 0, "Nonpositive Job turnaround")
        result[label] = {
            "analysis": description["mode"], "job_id": job, "request_id": request,
            "total_chunks": len(assignments), "summary_sha256": description["summary_hash"],
            "valid_records": description["summary"]["valid_records"],
            "malformed_records": description["summary"]["malformed_records"],
            "admission_ms": (admitted["qpc_ticks"] - command["qpc_ticks"]) * 1000 / frequency,
            "queue_ms": (assignments[0]["qpc_ticks"] - accepted["qpc_ticks"]) * 1000 / frequency,
            "turnaround_ms": elapsed * 1000,
            "client_response_ms": (final["qpc_ticks"] - command["qpc_ticks"]) * 1000 / frequency,
            "valid_records_per_second": description["summary"]["valid_records"] / elapsed,
            "worker_compute_us_sum": description["compute_us"],
            "opportunities_by_worker": dict(Counter(str(e["worker_id"]) for e in assignments)),
            "accepted_ticks": accepted["qpc_ticks"], "terminal_ticks": terminal["qpc_ticks"],
            "command_boundary_event": command["event"],
        }
        accepted_ticks.append(accepted["qpc_ticks"])
        terminal_ticks.append(terminal["qpc_ticks"])
    start, end = min(accepted_ticks), max(terminal_ticks)
    assignments = [e for e in broker if e["event"] == "task_assigned"]
    returns = {(e["job_id"], e["chunk_id"], e["worker_id"]): e for e in broker if e["event"] == "result_received"}
    busy = defaultdict(int)
    for assignment in assignments:
        key = (assignment["job_id"], assignment["chunk_id"], assignment["worker_id"])
        check(key in returns, "Assignment without settlement")
        busy[str(assignment["worker_id"])] += max(0, min(end, returns[key]["qpc_ticks"]) - max(start, assignment["qpc_ticks"]))
    observation = (end - start) / frequency
    system = {
        "observation_seconds": observation,
        "observation_boundary": "first measured Broker job_accepted to last measured Broker job_completed",
        "valid_records_per_second": sum(j["valid_records"] for j in result.values()) / observation,
        "worker_assignment_busy_fraction": {str(i): busy[str(i)] / (end - start) for i in range(1, len(c.processes) - 1)},
        "ipc_messages_probe_inclusive": after["ipc_messages"] - before["ipc_messages"],
        "ipc_bytes_probe_inclusive": after["ipc_bytes"] - before["ipc_bytes"],
        "ipc_messages_excluding_two_probe_messages": after["ipc_messages"] - before["ipc_messages"] - 2,
        "ipc_snapshot_before": {k: before[k] for k in ("ipc_messages", "ipc_bytes")},
        "ipc_snapshot_after": {k: after[k] for k in ("ipc_messages", "ipc_bytes")},
    }
    check(all(0 <= value <= 1.00001 for value in system["worker_assignment_busy_fraction"].values()), "Invalid busy fraction")
    if "B" in result:
        boundary = result["B"]["accepted_ticks"]
        prefix = [e for e in assignments if e["qpc_ticks"] >= boundary][:12]
        labels = {description["job"]: label for label, description in jobs.items()}
        system["first_12_dispatches_after_B_admission"] = [labels[e["job_id"]] for e in prefix]
        system["opportunities_in_that_prefix"] = dict(Counter(labels[e["job_id"]] for e in prefix))
    return result, system


def execute(run_id, descriptions, workers=3, policy="fifo", io_mode="async", delays=None, mixed=False):
    """Run serial submissions or overlap A/B with the same measurement boundaries."""
    with Cluster("exp-" + hashlib.sha256(run_id.encode()).hexdigest()[:12],
                workers=workers, policy=policy, io_mode=io_mode, worker_delays=delays) as c:
        jobs, request = {}, 1

        def collect(label, expected):
            result = c.receive("COMPLETED", jobs[label]["request"], timeout=90)
            check(result["summary"] == expected, f"Serial/oracle mismatch: {run_id}/{label}")
            jobs[label].update(summary=result["summary"], compute_us=result["worker_compute_us"],
                summary_hash=hashlib.sha256(json.dumps(result["summary"], sort_keys=True, separators=(",", ":")).encode()).hexdigest())

        for wid in range(1, workers + 1):
            wait_event(c, "broker", "worker_ready", lambda e, wid=wid: e["worker_id"] == wid)
        c.send("status")
        before = c.receive("STATUS", request)["monitor"]
        initial = {role: process_usage(p) for role, p in c.processes.items()}
        for label, path, mode, expected in descriptions:
            request += 1
            c.send(f'submit {mode} "{path}"')
            accepted = c.receive("ACCEPTED", request, timeout=30)
            jobs[label] = dict(job=accepted["job_id"], request=request, mode=mode, total_chunks=accepted["total_chunks"])
            if mixed and label == "A":
                wait_event(c, "worker", "test_delay_started" if delays else "compute_started",
                           lambda e: e["job_id"] == accepted["job_id"])
            if not mixed:
                collect(label, expected)
        if mixed:
            for label, _, _, expected in descriptions:
                collect(label, expected)
        request += 1
        c.send("status")
        after = c.receive("STATUS", request)["monitor"]
        final = {role: process_usage(p) for role, p in c.processes.items()}
        check(not after["ready_jobs"] and all(j["in_flight"] == 0 and j["state"] == "Completed" for j in after["jobs"]), "Unsettled benchmark jobs")
        job_metrics, system = measures(c, jobs, before, after)
        broker, client = traces(c.directory, "broker"), traces(c.directory, "client")
        frequency = broker[0]["qpc_frequency"]
        worker_events = [e for role in c.processes if role.startswith("worker") for e in traces(c.directory, role)]
        for job in job_metrics.values():
            starts = [e["qpc_ticks"] for e in worker_events if e["event"] == "compute_started" and e["job_id"] == job["job_id"]]
            job["first_compute_ms"] = (min(starts) - job["accepted_ticks"]) * 1000 / frequency
            job["progress_messages"] = sum(e["event"] == "response_queued" and e["detail"] == "PROGRESS" and e["job_id"] == job["job_id"] for e in broker)
        if mixed:
            command = one(client, "command_started", request=jobs["B"]["request"], kind="SUBMIT")
            check(command["qpc_ticks"] < job_metrics["A"]["terminal_ticks"], "A finished before B was submitted; use a larger workload")
            system["B_submit_after_A_accept_ms"] = (command["qpc_ticks"] - job_metrics["A"]["accepted_ticks"]) * 1000 / frequency
        telemetry = {}
        for role, start in initial.items():
            cpu = final[role]["cpu_seconds"] - start["cpu_seconds"]
            elapsed = final[role]["sample_seconds"] - start["sample_seconds"]
            telemetry[role] = dict(cpu_seconds=cpu, sample_seconds=elapsed, cpu_fraction=cpu / elapsed,
                                   handle_delta=final[role]["handles"] - start["handles"])
        c.shutdown(request + 1)
        return dict(id=run_id, workers=workers, policy=policy, io_mode=io_mode, artificial_delay_ms=delays or {},
                    jobs=job_metrics, system=system, process_usage=telemetry,
                    trace_directory=str(c.directory.relative_to(ROOT)))



def fairness(slowdowns):
    check(bool(slowdowns) and all(s > 0 for s in slowdowns), "Positive slowdowns required")
    service = [1 / s for s in slowdowns]
    return sum(service) ** 2 / (len(service) * sum(s * s for s in service))


def summarize(runs):
    buckets = defaultdict(list)
    for run in runs:
        key = (run["experiment"], run["policy"], run["io_mode"], run["workers"], run["chunk_bytes"], run["scenario"])
        buckets[key].append(run)
    rows = []
    for key, values in sorted(buckets.items()):
        rows.append(dict(zip(("experiment", "policy", "io_mode", "workers", "chunk_bytes", "scenario"), key)) | {
            "jobs": {label: {metric: distribution([r["jobs"][label][metric] for r in values])
                             for metric in ("admission_ms", "queue_ms", "turnaround_ms", "client_response_ms", "slowdown")}
                     for label in values[0]["jobs"]},
            "throughput": distribution([r["system"]["valid_records_per_second"] for r in values]),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=500000)
    parser.add_argument("--small-records", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20261017)
    parser.add_argument("--output", type=Path, default=BUILD / "experiments.json")
    args = parser.parse_args()
    check(os.name == "nt", "Windows process experiments require Windows")
    check(0 < args.small_records < args.records and args.repeats > 0, "Positive sizes/repetitions required")
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    inputs = BUILD / "experiment-input"
    report = dict(passed=False, seed=args.seed, repeats=args.repeats, runs=[], inputs={}, serial={},
                  machine=dict(os=platform.platform(), cpu=os.environ.get("PROCESSOR_IDENTIFIER", platform.processor()),
                               logical_processors=os.cpu_count(), python=sys.version,
                               compiler=subprocess.check_output(["g++", "--version"], text=True).splitlines()[0]),
                  started_utc=datetime.now(timezone.utc).isoformat())

    def checkpoint():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    checkpoint()  # A failed run must never leave an old passed=true result.
    descriptions = {}
    for i, (label, count) in enumerate((("A", args.records), ("B", args.small_records), ("delayA", 10000), ("delayB", 500))):
        path = inputs / f"{label}.csv"
        data = generate(path, records=count, seed=args.seed + i, routes=64, malformed_ratio=0.01)
        report["inputs"][label] = dict(data, path=str(path.relative_to(ROOT)), sha256=sha256(path))
        for mode in (("route", "time") if label == "A" else ("route",)):
            expected = oracle(path, mode)
            key = f"{label}-{mode}"
            descriptions[key] = (label, path, mode, expected)
            report["serial"][key] = []
            for repeat in range(args.repeats):
                summary, timing = reference(path, mode, inputs / f"serial-{key}-{repeat}.json")
                check(summary == expected, "Serial/independent oracle disagreement")
                report["serial"][key].append(timing)

    def measure(experiment, repeat, policy, workers, chunk, io_mode="async", delays=None):
        prefix = "delay" if delays else ""
        a = ("A", *descriptions[f"{prefix}A-route"][1:])
        b = ("B", *descriptions[f"{prefix}B-route"][1:])
        for _, path, _, _ in (a, b):
            prepare(path, chunk)
        alone = {}
        for scenario, jobs in (("A_alone", [a]), ("B_alone", [b]), ("mixed", [a, b])):
            run_id = f"{experiment}-r{repeat}-{policy}-{io_mode}-w{workers}-b{chunk}-{scenario}"
            print("Measuring", run_id, flush=True)
            run = execute(run_id, jobs, workers, policy, io_mode, delays, scenario == "mixed")
            run.update(experiment=experiment, repeat=repeat, chunk_bytes=chunk, scenario=scenario)
            for label, job in run["jobs"].items():
                if scenario != "mixed":
                    alone[label] = job["turnaround_ms"]
                job["alone_turnaround_ms"] = alone[label]
                job["slowdown"] = job["turnaround_ms"] / alone[label]
                serial = report["serial"][f"{prefix}{label}-route"][repeat]["duration_us"]
                job["serial_parse_over_turnaround"] = serial / (job["turnaround_ms"] * 1000)
            if scenario == "mixed":
                run["fairness_inverse_slowdown"] = fairness([j["slowdown"] for j in run["jobs"].values()])
            report["runs"].append(run)
            checkpoint()

    rng = random.Random(args.seed)
    for repeat in range(args.repeats):
        configurations = list(itertools.product(("fifo", "rr"), (1, 2, 3), (65536, 262144, 1048576)))
        rng.shuffle(configurations)
        for policy, workers, chunk in configurations:
            measure("real", repeat, policy, workers, chunk)
            if policy == "fifo":
                # Time analysis also runs with every Worker/chunk configuration.
                description = descriptions["A-time"]
                run = execute(f"time-r{repeat}-w{workers}-b{chunk}", [description], workers)
                run.update(experiment="time", repeat=repeat, chunk_bytes=chunk, scenario="A_alone")
                run["jobs"]["A"]["slowdown"] = 1.0
                report["runs"].append(run)
                checkpoint()
        controlled = list(itertools.product(("fifo", "rr"), ("async", "sequential-wait")))
        rng.shuffle(controlled)
        for policy, io_mode in controlled:
            measure("controlled", repeat, policy, 2, 32768, io_mode, {1: 120, 2: 12})
    report.update(passed=True, aggregate=summarize(report["runs"]), completed_utc=datetime.now(timezone.utc).isoformat())
    checkpoint()
    print(f"PASS: {len(report['runs'])} clusters; results: {output}")


if __name__ == "__main__":
    main()
