#!/usr/bin/env python3
"""Validate observed S2 memory events, without inferring CPU behavior from a model."""
from __future__ import annotations

import argparse
from collections import Counter, deque
import json
from pathlib import Path


def validate(path: Path, config: dict) -> dict:
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    if not records or records[0]["event"] != "init" or records[-1]["event"] != "stop":
        raise ValueError("trace must begin with init and finish with an actual CPU stop")
    capacity, items = config["capacity"], config["items"]
    queues = {s["address"]: deque() for s in records[0]["semaphores"].values()}
    sem_names = {s["address"]: name for name, s in records[0]["semaphores"].items()}
    initial = records[0]["semaphores"]
    if [initial[s]["value"] for s in ("empty", "full", "mutex")] != [capacity, 0, 1]:
        raise ValueError("wrong initial semaphore values")
    put_values, get_values = [], []
    pending = deque()
    wake_eligible: set[int] = set()
    sem_values = {s["address"]: s["value"] for s in initial.values()}
    counts: Counter[str] = Counter()
    consumption: Counter[int] = Counter()
    puts, gets, previous_clock = 0, 0, -1
    head, tail = 0, 0
    for number, record in enumerate(records, 1):
        event = record["event"]
        counts[event] += 1
        if record["seq"] != number or record["clock"] < previous_clock:
            raise ValueError(f"nonmonotonic sequence/time at event {number}")
        previous_clock = record["clock"]
        if not 0 <= record["occupancy"] <= capacity:
            raise ValueError(f"buffer bound violated at event {number}")
        if not (0 <= record["head"] < capacity and 0 <= record["tail"] < capacity):
            raise ValueError(f"head/tail out of bounds at event {number}")
        subject, sem = record["subject"], record["semaphore"]
        if event == "queue_enter":
            if any(subject in q for q in queues.values()):
                raise ValueError(f"duplicate waiting process {subject}")
            if record["semaphores"][sem_names[sem]]["value"] >= 0:
                raise ValueError("semaphore queue entry without negative semaphore value")
            queues[sem].append(subject)
        elif event == "queue_leave":
            if not queues[sem] or queues[sem].popleft() != subject:
                raise ValueError("semaphore wake order is not FIFO")
            wake_eligible.add(subject)
        elif event == "wakeup":
            if subject not in wake_eligible:
                raise ValueError("wakeup did not follow removal from a semaphore queue")
            wake_eligible.remove(subject)
        elif event == "block":
            if sum(subject in q for q in queues.values()) != 1:
                raise ValueError("blocking process is not in exactly one semaphore wait queue")
        elif event == "semaphore_value":
            if abs(record["value"] - sem_values[sem]) != 1:
                raise ValueError("semaphore value changed by more than one permit")
            sem_values[sem] = record["value"]
        elif event in ("put", "get"):
            if record["semaphores"]["mutex"]["value"] > 0:
                raise ValueError("buffer accessed without acquiring mutex")
            if event == "put":
                puts += 1
                put_values.append(record["value"])
                pending.append(record["value"])
                tail = (tail + 1) % capacity
                if record["buffer"][(tail - 1) % capacity] != record["value"]:
                    raise ValueError("put value disagrees with actual shared buffer memory")
            else:
                gets += 1
                get_values.append(record["value"])
                consumption[subject] += 1
                if not pending or pending.popleft() != record["value"]:
                    raise ValueError("consumption does not match FIFO buffer contents")
                head = (head + 1) % capacity
                if record["buffer"][(head - 1) % capacity] != record["value"]:
                    raise ValueError("get value disagrees with actual shared buffer memory")
            if [record["produced"], record["consumed"], record["occupancy"]] != [puts, gets, puts - gets]:
                raise ValueError("buffer counters disagree with observed put/get events")
            if (record["head"], record["tail"]) != (head, tail):
                raise ValueError("circular buffer indices disagree with put/get events")
        elif event == "dispatch":
            pcbs = {p["address"]: p for p in record["pcbs"]}
            selected = pcbs.get(record["current"])
            if not selected or selected["context"][5] != 1 or selected["terminated"]:
                raise ValueError("dispatched process is not runnable")
            if any(record["current"] in q for q in queues.values()):
                raise ValueError("dispatched process is still waiting")
            saved = selected["context"]
            if [record["pc"], record["sp"], record["fp"], record["rads"], record["retval"]] != [saved[0], saved[1] - 16, saved[2], saved[3], saved[4]]:
                raise ValueError("PC/SP/FP/rads/retval were not restored from the selected PCB")
            if record["registers"][:16] != selected["saved_registers"]:
                raise ValueError("r0..r15 were not restored from the selected process stack")
            if sum(p["context"][5] for p in pcbs.values()) != record["nump"]:
                raise ValueError("runnable PCB count disagrees with nump at dispatch")
            for semaphore in record["semaphores"].values():
                if list(queues[semaphore["address"]]) != semaphore["waiting"]:
                    raise ValueError("observed semaphore queue disagrees with entry/exit events")
                if len(semaphore["waiting"]) != max(0, -semaphore["value"]):
                    raise ValueError("negative semaphore count disagrees with waiting queue at dispatch")
    final = records[-1]
    expected = list(range(1, items + 1))
    if put_values != expected or get_values != expected or pending:
        raise ValueError("data lost, duplicated, reordered, or not fully consumed")
    if any(queues.values()) or wake_eligible:
        raise ValueError("unsettled semaphore waiting/wakeup at stop")
    if final["nump"] or final["occupancy"] or final["produced"] != items or final["consumed"] != items:
        raise ValueError("simulation stopped before completion")
    if not all(p["terminated"] and p["context"][5] == 0 for p in final["pcbs"]):
        raise ValueError("not all processes terminated")
    if [final["semaphores"][s]["value"] for s in ("empty", "full", "mutex")] != [capacity, 0, 1]:
        raise ValueError("semaphore permits did not return to their initial values")
    if len(consumption) != config["consumers"] or set(consumption.values()) != {items // config["consumers"]}:
        raise ValueError("consumer quotas disagree")
    if not counts["block"] or not counts["wakeup"] or not counts["dispatch"]:
        raise ValueError("trace does not exercise blocking, wakeup, and dispatch")
    return {"passed": True, "implementation": "actual-course-MOS-on-C-S2",
            "simulation_clocks": final["clock"], "events": len(records),
            "produced": puts, "consumed": gets, "max_occupancy": max(r["occupancy"] for r in records),
            "event_counts": dict(counts), "consumed_per_pcb": dict(consumption)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    manifest = args.manifest or args.trace.with_name("manifest.json")
    data = json.loads(manifest.read_text(encoding="utf-8"))
    print(json.dumps(validate(args.trace, data["config"]), indent=2))


if __name__ == "__main__":
    main()
