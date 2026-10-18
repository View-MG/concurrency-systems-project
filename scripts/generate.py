"""Reproducible synthetic CSV workload; no network or third-party dependencies."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random


def generate(path, records=10000, error_ratio=0.2, routes=16, seconds=3600,
             seed=42, hotspot=0.6, burst_start=900, burst_width=120,
             burst_fraction=0.5, malformed_ratio=0.0):
    if not (0 <= records <= 100_000_000 and 1 <= routes <= 4096 and seconds > 0):
        raise ValueError("Invalid record count, route count or time range")
    if any(not 0 <= v <= 1 for v in (error_ratio, hotspot, burst_fraction, malformed_ratio)):
        raise ValueError("Ratios must be in [0,1]")
    if not (0 <= burst_start < seconds and 1 <= burst_width <= seconds - burst_start):
        raise ValueError("Burst must fit within time range")
    rng = random.Random(seed)
    start = datetime(2026, 10, 11, tzinfo=timezone.utc)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = dict(valid_records=0, malformed_records=0, errors=0, errors4xx=0, errors5xx=0)
    with path.open("w", encoding="utf-8", newline="\n") as output:
        output.write("timestamp,route,status\n")
        for _ in range(records):
            malformed = rng.random() < malformed_ratio
            error = rng.random() < error_ratio
            route = 0 if error and rng.random() < hotspot else rng.randrange(routes)
            offset = (burst_start + rng.randrange(burst_width)
                      if error and rng.random() < burst_fraction else rng.randrange(seconds))
            stamp = (start + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
            status = rng.choice((400, 401, 403, 404, 429, 500, 502, 503)) if error else rng.choice((200, 201, 204, 301))
            if malformed:
                output.write("malformed-record\n")
                counts["malformed_records"] += 1
            else:
                output.write(f"{stamp},/route-{route:04d},{status}\n")
                counts["valid_records"] += 1
                if error:
                    counts["errors"] += 1
                    counts["errors4xx" if status < 500 else "errors5xx"] += 1
    metadata = dict(records=records, error_ratio=error_ratio, routes=routes, seconds=seconds,
                    seed=seed, hotspot=hotspot, burst_start=burst_start, burst_width=burst_width,
                    burst_fraction=burst_fraction, malformed_ratio=malformed_ratio,
                    realized=counts, bytes=path.stat().st_size)
    path.with_suffix(path.suffix + ".meta.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--error-ratio", type=float, default=0.2)
    parser.add_argument("--routes", type=int, default=16)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--hotspot", type=float, default=0.6, help="Probability of route 0 for an error")
    parser.add_argument("--burst-start", type=int, default=900)
    parser.add_argument("--burst-width", type=int, default=120)
    parser.add_argument("--burst-fraction", type=float, default=0.5)
    parser.add_argument("--malformed-ratio", type=float, default=0.0)
    args = vars(parser.parse_args())
    args["path"] = args.pop("file")
    print(json.dumps(generate(**args), indent=2))


if __name__ == "__main__":
    main()
