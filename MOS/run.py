#!/usr/bin/env python3
"""Build and run the actual course MOS on its C S2 simulator, with observation."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
KERNEL_END = "// function prototype\nreader()"


def symbols(listing: str) -> tuple[dict[str, int], dict[str, int], int]:
    globals_: dict[str, int] = {}
    labels: dict[str, int] = {}
    pending: list[str] = []
    in_symbols = False
    highest = 0
    for line in listing.splitlines():
        if line.strip() == ".symbol":
            in_symbols = True
            continue
        if line.strip().startswith(".code"):
            in_symbols = False
        if in_symbols:
            match = re.fullmatch(r"\s*(\w+)\s+(\d+)\s*", line)
            if match:
                globals_[match[1]] = int(match[2])
        match = re.fullmatch(r"\s*:(\w+)\s*", line)
        if match:
            pending.append(match[1])
        match = re.match(r"\s*(\d+)\s+[LDX]\s", line)
        if match:
            address = int(match[1])
            highest = max(highest, address)
            for label in pending:
                labels[label] = address
            pending.clear()
    return globals_, labels, highest


def command(args: list[str], cwd: Path, log: str, allow_failure: bool = False) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(args, cwd=cwd, capture_output=True, timeout=60)
    (cwd / log).write_bytes(result.stdout + result.stderr)
    if result.returncode and not allow_failure:
        raise RuntimeError(f"command failed ({result.returncode}): {args!r}; see {cwd / log}")
    return result


def run_experiment(capacity: int = 4, items: int = 24, consumers: int = 1,
                   timer: int = 400, output: Path | None = None,
                   tools: Path | None = None, gcc: str = "gcc",
                   max_clocks: int = 2_000_000, original_scheduler: bool = False) -> dict:
    if not 1 <= capacity <= 64:
        raise ValueError("capacity must be in 1..64")
    if not 1 <= consumers <= 8:
        raise ValueError("consumers must be in 1..8")
    if not 1 <= items <= 10_000 or items % consumers:
        raise ValueError("items must be in 1..10000 and divisible by consumers")
    if not 64 <= timer <= 1_000_000:
        raise ValueError("timer must be in 64..1000000 simulation clocks")
    if not 1000 <= max_clocks <= 100_000_000:
        raise ValueError("max_clocks must be in 1000..100000000")
    tools = (tools or ROOT / "tools").resolve()
    compiler, assembler = tools / "rz36.exe", tools / "as21.exe"
    gcc_path = shutil.which(gcc)
    if not compiler.is_file() or not assembler.is_file() or not gcc_path:
        raise FileNotFoundError("need Rz36/as21 Windows executables and GCC; see README.md")
    output = (output or ROOT.parent / "build" / "mos" / f"k{capacity}-c{consumers}-t{timer}").resolve()
    output.mkdir(parents=True, exist_ok=True)
    baseline = (ROOT / "course" / "mos-rz2.txt").read_text(encoding="utf-8")
    kernel = baseline.split(KERNEL_END)[0]
    if kernel == baseline:
        raise RuntimeError("course MOS source does not contain the expected application marker")
    if not original_scheduler:
        original_nextp = kernel[kernel.index("nextp()           // next process"):kernel.index("terminate()          // terminate")]
        safe_nextp = (ROOT / "scheduler-nextp.rz").read_text(encoding="ascii")
        kernel = kernel.replace(original_nextp, safe_nextp + "\n")
    app = (ROOT / "buffer-app.rz").read_text(encoding="utf-8")
    for name, value in {"CAPACITY": capacity, "ITEMS": items, "CONSUMERS": consumers, "TIMER": timer}.items():
        app = app.replace(f"@{name}@", str(value))
    source = output / "buffer.txt"
    source.write_text(kernel + app, encoding="ascii")
    compiled = command([str(compiler), source.name], output, "compiler.log")
    if b"error" in compiled.stdout.lower() or b"error" in compiled.stderr.lower():
        raise RuntimeError(f"Rz compilation reported an error; see {output / 'compiler.log'}")
    assembly = output / "buffer-s.txt"
    assembly.write_bytes(compiled.stdout)
    command([str(assembler), assembly.name], output, "assembler.log")
    object_file = output / "buffer-s.obj"
    listing = output / "buffer-s.lis"
    if not object_file.is_file() or not listing.is_file():
        raise RuntimeError("assembler did not create object/listing")
    globals_, labels, highest = symbols(listing.read_text(encoding="ascii"))
    if highest >= 1000:
        raise RuntimeError("compiled code overlaps the S2 interrupt vector at address 1000")
    header = [f"#define G_{name} {value}" for name, value in globals_.items()]
    header += [f"#define F_{name} {value}" for name, value in labels.items()]
    header += [f"#define MOS_PROCESSES {consumers + 1}", f"#define MOS_CAPACITY {capacity}", f"#define MOS_MAX_CLOCKS {max_clocks}"]
    (output / "symbols.h").write_text("\n".join(header) + "\n", encoding="ascii")
    (output / "symbols.json").write_text(json.dumps({"globals": globals_, "labels": labels}, indent=2), encoding="utf-8")

    # Keep the source copies unchanged. Build-only adapters rename the original
    # interactive main, replace its interrupt printf with a read-only hook, and
    # replace the unused nonstandard RNG dependency with the C library RNG.
    simulator = (ROOT / "course" / "sim21" / "s21.c").read_text(encoding="ascii")
    hook = 'printf("interrupt%d\\n",n);'
    if simulator.count(hook) != 1:
        raise RuntimeError("unexpected course interrupt implementation")
    simulator = "#define main course_main\nvoid mos_interrupt(int n);\nvoid mos_after_instruction(void);\n" + simulator.replace(hook, "mos_interrupt(n);")
    instruction_hook = "\t\trun();"
    if simulator.count(instruction_hook) != 1:
        raise RuntimeError("unexpected course instruction execution loop")
    simulator = simulator.replace(instruction_hook, instruction_hook + "\n\t\tmos_after_instruction();")
    (output / "s21-observed.c").write_text(simulator, encoding="ascii")
    io_source = (ROOT / "course" / "sim21" / "io.c").read_text(encoding="ascii")
    io_source = io_source.replace("#include <random.h>", "/* RNG port unused in this experiment; use ISO C RNG. */")
    io_source = io_source.replace("mt_init_genrand(0)", "srand(0)").replace("genrand_int31()", "rand()")
    (output / "io-portable.c").write_text(io_source, encoding="ascii")
    executable = output / "sim-trace.exe"
    command([gcc_path, "-std=c99", "-O2", "-fwrapv", "-I", str(ROOT / "course" / "sim21"),
             "-I", str(output), str(output / "s21-observed.c"),
             str(ROOT / "course" / "sim21" / "load.c"), str(output / "io-portable.c"),
             str(ROOT / "observer.c"), "-lm", "-o", str(executable)], output, "simulator-build.log")
    trace = output / "trace.jsonl"
    execution = command([str(executable), str(object_file), str(trace)], output, "simulator.log", allow_failure=True)
    config = {"capacity": capacity, "items": items, "consumers": consumers,
              "timer": timer, "timer_unit": "S2 simulation clocks", "max_clocks": max_clocks,
              "scheduler": "original-course-nextp" if original_scheduler else "interrupt-safe-nextp"}
    (output / "manifest.json").write_text(json.dumps({"config": config}, indent=2) + "\n", encoding="utf-8")
    from validate_trace import validate
    try:
        if execution.returncode:
            raise ValueError(f"S2 simulator did not stop normally (code {execution.returncode}); see simulator.log")
        report = validate(trace, config)
    except ValueError as error:
        (output / "validation.json").write_text(json.dumps({"passed": False, "error": str(error)}, indent=2) + "\n", encoding="utf-8")
        raise
    (output / "validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return {"output": str(output), **report}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capacity", type=int, default=4)
    parser.add_argument("--items", type=int, default=24)
    parser.add_argument("--consumers", type=int, default=1)
    parser.add_argument("--timer", type=int, default=400)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--tools", type=Path)
    parser.add_argument("--gcc", default="gcc")
    parser.add_argument("--max-clocks", type=int, default=2_000_000)
    parser.add_argument("--original-scheduler", action="store_true", help="reproduce the unmodified nextp frame corruption; validation may fail")
    options = vars(parser.parse_args())
    print(json.dumps(run_experiment(**options), indent=2))


if __name__ == "__main__":
    main()
