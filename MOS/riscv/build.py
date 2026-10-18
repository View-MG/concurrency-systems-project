#!/usr/bin/env python3
"""Compile the small counting function to real RV32I assembly and ELF object."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent


def run(args: list[str], cwd: Path) -> str:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"command failed: {args!r}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def main() -> None:
    default = Path(os.environ.get("LOCALAPPDATA", "")) / "Arduino15/packages/esp32/tools/esp-rv32/2411/bin"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--toolchain", type=Path, default=default)
    parser.add_argument("--output", type=Path, default=ROOT.parents[1] / "build" / "riscv")
    parser.add_argument("--host-cc", default="gcc")
    args = parser.parse_args()
    tools = args.toolchain.resolve()
    compiler = tools / "riscv32-esp-elf-gcc.exe"
    objdump = tools / "riscv32-esp-elf-objdump.exe"
    readelf = tools / "riscv32-esp-elf-readelf.exe"
    host_cc = shutil.which(args.host_cc)
    if not all(path.is_file() for path in (compiler, objdump, readelf)) or not host_cc:
        raise FileNotFoundError("need the RISC-V GCC/binutils toolchain and native GCC; see README.md")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    assembly, object_file = output / "count_errors.rv32i.s", output / "count_errors.rv32i.o"
    flags = ["-march=rv32i", "-mabi=ilp32", "-ffreestanding", "-fno-builtin", "-O1"]
    commands = [[str(compiler), *flags, "-S", str(ROOT / "count_errors.c"), "-o", str(assembly)],
                [str(compiler), *flags, "-c", str(ROOT / "count_errors.c"), "-o", str(object_file)],
                [str(objdump), "-dr", "-M", "no-aliases", str(object_file)],
                [str(readelf), "-h", "-A", str(object_file)]]
    run(commands[0], output)
    run(commands[1], output)
    disassembly, elf = run(commands[2], output), run(commands[3], output)
    (output / "count_errors.disasm.txt").write_text(disassembly)
    (output / "count_errors.elf.txt").write_text(elf)
    if "Machine:" not in elf or "RISC-V" not in elf or '"rv32i2p1"' not in elf:
        raise ValueError("compiler did not produce the expected RV32I ELF object")
    for instruction in (r"\blw\b", r"\bsw\b", r"\b(?:beq|bne|bltu|bgeu)\b"):
        if not re.search(instruction, disassembly):
            raise ValueError(f"expected instruction class missing: {instruction}")
    host_test = output / "count-errors-host-test.exe"
    host_command = [host_cc, "-std=c99", "-Wall", "-Wextra", "-Werror", str(ROOT / "count_errors.c"),
                    str(ROOT / "test_count_errors.c"), "-o", str(host_test)]
    run(host_command, output)
    run([str(host_test)], output)
    print("PASS: RV32I ELF load/store/branch and native C boundary tests (RV32I execution not tested)")


if __name__ == "__main__":
    main()
