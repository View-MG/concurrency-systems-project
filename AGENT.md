# Project working guide

Read this file before editing. Respond in Thai by default and preserve English identifiers.
[README.md](README.md) is the single current setup/run/test/contract guide.
[Full_Context.md](Full_Context.md) is the original project brief, retained as source material.
The user's current cleanup request overrides historical evidence/packaging requirements in that brief.

## Scope

Asynchronous Multi-Process Log Analyzer has two required parts:
- Real C++17 Windows Client/Broker/Workers communicating through overlapped Named Pipes,
  Python/Tk monitor, prepared record-boundary chunks, serial reference and seeded experiments.
- Course MOS/Rz/S2 bounded-buffer experiment with actual simulator observation and validation.
  Keep the separate small RV32I compile/disassembly/native-semantics example.

No remote workers, reconnect/retry/restart, adaptive scheduling, hardware integrations,
presentation-only GUI controls, report/plot framework or release/archive pipeline.
Windows schedules CPU execution; Broker schedules application chunks.
I/O completion is not an ISR; direct interrupt/context-switch study belongs to MOS.

## Ownership and invariants

One Broker event loop owns job/Worker state, ready queue, assignments and aggregation.
Worker has at most one assigned chunk; Client has only a blocking stdin helper thread.
GUI background threads pass events to Tk's main thread.

1. Every physical record belongs to exactly one record-aligned chunk.
2. Each Worker owns at most one assignment.
3. Results match request/job/chunk/Worker identity before release or merge.
4. Merge each chunk at most once.
5. Completed requires all validated chunks.
6. Cancelled/Failed cannot become Completed via late results.
7. Release Workers and input locks only after actual assignment settlement.

Cancel stops new assignments and settles running chunks; CancelIoEx cancels I/O only.
Worker loss fails affected work while other work continues.
Client loss cancels its work and settles assignments.
Keep RAII handles, separate read/write OVERLAPPED and stable buffers until completion is collected.
Do not synchronously scan large inputs during Broker admission; Prepare performs indexing first.
Retain bounds, overflow/count checks, framing/UTF-8 checks and explicit final-delivery failure.

## Validation

- Build: `./scripts/build.ps1` (GCC, warnings as errors).
- Full tests: `./scripts/test.ps1` (requires course tools and the documented RV32I toolchain).
- Windows acceptance: `python tests/processes.py`.
- GUI and measurement tests, MOS tests and individual commands are documented in README.
- Experiments: `python scripts/experiments.py`; use `--repeats 1` for a full-configuration smoke run.
- Run relevant tests after changing code. Test evidence is fresh console output, not a retained old report.
- Preserve independent expected fixtures and oracle checks, cancellation/failure/late-result tests,
  history/handle bounds and original-scheduler/corrupted-trace negative tests.
- Distinguish QPC admission/queue/turnaround/response boundaries, assignment busy time and CPU time.
  Synthetic delays test responsiveness; they do not measure real parser throughput.

## Keep the source tree minimal

Generated files belong under build/ (except Prepare sidecars beside the selected input).
Do not retain historical traces, logs, evidence, reports, archives, backups or generated binaries.
Keep only the current build/run/test sources and fixtures. Do not add optional dependencies.
Do not restore phase-specific scripts/documents or create new design layers without a real need.
Keep original course sources/tool binaries: they are required inputs with provenance in README.
Do not edit course int0(); build adapters modify generated copies, and nextp() is a separate fix.
