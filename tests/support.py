"""Owned Windows processes, live traces and independent test-data helpers."""
import ctypes
import json
from pathlib import Path
import queue
import struct
import subprocess
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"


def check(condition, message):
    if not condition:
        raise AssertionError(message)


class Cluster:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __init__(self, label, delay=0, real_worker=True, workers=1, policy="fifo", capacity=32, io_mode="async", worker_delays=None):
        self.directory = BUILD / "traces" / f"test-{label}-{uuid.uuid4().hex[:8]}"
        self.session = self.directory.name
        self.processes = {}
        self.logs = {}
        self.events = queue.Queue()
        self.saved = []
        try:
            self.start("broker", ["--workers", str(workers), "--policy", policy, "--capacity", str(capacity), "--io-mode", io_mode])
            deadline = time.monotonic() + 5
            while not (self.directory / "broker.jsonl").exists():
                check(time.monotonic() < deadline, "Broker startup timed out")
                check(self.processes["broker"].poll() is None, "Broker failed at startup")
                time.sleep(0.01)
            if real_worker:
                for worker_id in range(1, workers + 1):
                    role = "worker" if worker_id == 1 else f"worker{worker_id}"
                    worker_delay = (worker_delays or {}).get(worker_id, delay)
                    self.start(role, ["--id", str(worker_id), "--delay-ms", str(worker_delay)])
            self.start("client", client=True)
            self.reader = threading.Thread(target=self.read, daemon=True)
            self.reader.start()
        except BaseException:
            self.close()
            raise

    def start(self, role, extra=None, client=False):
        self.directory.mkdir(parents=True, exist_ok=True)
        executable = "worker" if role.startswith("worker") else role
        args = [str(BUILD / f"{executable}.exe"), "--session", self.session, "--trace", str(self.directory)] + (extra or [])
        if client:
            output = subprocess.PIPE
        else:
            output = open(self.directory / f"{role}.console.txt", "w", encoding="utf-8")
            self.logs[role] = output
        self.processes[role] = subprocess.Popen(
            args, cwd=ROOT, stdin=subprocess.PIPE if client else subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, text=True, encoding="utf-8",
            creationflags=subprocess.CREATE_NO_WINDOW,
        )

    def read(self):
        with open(self.directory / "client.console.txt", "w", encoding="utf-8") as log:
            for line in self.processes["client"].stdout:
                log.write(line)
                log.flush()
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self.events.put(value)
        self.events.put({"event": "client_exited"})

    def send(self, command):
        client = self.processes["client"]
        check(client.poll() is None, "Client exited before command")
        client.stdin.write(command + "\n")
        client.stdin.flush()

    def receive(self, kind, request=None, timeout=8):
        def matches(value):
            kinds = (kind,) if isinstance(kind, str) else kind
            return value.get("type") in kinds and "event" not in value and (request is None or value.get("request_id") == request)
        for i, value in enumerate(self.saved):
            if matches(value):
                return self.saved.pop(i)
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            check(remaining > 0, f"Timeout waiting for {kind}; trace: {self.directory}")
            try:
                value = self.events.get(timeout=remaining)
            except queue.Empty:
                raise AssertionError(f"Timeout waiting for {kind}; trace: {self.directory}") from None
            check(value.get("event") != "client_exited", f"Client exited waiting for {kind}; trace: {self.directory}")
            if matches(value):
                return value
            self.saved.append(value)

    def shutdown(self, request):
        self.send("shutdown")
        self.receive("SHUTDOWN_ACK", request)
        for role, process in self.processes.items():
            check(process.wait(timeout=5) == 0, f"{role} did not exit cleanly: {self.directory}")
        self.reader.join(timeout=2)

    def close(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if hasattr(self, "reader"):
            self.reader.join(timeout=2)
        for process in self.processes.values():
            for stream in (process.stdin, process.stdout):
                if stream:
                    stream.close()
        for log in self.logs.values():
            log.close()


def traces(directory, role):
    # A live writer may not have finished its final JSONL record yet.
    return [json.loads(line) for line in (directory / f"{role}.jsonl").read_bytes().split(b"\n")[:-1]]


def process_usage(process):
    """Read actual CPU time and executive handle count from the owned process."""
    from ctypes import wintypes as w
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.GetProcessTimes.argtypes = [w.HANDLE, *([ctypes.POINTER(w.FILETIME)] * 4)]
    api.GetProcessHandleCount.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
    values = [w.FILETIME() for _ in range(4)]
    count = w.DWORD()
    check(api.GetProcessTimes(int(process._handle), *(ctypes.byref(v) for v in values)), "GetProcessTimes failed")
    check(api.GetProcessHandleCount(int(process._handle), ctypes.byref(count)), "GetProcessHandleCount failed")
    cpu = sum((v.dwHighDateTime << 32) | v.dwLowDateTime for v in values[2:]) / 10_000_000
    return {"cpu_seconds": cpu, "handles": count.value, "sample_seconds": time.perf_counter()}


class RawWorker:
    """Independent wire peer to test Broker assignment validation, not analyzer code."""
    def __init__(self, session):
        from ctypes import wintypes as w
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        self.api.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
        self.api.CreateFileW.restype = w.HANDLE
        self.api.WriteFile.argtypes = [w.HANDLE, w.LPCVOID, w.DWORD, ctypes.POINTER(w.DWORD), w.LPVOID]
        self.api.ReadFile.argtypes = [w.HANDLE, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD), w.LPVOID]
        self.api.CloseHandle.argtypes = [w.HANDLE]
        self.handle = self.api.CreateFileW(r"\\.\pipe\lga_" + session + "_worker", 0xC0000000, 0, None, 3, 0, None)
        check(self.handle not in (None, ctypes.c_void_p(-1).value), "Raw Worker connection failed")
        self.send(1, struct.pack("<Q", 1))

    def send(self, kind, payload):
        frame = struct.pack("<4sHHI", b"LGA1", 2, kind, len(payload)) + payload
        buf = ctypes.create_string_buffer(frame)
        n = ctypes.c_ulong()
        check(self.api.WriteFile(self.handle, buf, len(frame), ctypes.byref(n), None) and n.value == len(frame), "Raw write failed")

    def read_exact(self, size):
        output = b""
        while len(output) < size:
            buf = ctypes.create_string_buffer(size - len(output))
            n = ctypes.c_ulong()
            check(self.api.ReadFile(self.handle, buf, len(buf), ctypes.byref(n), None) and n.value, "Raw read failed")
            output += buf.raw[:n.value]
        return output

    def task(self):
        magic, version, kind, size = struct.unpack("<4sHHI", self.read_exact(12))
        check((magic, version, kind) == (b"LGA1", 2, 5), "Expected TASK")
        return struct.unpack("<QQQQ", self.read_exact(size)[:32])

    def close(self):
        if self.handle:
            self.api.CloseHandle(self.handle)
            self.handle = None


def prepare(path, size):
    value = subprocess.check_output([str(BUILD / "prepare.exe"), "--file", str(path), "--chunk-bytes", str(size)], cwd=ROOT, text=True, encoding="utf-8")
    return json.loads(value)


def oracle(path, mode):
    # Independent Python oracle for the deliberately valid generator dialect,
    # plus its explicit malformed-record marker. Never calls C++ analyzer.
    result = dict(valid_records=0, malformed_records=0, groups={})
    for line in Path(path).read_text(encoding="utf-8-sig").splitlines()[1:]:
        if line == "malformed-record":
            result["malformed_records"] += 1
            continue
        stamp, route, status = line.split(",")
        status = int(status)
        result["valid_records"] += 1
        if status >= 400:
            key = route if mode == "route" else stamp[:16] + ":00Z"
            group = result["groups"].setdefault(key, {"errors": 0, "4xx": 0, "5xx": 0})
            group["errors"] += 1
            group["4xx" if status < 500 else "5xx"] += 1
    return result


def wait_event(c, role, event, predicate=lambda e: True, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        path = c.directory / f"{role}.jsonl"
        if path.exists():
            for value in traces(c.directory, role):
                if value["event"] == event and predicate(value):
                    return value
        time.sleep(0.01)
    raise AssertionError(f"Missing {role}/{event}: {c.directory}")


