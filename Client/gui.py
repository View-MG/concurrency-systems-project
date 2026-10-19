"""Native monitor for the real C++ Client/Broker/Worker processes.

The GUI uses client.exe's stdin/stdout. Named Pipe framing, request correlation,
and overlapped I/O remain in C++; no Tk calls occur on background threads.
"""
from __future__ import annotations

from collections import OrderedDict, deque
from contextlib import suppress
from dataclasses import dataclass
import json
from pathlib import Path
import queue
import subprocess
import threading
import time
import tkinter as tk
from tkinter import filedialog, ttk
import uuid


ROOT = Path(__file__).resolve().parents[1]
TERMINAL = {"Completed", "Failed", "Cancelled"}
BG = "#101722"
PANEL = "#192331"
TEXT = "#EAF0F8"
MUTED = "#A5B3C5"
ACCENT = "#64D8B0"
BLUE = "#73B8F3"
AMBER = "#F1BD72"
RED = "#F58F98"


@dataclass
class ChunkSpan:
    job_id: int
    chunk_id: int
    worker_id: int
    assigned: float | None = None
    returned: float | None = None
    compute_started: float | None = None
    compute_finished: float | None = None
    disconnected: float | None = None


class MonitorModel:
    """Bounded presentation state, independent of Tk and process execution."""

    def __init__(self):
        self.jobs: dict[int, dict] = {}
        self.workers: dict[int, dict] = {}
        self.ready_jobs: list[int] = []
        self.results: dict[int, dict] = {}
        self.policy = "fifo"
        self.events: deque[dict] = deque(maxlen=2000)
        self.spans: OrderedDict[tuple[int, int, int], ChunkSpan] = OrderedDict()
        self.origin: float | None = None
        self.latest: float | None = None
        self.version = 0

    def add_log(self, source, event, detail="", **fields):
        self.events.append({"source": source, "event": event, "detail": str(detail), **fields})
        self.version += 1

    def apply_response(self, value):
        kind = value.get("type", value.get("event", "message"))
        monitor = value.get("monitor")
        if isinstance(monitor, dict):
            if isinstance(monitor.get("jobs"), list):
                if kind == "STATUS":
                    retained = {int(j["job_id"]) for j in monitor["jobs"]
                                if isinstance(j, dict) and j.get("job_id")}
                    self.jobs = {jid: job for jid, job in self.jobs.items() if jid in retained}
                    self.results = {jid: result for jid, result in self.results.items() if jid in retained}
                for job in monitor["jobs"]:
                    if isinstance(job, dict) and job.get("job_id"):
                        jid = int(job["job_id"])
                        self.jobs[jid] = {**self.jobs.get(jid, {}), **job}
            if isinstance(monitor.get("workers"), list):
                self.workers = {int(w["worker_id"]): dict(w) for w in monitor["workers"]
                                if isinstance(w, dict) and w.get("worker_id")}
            if isinstance(monitor.get("ready_jobs"), list):
                self.ready_jobs = [int(j) for j in monitor["ready_jobs"]]
            self.policy = str(monitor.get("policy", self.policy))
        jid = int(value.get("job_id", 0) or 0)
        if jid and kind == "ACCEPTED":
            job = self.jobs.setdefault(jid, {"job_id": jid, "state": "Queued"})
            job["request_id"] = value.get("request_id", 0)
        if jid and kind in {"ACCEPTED", "STATUS", "PROGRESS", "CANCEL_ACK", "CANCELLED"}:
            job = self.jobs.setdefault(jid, {"job_id": jid})
            if "job_state" in value:
                job["state"] = value["job_state"]
            for field in ("total_chunks", "completed_chunks", "in_flight", "pending_chunks"):
                if field in value:
                    job[field] = value[field]
        if jid and kind in {"COMPLETED", "FAILED", "CANCELLED"}:
            job = self.jobs.setdefault(jid, {"job_id": jid})
            job["state"] = {"COMPLETED": "Completed", "FAILED": "Failed", "CANCELLED": "Cancelled"}[kind]
            if kind == "COMPLETED":
                self.results[jid] = dict(value.get("summary", {}))
                job["completed_chunks"] = job.get("total_chunks", job.get("completed_chunks", 0))
                job["in_flight"] = 0
                for field in ("valid_records", "malformed_records"):
                    if field in self.results[jid]:
                        job[field] = self.results[jid][field]
        if jid and self.jobs.get(jid, {}).get("state") in TERMINAL | {"Cancelling"}:
            self.jobs[jid]["pending_chunks"] = 0
            self.ready_jobs = [other for other in self.ready_jobs if other != jid]
        while len(self.jobs) > 256:
            oldest = next(jid for jid, job in self.jobs.items() if job.get("state") in TERMINAL)
            self.jobs.pop(oldest)
            self.results.pop(oldest, None)
        self.add_log("Client", str(kind), value.get("reason", ""), job_id=jid,
                     chunk_id=value.get("chunk_id", 0), worker_id=value.get("worker_id", 0))

    def apply_trace(self, value, filename=""):
        frequency = value.get("qpc_frequency", 0)
        stamp = value.get("qpc_ticks")
        seconds = stamp / frequency if stamp is not None and frequency else None
        if seconds is not None:
            self.origin = seconds if self.origin is None else min(self.origin, seconds)
            self.latest = seconds if self.latest is None else max(self.latest, seconds)
        jid = int(value.get("job_id", 0) or 0)
        cid = int(value.get("chunk_id", 0) or 0)
        wid = int(value.get("worker_id", 0) or 0)
        event = str(value.get("event", "trace"))
        if seconds is not None and jid and cid and wid and event in {
                "task_assigned", "result_received", "compute_started", "compute_finished", "compute_failed", "worker_disconnected"}:
            key = (jid, cid, wid)
            span = self.spans.setdefault(key, ChunkSpan(jid, cid, wid))
            attr = {"task_assigned": "assigned", "result_received": "returned",
                    "compute_started": "compute_started", "compute_finished": "compute_finished",
                    "compute_failed": "compute_finished", "worker_disconnected": "disconnected"}[event]
            # A duplicate Result must not rewrite the first measured boundary.
            if getattr(span, attr) is None:
                setattr(span, attr, seconds)
            while len(self.spans) > 2048:
                self.spans.popitem(last=False)
        self.add_log(value.get("component", filename or "Trace"), event, value.get("detail", ""),
                     time=seconds, job_id=jid, chunk_id=cid, worker_id=wid)

    def progress(self, job):
        total = int(job.get("total_chunks", 0) or 0)
        completed = int(job.get("completed_chunks", 0) or 0)
        return completed, total, min(100, completed * 100 / total) if total else 0


class TraceTailer:
    """Read append-only JSONL files without losing a partial final line."""

    def __init__(self, directory, emit):
        self.directory = Path(directory)
        self.emit = emit
        self.offsets = {}
        self.pending = {}

    def poll(self):
        for path in sorted(self.directory.glob("*.jsonl")):
            try:
                offset = self.offsets.get(path, 0)
                if path.stat().st_size < offset:
                    offset = 0
                    self.pending.pop(path, None)
                with path.open("rb") as stream:
                    stream.seek(offset)
                    content = stream.read(256 * 1024)
                    self.offsets[path] = stream.tell()
                if not content:
                    continue
                lines = (self.pending.get(path, b"") + content).split(b"\n")
                self.pending[path] = lines.pop()
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line.decode("utf-8"))
                        if isinstance(event, dict):
                            self.emit({"kind": "trace", "payload": event, "filename": path.name})
                    except (ValueError, UnicodeDecodeError) as error:
                        self.emit({"kind": "log", "source": "Trace", "text": f"{path.name}: {error}"})
            except OSError:
                # A just-created file can be temporarily unavailable; retry later.
                continue

# เปิดและปิด Process รวมถึงส่งคำสั่งผ่าน C++ Client
class ClusterController:
    """Own only the child processes launched for this GUI session."""

    def __init__(self, events, project_root=ROOT):
        self.events = events
        self.root = Path(project_root)
        self.processes = {}
        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.stop_requested = threading.Event()
        self.stopped = threading.Event()
        self.directory = None
        self.session = ""
        self.controller_id = uuid.uuid4().hex

    def emit(self, value):
        self.events.put({**value, "controller_id": self.controller_id})

    def log(self, source, text):
        self.emit({"kind": "log", "source": source, "text": text})

    def _spawn(self, role, args, client=False):
        with self.lock:
            if self.stop_requested.is_set():
                raise RuntimeError("Session is stopping")
            process = subprocess.Popen(args, cwd=self.root,
                                       stdin=subprocess.PIPE if client else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       text=True, encoding="utf-8", errors="replace", bufsize=1,
                                       creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            self.processes[role] = process
        threading.Thread(target=self._read, args=(role, process), daemon=True).start()
        return process

    def _read(self, role, process):
        try:
            for line in process.stdout:
                text = line.rstrip()
                try:
                    value = json.loads(text) if role == "client" else None
                except ValueError:
                    value = None
                if isinstance(value, dict):
                    self.emit({"kind": "response", "payload": value})
                elif text:
                    self.log(role, text)
        except (OSError, ValueError) as error:
            self.log(role, str(error))
        finally:
            code = process.wait()
            process.stdout.close()
            if role.startswith("prepare-"):
                with self.lock:
                    self.processes.pop(role, None)
            self.emit({"kind": "process_exit", "role": role, "code": code})

    def start(self, workers=3, policy="fifo", capacity=32):
        try:
            for executable in ("broker", "worker", "client", "prepare"):
                if not (self.root / "build" / f"{executable}.exe").is_file():
                    raise RuntimeError(f"Missing build/{executable}.exe; run scripts/build.ps1")
            self.session = "gui-" + uuid.uuid4().hex[:12]
            self.directory = self.root / "build" / "traces" / self.session
            self.directory.mkdir(parents=True, exist_ok=False)
            common = ["--session", self.session, "--trace", str(self.directory)]
            self._spawn("broker", [str(self.root / "build/broker.exe"), *common, "--workers", str(workers),
                                   "--policy", policy, "--capacity", str(capacity)])
            deadline = time.monotonic() + 5
            while not (self.directory / "broker.jsonl").exists():
                if self.stop_requested.is_set():
                    raise RuntimeError("Session is stopping")
                if time.monotonic() > deadline or self.processes["broker"].poll() is not None:
                    raise RuntimeError("Broker did not start; see the Event Log")
                time.sleep(0.02)
            tailer = TraceTailer(self.directory, self.emit)
            threading.Thread(target=self._tail, args=(tailer,), daemon=True).start()
            for wid in range(1, workers + 1):
                self._spawn(f"worker{wid}", [str(self.root / "build/worker.exe"), *common, "--id", str(wid)])
            self._spawn("client", [str(self.root / "build/client.exe"), *common], client=True)
            self.emit({"kind": "started", "directory": str(self.directory), "session": self.session})
        except Exception as error:
            self.emit({"kind": "error", "text": str(error)})
            self.stop_requested.set()
            self._terminate_owned()
            self.stopped.set()
            self.emit({"kind": "stopped"})

    def _tail(self, tailer):
        while not self.stopped.wait(0.12):
            tailer.poll()
        tailer.poll()

    def send(self, command):
        if "\n" in command or "\r" in command:
            raise ValueError("A command must fit on one line")
        with self.write_lock:
            with self.lock:
                client = self.processes.get("client")
            if client is None or client.poll() is not None:
                raise RuntimeError("Client is not running")
            client.stdin.write(command + "\n")
            client.stdin.flush()

    def prepare_submit(self, filename, analysis, chunk_bytes):
        success = False
        try:
            path = Path(filename).resolve()
            if any(c in str(path) for c in ('"', "\n", "\r")):
                raise ValueError("Unsupported character in file path")
            if not path.is_file():
                raise ValueError("Choose an existing CSV file")
            if analysis not in ("route", "time") or chunk_bytes < 1:
                raise ValueError("Invalid analysis or chunk size")
            self.log("Prepare", f"Indexing {path.name} · {chunk_bytes:,} bytes/chunk")
            role = "prepare-" + uuid.uuid4().hex[:6]
            process = self._spawn(role, [str(self.root / "build/prepare.exe"), "--file", str(path),
                                          "--chunk-bytes", str(chunk_bytes)])
            if process.wait() != 0:
                raise RuntimeError("Input preparation failed; see the Event Log")
            if self.stop_requested.is_set():
                return
            self.send(f'submit {analysis} "{path}"')
            self.log("GUI", f"Submitted {analysis}: {path.name}")
            success = True
        except Exception as error:
            self.emit({"kind": "error", "text": str(error)})
        finally:
            self.emit({"kind": "prepared", "success": success})

    def _terminate_owned(self):
        with self.lock:
            processes = list(self.processes.values())
        for process in processes:
            if process.poll() is None:
                try:
                    process.terminate()
                except OSError:
                    pass
        for process in processes:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
            with self.write_lock:
                if process.stdin:
                    process.stdin.close()

    def stop(self, job_ids=()):
        self.stop_requested.set()
        try:
            with suppress(OSError, RuntimeError, ValueError):
                for jid in job_ids:
                    self.send(f"cancel {jid}")
                self.send("quit")
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                with self.lock:
                    running = [p for p in self.processes.values() if p.poll() is None]
                if not running:
                    break
                time.sleep(0.05)
        finally:
            with self.lock:
                force = any(p.poll() is None for p in self.processes.values())
            if force:
                self.log("GUI", "Graceful stop timed out; terminating this session's child processes")
            self._terminate_owned()
            self.stopped.set()
            self.emit({"kind": "stopped"})


class MonitorWindow:
    def __init__(self, root):
        self.root = root
        self.root.title("Log Analyzer — Process Monitor")
        self.root.geometry("1400x820")
        self.root.minsize(1120, 800)
        self.root.configure(bg=BG)
        self.events = queue.Queue(maxsize=4096)
        self.model = MonitorModel()
        self.controller = None
        self.running = False
        self.preparing = False
        self.closing = False
        self.last_version = -1
        self.last_snapshot = 0.0
        self.selected_result = None
        self._style()
        self._layout()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.after(80, self._poll)

    def _style(self):
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure(".", background=BG, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("TFrame", background=BG)
        style.configure("Panel.TFrame", background=PANEL)
        style.configure("TLabel", background=BG, foreground=TEXT)
        style.configure("Muted.TLabel", foreground=MUTED)
        style.configure("Title.TLabel", font=("Segoe UI", 24, "bold"))
        style.configure("Small.TLabel", foreground=MUTED, font=("Segoe UI", 9))
        style.configure("Card.TLabel", background=PANEL, foreground=MUTED, font=("Segoe UI", 9))
        style.configure("Number.TLabel", background=PANEL, foreground=TEXT, font=("Segoe UI", 24, "bold"))
        style.configure("TButton", background="#2A394B", foreground=TEXT, borderwidth=0, padding=(14, 8))
        style.configure("TCheckbutton", background=BG, foreground=TEXT)
        style.map("TButton", background=[("active", "#3A5069"), ("disabled", "#202C3A")],
                  foreground=[("disabled", "#748398")])
        style.configure("Accent.TButton", background=ACCENT, foreground="#0F3027", font=("Segoe UI", 10, "bold"))
        style.map("Accent.TButton", background=[("active", "#8DE7C9"), ("disabled", "#244A43")])
        style.configure("TEntry", fieldbackground=PANEL, foreground=TEXT, bordercolor="#344358", padding=7)
        style.configure("TCombobox", fieldbackground=PANEL, foreground=TEXT, background="#2A394B", padding=5)
        style.map("TCombobox", fieldbackground=[("readonly", PANEL)], foreground=[("readonly", TEXT)])
        style.configure("TSpinbox", fieldbackground=PANEL, foreground=TEXT, background="#2A394B", padding=5)
        style.configure("Treeview", background=PANEL, fieldbackground=PANEL, foreground=TEXT,
                        rowheight=29, borderwidth=0, font=("Segoe UI", 10))
        style.configure("Treeview.Heading", background="#223044", foreground=MUTED,
                        font=("Segoe UI", 9, "bold"), padding=7, relief="flat")
        style.map("Treeview", background=[("selected", "#31506B")], foreground=[("selected", TEXT)])
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure("TNotebook.Tab", background="#1D2939", foreground=MUTED, padding=(18, 8))
        style.map("TNotebook.Tab", background=[("selected", "#2A3B50")], foreground=[("selected", TEXT)])
        style.configure("TPanedwindow", background=BG)
        self.root.option_add("*TCombobox*Listbox.background", PANEL)
        self.root.option_add("*TCombobox*Listbox.foreground", TEXT)

    def _layout(self):
        shell = ttk.Frame(self.root, padding=(28, 20, 28, 14))
        shell.pack(fill="both", expand=True)
        header = ttk.Frame(shell)
        header.pack(fill="x")
        ttk.Label(header, text="Log Analyzer", style="Title.TLabel").pack(side="left")
        ttk.Label(header, text="  /  PROCESS MONITOR", style="Muted.TLabel").pack(side="left", pady=(10, 0))
        self.status = tk.StringVar(value="●  OFFLINE")
        self.status_label = ttk.Label(header, textvariable=self.status, foreground=MUTED, font=("Segoe UI", 10, "bold"))
        self.status_label.pack(side="right", pady=(10, 0))
        ttk.Label(shell, text="ส่งงาน • ติดตามคิวและ Worker • ตรวจผลจาก Process จริง", style="Muted.TLabel").pack(anchor="w", pady=(4, 16))

        config = ttk.Frame(shell)
        config.pack(fill="x")
        self.worker_count = tk.StringVar(value="3")
        self.policy = tk.StringVar(value="Round-robin")
        self.chunk_kib = tk.StringVar(value="64")
        self.capacity = tk.StringVar(value="32")
        self.config_widgets = []
        for label, variable, width, choices in (
                ("WORKERS", self.worker_count, 6, None), ("POLICY", self.policy, 15, ("FIFO", "Round-robin")),
                ("CHUNK / KiB", self.chunk_kib, 9, None), ("CAPACITY", self.capacity, 7, None)):
            cell = ttk.Frame(config)
            cell.pack(side="left", padx=(0, 14))
            ttk.Label(cell, text=label, style="Small.TLabel").pack(anchor="w", pady=(0, 4))
            if choices:
                widget = ttk.Combobox(cell, textvariable=variable, values=choices, state="readonly", width=width)
            else:
                upper = 8 if label == "WORKERS" else 65536 if label == "CHUNK / KiB" else 128
                widget = ttk.Spinbox(cell, textvariable=variable, from_=1, to=upper, width=width)
            widget.pack()
            self.config_widgets.append(widget)
        actions = ttk.Frame(config)
        actions.pack(side="right", anchor="s")
        self.start_button = ttk.Button(actions, text="Start cluster", style="Accent.TButton", command=self.start)
        self.start_button.pack(side="left", padx=(0, 8))
        self.stop_button = ttk.Button(actions, text="Stop", command=self.stop, state="disabled")
        self.stop_button.pack(side="left")

        cards = ttk.Frame(shell)
        cards.pack(fill="x", pady=(18, 16))
        self.metrics = {}
        for column, (key, label) in enumerate((("jobs", "ADMITTED JOBS"), ("active", "ACTIVE / QUEUED"),
                                                ("workers", "BUSY WORKERS"), ("records", "VALID / MALFORMED"))):
            cards.columnconfigure(column, weight=1, uniform="card")
            card = ttk.Frame(cards, style="Panel.TFrame", padding=(16, 10))
            card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 0))
            ttk.Label(card, text=label, style="Card.TLabel").pack(anchor="w")
            self.metrics[key] = tk.StringVar(value="0 / 0" if key != "jobs" else "0")
            ttk.Label(card, textvariable=self.metrics[key], style="Number.TLabel").pack(anchor="w", pady=(3, 0))

        submit = ttk.Frame(shell)
        submit.pack(fill="x", pady=(0, 12))
        self.file_path = tk.StringVar(value=str(ROOT / "data/small.csv"))
        ttk.Entry(submit, textvariable=self.file_path).pack(side="left", fill="x", expand=True)
        ttk.Button(submit, text="Browse…", command=self.browse).pack(side="left", padx=8)
        self.analysis = tk.StringVar(value="route")
        ttk.Combobox(submit, textvariable=self.analysis, values=("route", "time"), width=9, state="readonly").pack(side="left")
        self.submit_button = ttk.Button(submit, text="Prepare & submit", style="Accent.TButton", command=self.submit, state="disabled")
        self.submit_button.pack(side="left", padx=(8, 0))
        self.hint = tk.StringVar(value="ตั้งค่าก่อน Start • เตรียม index ใน background แล้วส่งงานผ่าน C++ Client")
        ttk.Label(shell, textvariable=self.hint, style="Small.TLabel").pack(anchor="w", pady=(0, 12))

        body = ttk.Panedwindow(shell, orient="vertical")
        body.pack(fill="both", expand=True)
        middle = ttk.Panedwindow(body, orient="horizontal")
        body.add(middle, weight=1)
        job_panel = ttk.Frame(middle)
        worker_panel = ttk.Frame(middle)
        middle.add(job_panel, weight=4)
        middle.add(worker_panel, weight=2)
        title = ttk.Frame(job_panel)
        title.pack(fill="x", pady=(0, 8))
        ttk.Label(title, text="Jobs", font=("Segoe UI", 12, "bold")).pack(side="left")
        self.cancel_button = ttk.Button(title, text="Cancel selected", command=self.cancel_selected, state="disabled")
        self.cancel_button.pack(side="right")
        self.refresh_button = ttk.Button(title, text="Refresh", command=self.refresh, state="disabled")
        self.refresh_button.pack(side="right", padx=6)
        self.jobs = self._tree(job_panel, ("job", "analysis", "state", "progress", "pending", "flight", "file"),
                               ("JOB", "ANALYSIS", "STATE", "PROGRESS", "WAIT", "IN-FLIGHT", "FILE"),
                               (55, 75, 95, 105, 50, 80, 180), height=5)
        self.jobs.bind("<<TreeviewSelect>>", lambda _: self._render())
        for state, color in (("Completed", ACCENT), ("Active", BLUE), ("Queued", TEXT),
                             ("Cancelling", AMBER), ("Cancelled", AMBER), ("Failed", RED)):
            self.jobs.tag_configure(state, foreground=color)
        self.queue_text = tk.StringVar(value="Ready-job queue  —")
        ttk.Label(job_panel, textvariable=self.queue_text, style="Small.TLabel", wraplength=720).pack(
            side="bottom", anchor="w", pady=(8, 0), before=self.jobs.master)
        ttk.Label(worker_panel, text="Workers", font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(10, 14))
        self.workers = self._tree(worker_panel, ("worker", "state", "assignment"),
                                  ("WORKER", "STATE", "CURRENT CHUNK"), (75, 95, 175), height=5)
        self.workers.tag_configure("Busy", foreground=BLUE)
        self.workers.tag_configure("Disconnected", foreground=RED)

        tabs = ttk.Notebook(body)
        body.add(tabs, weight=1)
        timeline_tab = ttk.Frame(tabs)
        results_tab = ttk.Frame(tabs)
        events_tab = ttk.Frame(tabs)
        tabs.add(timeline_tab, text="Chunk timeline")
        tabs.add(results_tab, text="Results")
        tabs.add(events_tab, text="Event log")
        ttk.Label(timeline_tab, text="ขอบสีน้ำเงิน = assignment → result  ·  แถบสีเขียว = compute  ·  เวลาอ้างอิง QPC จาก trace จริง",
                  style="Small.TLabel").pack(anchor="w", pady=(10, 4))
        timeline_frame = ttk.Frame(timeline_tab)
        timeline_frame.pack(fill="both", expand=True)
        self.timeline = tk.Canvas(timeline_frame, background=PANEL, highlightthickness=0, height=145)
        self.timeline.pack(side="left", fill="both", expand=True)
        timeline_scroll = ttk.Scrollbar(timeline_frame, orient="vertical", command=self.timeline.yview)
        timeline_scroll.pack(side="right", fill="y")
        self.timeline.configure(yscrollcommand=timeline_scroll.set)
        self.timeline.bind("<Configure>", lambda _: self._draw_timeline())
        self.result_title = tk.StringVar(value="เลือก Job เพื่อดูผลลัพธ์")
        ttk.Label(results_tab, textvariable=self.result_title, style="Muted.TLabel").pack(anchor="w", pady=10)
        self.result_tree = self._tree(results_tab, ("key", "errors", "4xx", "5xx"),
                                     ("ROUTE / TIME BUCKET", "ERRORS", "4xx", "5xx"), (540, 110, 110, 110), height=5)
        self.event_tree = self._tree(events_tab, ("time", "source", "event", "job", "chunk", "worker", "detail"),
                                    ("TIME / s", "SOURCE", "EVENT", "JOB", "CHUNK", "WORKER", "DETAIL"),
                                    (85, 85, 170, 55, 65, 70, 400), height=6)
        self.footer = tk.StringVar(value="No session  •  Native GUI → C++ Client → Named Pipes → Broker → Workers")
        ttk.Label(shell, textvariable=self.footer, style="Small.TLabel").pack(side="bottom", anchor="w", pady=(10, 0), before=body)
        def initial_split():
            if body.winfo_height() < 200:
                self.root.after(50, initial_split)
            else:
                body.sashpos(0, max(160, int(body.winfo_height() * 0.44)))
        self.root.after(50, initial_split)

    @staticmethod
    def _tree(parent, columns, labels, widths, height):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=height, selectmode="browse")
        for column, label, width in zip(columns, labels, widths):
            tree.heading(column, text=label)
            tree.column(column, width=width, minwidth=min(width, 60), anchor="w", stretch=column == columns[-1])
        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        return tree

    def browse(self):
        path = filedialog.askopenfilename(parent=self.root, title="เลือก CSV", filetypes=(("CSV files", "*.csv"), ("All files", "*.*")))
        if path:
            self.file_path.set(path)

    def start(self):
        if self.controller and not self.controller.stopped.is_set():
            return
        try:
            workers, chunk, capacity = int(self.worker_count.get()), int(self.chunk_kib.get()), int(self.capacity.get())
            if not 1 <= workers <= 8 or not 1 <= chunk <= 65536 or not 1 <= capacity <= 128:
                raise ValueError("Workers 1–8, chunk 1–65,536 KiB, capacity 1–128")
        except ValueError as error:
            self.hint.set(f"ตั้งค่าไม่ถูกต้อง: {error}")
            return
        self.model = MonitorModel()
        self.last_version = -1
        self.selected_result = None
        self.running = False
        self.controller = ClusterController(self.events)
        self.status.set("●  STARTING")
        self.status_label.configure(foreground=AMBER)
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        for widget in self.config_widgets:
            widget.configure(state="disabled")
        policy = "rr" if self.policy.get() == "Round-robin" else "fifo"
        threading.Thread(target=self.controller.start, args=(workers, policy, capacity), daemon=True).start()

    def submit(self):
        if not self.running or self.preparing:
            return
        try:
            chunk_bytes = int(self.chunk_kib.get()) * 1024
        except ValueError:
            return
        self.preparing = True
        self.submit_button.configure(state="disabled")
        self.hint.set("กำลังเตรียม index ใน background… สามารถดูสถานะและยกเลิก Job อื่นได้")
        threading.Thread(target=self.controller.prepare_submit,
                         args=(self.file_path.get(), self.analysis.get(), chunk_bytes), daemon=True).start()

    def _send(self, command):
        if not self.running:
            return
        try:
            self.controller.send(command)
        except (OSError, RuntimeError, ValueError) as error:
            self.hint.set(str(error))
            self.model.add_log("GUI", "command_error", str(error))

    def refresh(self):
        self._send("status")

    def cancel_selected(self):
        selected = self.jobs.selection()
        if selected:
            jid = int(selected[0])
            if self.model.jobs.get(jid, {}).get("state") not in TERMINAL:
                self._send(f"cancel {jid}")

    def stop(self):
        if not self.controller or self.controller.stopped.is_set() or self.controller.stop_requested.is_set():
            return
        self.running = False
        self.status.set("●  STOPPING")
        self.status_label.configure(foreground=AMBER)
        self.stop_button.configure(state="disabled")
        self.submit_button.configure(state="disabled")
        self.refresh_button.configure(state="disabled")
        self.cancel_button.configure(state="disabled")
        unfinished = [jid for jid, job in self.model.jobs.items() if job.get("state") not in TERMINAL]
        threading.Thread(target=self.controller.stop, args=(unfinished,), daemon=True).start()

    def close(self):
        self.closing = True
        if self.controller and not self.controller.stopped.is_set():
            self.stop()
        else:
            self.root.destroy()

    def _poll(self):
        for _ in range(1500):
            try:
                item = self.events.get_nowait()
            except queue.Empty:
                break
            if item.get("controller_id") and (self.controller is None or item["controller_id"] != self.controller.controller_id):
                continue
            kind = item["kind"]
            if kind == "response":
                self.model.apply_response(item["payload"])
            elif kind == "trace":
                self.model.apply_trace(item["payload"], item.get("filename", ""))
            elif kind == "log":
                self.model.add_log(item["source"], "console", item["text"])
            elif kind == "error":
                self.model.add_log("GUI", "error", item["text"])
                self.hint.set(item["text"])
            elif kind == "started":
                if self.closing or self.controller.stop_requested.is_set():
                    continue
                self.running = True
                self.status.set("●  CONNECTED")
                self.status_label.configure(foreground=ACCENT)
                for widget in (self.submit_button, self.refresh_button, self.cancel_button):
                    widget.configure(state="normal")
                self.footer.set(f"Session {item['session']}  •  {item['directory']}")
                self.hint.set("พร้อมรับงาน • เตรียม index ก่อน Submit • Status อัปเดตทุก 2 วินาที")
                self.last_snapshot = 0
            elif kind == "prepared":
                self.preparing = False
                if self.running:
                    self.submit_button.configure(state="normal")
                    if item.get("success"):
                        self.hint.set("พร้อมรับงานถัดไป • ติดตามการแจก Chunk ได้จาก Timeline และ Event log")
            elif kind == "process_exit":
                self.model.add_log(item["role"], "process_exit", f"exit code {item['code']}")
                if item["role"] in ("client", "broker") and self.running:
                    self.hint.set(f"{item['role']} exited ({item['code']}); stopping owned processes")
                    self.stop()
            elif kind == "stopped":
                self.running = False
                self.preparing = False
                for worker in self.model.workers.values():
                    worker.update(state="Disconnected", job_id=0, chunk_id=0)
                self.model.version += 1
                self.status.set("●  OFFLINE")
                self.status_label.configure(foreground=MUTED)
                self.start_button.configure(state="normal")
                self.stop_button.configure(state="disabled")
                self.submit_button.configure(state="disabled")
                self.refresh_button.configure(state="disabled")
                self.cancel_button.configure(state="disabled")
                for widget in self.config_widgets:
                    widget.configure(state="readonly" if isinstance(widget, ttk.Combobox) else "normal")
                if self.closing:
                    self.root.destroy()
                    return
        if self.running and time.monotonic() - self.last_snapshot >= 2:
            self.last_snapshot = time.monotonic()
            self.refresh()
        if self.model.version != self.last_version:
            self.last_version = self.model.version
            self._render()
        self.root.after(100, self._poll)

    def _render(self):
        jobs = self.model.jobs
        for item in self.jobs.get_children():
            if int(item) not in jobs:
                self.jobs.delete(item)
        self.metrics["jobs"].set(str(len(jobs)))
        self.metrics["active"].set(f"{sum(j.get('state') in ('Active', 'Cancelling') for j in jobs.values())} / "
                                   f"{sum(j.get('state') == 'Queued' for j in jobs.values())}")
        self.metrics["workers"].set(f"{sum(w.get('state') == 'Busy' for w in self.model.workers.values())} / {len(self.model.workers)}")
        self.metrics["records"].set(f"{sum(j.get('valid_records', 0) for j in jobs.values()):,} / "
                                   f"{sum(j.get('malformed_records', 0) for j in jobs.values()):,}")
        for jid, job in sorted(jobs.items()):
            done, total, percent = self.model.progress(job)
            state = job.get("state", "Queued")
            values = (jid, job.get("analysis", "—"), state, f"{done}/{total} · {percent:.0f}%" if total else "—",
                      job.get("pending_chunks", "—"), job.get("in_flight", "—"), Path(job.get("path", "")).name)
            if self.jobs.exists(str(jid)):
                self.jobs.item(str(jid), values=values, tags=(state,))
            else:
                self.jobs.insert("", "end", iid=str(jid), values=values, tags=(state,))
        self.workers.delete(*self.workers.get_children())
        for wid, worker in sorted(self.model.workers.items()):
            assignment = f"Job {worker.get('job_id', 0)} / Chunk {worker.get('chunk_id', 0)}" if worker.get("job_id") else "—"
            self.workers.insert("", "end", values=(f"W{wid}", worker.get("state", "—"), assignment), tags=(worker.get("state", ""),))
        ready = self.model.ready_jobs
        suffix = f"  … (+{len(ready) - 24} Jobs)" if len(ready) > 24 else ""
        self.queue_text.set(f"Ready-job queue ({len(ready)})  " + ("  →  ".join(f"Job {jid}" for jid in ready[:24]) or "—") + suffix)
        self.event_tree.delete(*self.event_tree.get_children())
        for event in list(self.model.events)[-300:]:
            stamp = event.get("time")
            relative = f"{stamp - self.model.origin:.3f}" if stamp is not None and self.model.origin is not None else "—"
            self.event_tree.insert("", "end", values=(relative, event["source"], event["event"],
                                                       event.get("job_id") or "—", event.get("chunk_id") or "—",
                                                       event.get("worker_id") or "—", event.get("detail", "")))
        children = self.event_tree.get_children()
        if children:
            self.event_tree.see(children[-1])
        self._show_result()
        self._draw_timeline()

    def _show_result(self):
        selected = self.jobs.selection()
        jid = int(selected[0]) if selected else None
        result = self.model.results.get(jid)
        # Completed summaries are replaced as a whole; no JSON serialization per repaint.
        signature = (jid, result)
        if signature == self.selected_result:
            return
        self.selected_result = signature
        self.result_tree.delete(*self.result_tree.get_children())
        if result is None:
            self.result_title.set(f"Job {jid} · ยังไม่มีผลสำเร็จ" if jid else "เลือก Job เพื่อดูผลลัพธ์")
            return
        self.result_title.set(f"Job {jid}  ·  Valid {result.get('valid_records', 0):,}  ·  Malformed {result.get('malformed_records', 0):,}")
        for key, counts in sorted(result.get("groups", {}).items()):
            self.result_tree.insert("", "end", values=(key, counts.get("errors", 0), counts.get("4xx", 0), counts.get("5xx", 0)))

    def _draw_timeline(self):
        canvas = self.timeline
        canvas.delete("all")
        width, height = canvas.winfo_width(), canvas.winfo_height()
        spans = [span for span in self.model.spans.values() if span.assigned is not None]
        if not spans or width < 120:
            canvas.configure(scrollregion=(0, 0, width, height))
            canvas.create_text(24, 36, text="รอ task_assigned จาก Broker trace…", fill=MUTED, anchor="w", font=("Segoe UI", 11))
            canvas.create_text(24, 62, text="Timeline จะแสดงเฉพาะเหตุการณ์จริงของ session นี้", fill=MUTED, anchor="w", font=("Segoe UI", 9))
            return
        spans = spans[-160:]
        workers = sorted({s.worker_id for s in spans} | set(self.model.workers))
        height = max(height, len(workers) * 32 + 58)
        canvas.configure(scrollregion=(0, 0, width, height))
        origin = min(s.assigned for s in spans)
        boundaries = [s.returned or s.disconnected or s.compute_finished or s.compute_started or s.assigned for s in spans]
        if any(s.returned is None and s.disconnected is None for s in spans):
            boundaries.append(self.model.latest or origin)
        latest = max(boundaries)
        duration = max(latest - origin, 0.001)
        left, right, top = 68, max(120, width - 24), 30
        row = max(28, min(48, (height - 58) / max(1, len(workers))))
        x = lambda stamp: left + (stamp - origin) / duration * (right - left)
        for index in range(6):
            px = left + index * (right - left) / 5
            canvas.create_line(px, top - 8, px, height - 17, fill="#2B394C")
            canvas.create_text(px, 12, text=f"{index * duration / 5:.3f}s", fill=MUTED, font=("Segoe UI", 8))
        for i, wid in enumerate(workers):
            y = top + i * row + row / 2
            canvas.create_text(18, y, text=f"W{wid}", fill=TEXT, anchor="w", font=("Segoe UI", 10, "bold"))
            canvas.create_line(left, y + row / 2, right, y + row / 2, fill="#263447")
        for span in spans:
            y = top + workers.index(span.worker_id) * row + row / 2
            settled = span.returned is not None or span.disconnected is not None
            end = span.returned if span.returned is not None else span.disconnected if span.disconnected is not None else latest
            a, b = x(span.assigned), max(x(end), x(span.assigned) + 2)
            canvas.create_rectangle(a, y - 10, b, y + 10, outline=RED if span.disconnected is not None else BLUE if settled else AMBER,
                                    width=1, dash=() if settled else (3, 2))
            if span.compute_started is not None and span.compute_finished is not None:
                canvas.create_rectangle(x(span.compute_started), y - 7, max(x(span.compute_finished), x(span.compute_started) + 2),
                                        y + 7, fill="#308D76", outline="")
            if b - a > 72:
                canvas.create_text((a + b) / 2, y, text=f"J{span.job_id} · C{span.chunk_id}", fill=TEXT, font=("Segoe UI", 8))
        canvas.create_text(left, height - 7, text="Last 160 observed chunks · open outlines end at the latest observed trace event",
                           anchor="w", fill=MUTED, font=("Segoe UI", 8))


def main():
    root = tk.Tk()
    MonitorWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
