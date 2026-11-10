"""Benchmark BGSAVE: the fork pause, copy-on-write memory, and what gc.freeze() changes.

Linux only (on Windows, run it in WSL). Needs redis-server and redis-benchmark.

Run from the project root:
    python bench/rdb.py            # 3 runs per case, about 4 minutes
    python bench/rdb.py --runs 1   # quick check

Writes the report to bench/results/bgsave-fork-and-cow-<date>-<commit>.md.

Each run starts a fresh server, loads KEYS keys, starts the write load (if any)
with redis-benchmark, and sends BGSAVE. While the child runs, the benchmark
reads the child's /proc/<pid>/smaps_rollup every few milliseconds. Private_Dirty
there is memory the child no longer shares with the parent: pages that either
process wrote to after the fork, so the kernel had to copy them. Its peak is
the extra memory the save costs, the number Redis reports as its CoW size.

mini_redis runs twice: as it is, and with gc.freeze() from just before the
fork until the child is reaped (added by a small wrapper, so the server needs
no benchmark-only option). gc.freeze() moves every object into a generation
the garbage collector never visits, so a collection in either process does not
write to the GC headers of the shared objects. Real Redis runs once as a
reference.
"""

import argparse
import datetime
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from aof import KEYS, VALUE, Prober, preload, resp, stop
from run import ROOT, command_output, free_port, machine_info, wait_for_port

SERVERS = ["mini_redis", "mini_redis + gc.freeze()", "real Redis"]
LOADS = {  # name -> redis-benchmark options for the writes made during the save
    "no writes": None,
    "SET, 50 clients, pipeline 1": ["-c", "50", "-P", "1"],
}
SAMPLE_EVERY = 0.005  # seconds between reads of the child's smaps_rollup

# Runs `python -m mini_redis` with gc.freeze() from just before each fork until
# the child is reaped. child.start() is looked up on the module at call time,
# so replacing it here changes what BGSAVE does.
GC_FREEZE_SERVER = """
import gc, sys
from mini_redis.persistence import child

real_start = child.start

def start(kind, work, on_done):
    def done(ok):
        gc.unfreeze()
        on_done(ok)
    gc.freeze()
    try:
        real_start(kind, work, done)
    except BaseException:
        gc.unfreeze()
        raise

child.start = start

from mini_redis.__main__ import main
main(sys.argv[1:])
"""


def start(server: str, port: int, data_dir: Path) -> subprocess.Popen:
    if server == "real Redis":
        # No compression: mini_redis does not compress either, so save times compare.
        cmd = ["redis-server", "--port", str(port), "--dir", str(data_dir), "--save", "",
               "--appendonly", "no", "--rdbcompression", "no"]
        cwd = data_dir
    else:
        flags = ["--port", str(port), "--rdb-path", str(data_dir / "dump.rdb")]
        if server == "mini_redis":
            cmd = [sys.executable, "-m", "mini_redis", *flags]
        else:
            cmd = [sys.executable, "-c", GC_FREEZE_SERVER, *flags]
        cwd = ROOT
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_port(proc, port)
    return proc


# ---------------------------------------------------------------------------
# /proc readers
# ---------------------------------------------------------------------------

def rss_mb(pid: int) -> float:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    raise RuntimeError(f"no VmRSS for {pid}")


def private_dirty_mb(pid: int) -> float | None:
    """The process's Private_Dirty, or None once it has exited (a zombie has no memory left)."""
    try:
        text = Path(f"/proc/{pid}/smaps_rollup").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    for line in text.splitlines():
        if line.startswith("Private_Dirty:"):
            return int(line.split()[1]) / 1024
    return None


def child_pids(pid: int) -> list[int]:
    """Process IDs whose parent is pid."""
    children = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            stat = Path(f"/proc/{entry}/stat").read_text()
        except OSError:
            continue  # exited while we looked
        # Field 4 is the parent PID. Field 2 (the name) may contain spaces.
        if int(stat.rpartition(")")[2].split()[1]) == pid:
            children.append(int(entry))
    return children


# ---------------------------------------------------------------------------
# One BGSAVE
# ---------------------------------------------------------------------------

def bgsave_once(server: str, load: list[str] | None, tmp: Path) -> dict[str, float]:
    data_dir = Path(tempfile.mkdtemp(dir=tmp))
    port = free_port()
    proc = start(server, port, data_dir)
    writer = None
    try:
        preload(port)
        prober = Prober(port)
        prober.start()
        if load is not None:
            writer = subprocess.Popen(
                ["redis-benchmark", "-p", str(port), "-t", "set", "-n", "1000000000",
                 "-r", str(KEYS), "-d", str(len(VALUE)), *load, "-q"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        time.sleep(1)  # let the load settle
        memory_before = rss_mb(proc.pid)

        with socket.create_connection(("127.0.0.1", port)) as sock:
            started = time.perf_counter()
            sock.sendall(resp(b"BGSAVE"))
            reply = sock.recv(100)
            fork_pause = time.perf_counter() - started
        if not reply.startswith(b"+Background saving started"):
            raise RuntimeError(f"BGSAVE failed: {reply!r}")

        pids = child_pids(proc.pid)
        if len(pids) != 1:
            raise RuntimeError(f"expected one save child, found {pids}")
        peak = 0.0
        while (dirty := private_dirty_mb(pids[0])) is not None:
            peak = max(peak, dirty)
            if time.perf_counter() - started > 300:
                raise RuntimeError("save did not finish")
            time.sleep(SAMPLE_EVERY)
        ended = time.perf_counter()

        time.sleep(0.3)  # let the parent reap the child, and the last PINGs come back
        prober.done.set()
        prober.join()
        if not (data_dir / "dump.rdb").exists():
            raise RuntimeError("the save did not write dump.rdb")
        return {
            "memory_mb": memory_before,
            "fork_pause_ms": fork_pause * 1000,
            "save_s": ended - started,
            "cow_mb": peak,
            "slowest_ping_ms": max(prober.between(started, ended)) * 1000,
        }
    finally:
        if writer is not None:
            writer.kill()
            writer.wait()
        stop(proc)


def run_all(runs: int, tmp: Path) -> dict[tuple[str, str], list[dict[str, float]]]:
    results: dict[tuple[str, str], list[dict[str, float]]] = {}
    for server in SERVERS:
        for name, load in LOADS.items():
            results[server, name] = [bgsave_once(server, load, tmp) for _ in range(runs)]
            print(f"{server}, {name}: done", flush=True)
    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

COLUMNS = [  # key, heading, format
    ("memory_mb", "Memory (MB)", ".0f"),
    ("fork_pause_ms", "Fork pause (ms)", ".1f"),
    ("save_s", "Save (s)", ".2f"),
    ("cow_mb", "Copy-on-write peak (MB)", ".0f"),
    ("slowest_ping_ms", "Slowest PING (ms)", ".1f"),
]


def report(results: dict, runs: int, commit: str) -> str:
    def med(rows: list[dict[str, float]], key: str) -> float:
        return statistics.median(r[key] for r in rows)

    headings = " | ".join(h for _, h, _ in COLUMNS)
    lines = [
        f"# BGSAVE benchmark {datetime.date.today()} at commit {commit}",
        "",
        f"Median of {runs} run(s).",
        "",
        *machine_info(),
        f"- RDB directory file system: {command_output('stat', '-f', '-c', '%T', tempfile.gettempdir())}",
        "",
        "## Fork pause and copy-on-write memory",
        "",
        f"{KEYS:,} keys with {len(VALUE)}-byte values. During the save, "
        f"`redis-benchmark -t set -r {KEYS} -d {len(VALUE)}` writes with the load shown.",
        "A probe client sends one PING at a time. Real Redis runs with `rdbcompression no`.",
        "",
        "- Memory: the server's RSS just before BGSAVE.",
        "- Fork pause: how long the BGSAVE reply took. fork() copies the page tables, so it grows with memory.",
        "- Save: from BGSAVE until the child exited.",
        "- Copy-on-write peak: the child's peak Private_Dirty, the memory the two processes no longer share.",
        "- Copy-on-write %: the peak as a share of Memory.",
        "- Slowest PING: the slowest probe PING from BGSAVE until the child exited.",
        "",
        f"| Server | Writes during save | {headings} | Copy-on-write % |",
        "| --- | --- |" + " ---: |" * (len(COLUMNS) + 1),
    ]
    for (server, load), rows in results.items():
        cells = " | ".join(format(med(rows, key), fmt) for key, _, fmt in COLUMNS)
        share = 100 * statistics.median(r["cow_mb"] / r["memory_mb"] for r in rows)
        lines.append(f"| {server} | {load} | {cells} | {share:.0f}% |")
    lines += [
        "",
        "## All runs",
        "",
        f"| Server | Writes during save | Run | {headings} |",
        "| --- | --- | ---: |" + " ---: |" * len(COLUMNS),
    ]
    for (server, load), rows in results.items():
        for i, r in enumerate(rows, 1):
            cells = " | ".join(format(r[key], fmt) for key, _, fmt in COLUMNS)
            lines.append(f"| {server} | {load} | {i} | {cells} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=3, help="runs per case (default: 3)")
    runs = parser.parse_args().runs

    commit = command_output("git", "rev-parse", "--short", "HEAD")
    if command_output("git", "status", "--porcelain", "--", "mini_redis"):  # server code changed
        commit += "-dirty"

    with tempfile.TemporaryDirectory() as tmp_name:
        results = run_all(runs, Path(tmp_name))

    text = report(results, runs, commit)
    out = ROOT / "bench" / "results" / f"bgsave-fork-and-cow-{datetime.date.today()}-{commit}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
    print(f"Saved to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
