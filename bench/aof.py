"""Benchmark the AOF: SET throughput per fsync policy, and the BGREWRITEAOF pauses.

Linux only (on Windows, run it in WSL). Needs redis-server and redis-benchmark.

Run from the project root:
    python bench/aof.py            # 3 runs per case, about 4 minutes
    python bench/aof.py --runs 1   # quick check

Writes the report to bench/results/<date>-<commit>-aof.md.

Part 1 runs `redis-benchmark -t set` against mini_redis and real Redis with
each appendfsync policy.

Part 2 measures how long the event loop stops during BGREWRITEAOF. It loads
KEYS keys, starts a write load with redis-benchmark, then sends BGREWRITEAOF.
A probe client sends PING one at a time the whole run. The rewrite stops the
loop twice: at the fork, and at the end, when the parent writes the rewrite
buffer (the writes made while the child ran) to the new file, fsyncs it and
swaps it in. The PING that was waiting while the file was swapped shows the
second pause.
"""

import argparse
import datetime
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from run import ROOT, cell, command_output, free_port, machine_info, wait_for_port

POLICIES = ["always", "everysec", "no"]
SERVERS = ["mini_redis", "real Redis"]
PIPELINES = [1, 16]
CLIENTS = 50
REQUESTS = 100_000

KEYS = 1_000_000
VALUE = b"v" * 100
LOADS = {  # name -> redis-benchmark options for the writes made during the rewrite
    "no writes": None,
    "SET, 50 clients, pipeline 1": ["-c", "50", "-P", "1"],
    "SET, 50 clients, pipeline 16": ["-c", "50", "-P", "16"],
}


def resp(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def start(server: str, port: int, policy: str, data_dir: Path) -> subprocess.Popen:
    if server == "real Redis":
        cmd = ["redis-server", "--port", str(port), "--save", "", "--dir", str(data_dir),
               "--appendonly", "yes", "--appendfsync", policy]
        cwd = data_dir
    else:
        cmd = [sys.executable, "-m", "mini_redis", "--port", str(port), "--aof",
               "--aof-path", str(data_dir / "appendonly.aof"), "--appendfsync", policy]
        cwd = ROOT
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_port(proc, port)
    return proc


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Part 1: throughput per fsync policy
# ---------------------------------------------------------------------------

def set_rps(port: int, pipeline: int) -> float | None:
    try:
        out = subprocess.run(
            ["redis-benchmark", "-p", str(port), "-t", "set", "-n", str(REQUESTS),
             "-c", str(CLIENTS), "-P", str(pipeline), "--csv"],
            capture_output=True, text=True, timeout=600, check=True,
        ).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return float(out.splitlines()[1].split(",")[1].strip('"'))


def throughput(runs: int, tmp: Path) -> dict[tuple, list[float | None]]:
    results: dict[tuple, list[float | None]] = {}
    for server in SERVERS:
        for policy in POLICIES:
            data_dir = Path(tempfile.mkdtemp(dir=tmp))
            port = free_port()
            proc = start(server, port, policy, data_dir)
            try:
                for pipeline in PIPELINES:
                    for _ in range(runs):
                        results.setdefault((server, policy, pipeline), []).append(set_rps(port, pipeline))
                    print(f"{server}, appendfsync {policy}, pipeline {pipeline}: done", flush=True)
            finally:
                stop(proc)
    return results


# ---------------------------------------------------------------------------
# Part 2: BGREWRITEAOF pauses
# ---------------------------------------------------------------------------

def preload(port: int) -> int:
    """SET KEYS keys, and return the size of the AOF commands that rebuild them."""
    size = 0
    with socket.create_connection(("127.0.0.1", port)) as sock:
        batch = 10_000
        for start_ in range(0, KEYS, batch):
            cmds = [resp(b"SET", b"key:%d" % i, VALUE) for i in range(start_, start_ + batch)]
            size += sum(map(len, cmds))
            sock.sendall(b"".join(cmds))
            want = len(b"+OK\r\n") * batch
            got = 0
            while got < want:
                got += len(sock.recv(65536))
    return size


class Prober(threading.Thread):
    """Send PING, wait for PONG, repeat. Records (sent_at, round trip) in seconds."""

    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.samples: list[tuple[float, float]] = []
        self.done = threading.Event()

    def run(self) -> None:
        ping = resp(b"PING")
        while not self.done.is_set():
            sent = time.perf_counter()
            self.sock.sendall(ping)
            if self.sock.recv(16) != b"+PONG\r\n":
                raise RuntimeError("bad PING reply")
            self.samples.append((sent, time.perf_counter() - sent))
            time.sleep(0.001)

    def between(self, start: float, end: float) -> list[float]:
        return [rtt for sent, rtt in self.samples if start <= sent < end]


def rewrite_once(load: list[str] | None, tmp: Path) -> dict[str, float]:
    data_dir = Path(tempfile.mkdtemp(dir=tmp))
    aof_path = data_dir / "appendonly.aof"
    port = free_port()
    proc = start("mini_redis", port, "everysec", data_dir)
    writer = None
    try:
        snapshot_size = preload(port)
        prober = Prober(port)
        prober.start()
        if load is not None:
            writer = subprocess.Popen(
                ["redis-benchmark", "-p", str(port), "-t", "set", "-n", "1000000000",
                 "-r", str(KEYS), "-d", str(len(VALUE)), *load, "-q"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        time.sleep(1)  # let the load settle, and measure the normal round trip
        baseline_start = time.perf_counter()
        time.sleep(1)

        old_inode = aof_path.stat().st_ino
        with socket.create_connection(("127.0.0.1", port)) as sock:
            started = time.perf_counter()
            sock.sendall(resp(b"BGREWRITEAOF"))
            if not sock.recv(100).startswith(b"+Background"):
                raise RuntimeError("BGREWRITEAOF failed")
            fork_pause = time.perf_counter() - started

        while (stat := aof_path.stat()).st_ino == old_inode:
            if time.perf_counter() - started > 120:
                raise RuntimeError("rewrite did not finish")
            time.sleep(0.0005)
        swapped = time.perf_counter()
        buffer_size = stat.st_size - snapshot_size

        time.sleep(0.5)
        prober.done.set()
        prober.join()

        baseline = prober.between(baseline_start, started)
        # The PINGs sent shortly before the swap waited for the whole finish step.
        finish = prober.between(swapped - 2, swapped)
        return {
            "baseline_max_ms": max(baseline) * 1000,
            "fork_pause_ms": fork_pause * 1000,
            "rewrite_s": swapped - started,
            "buffer_mb": buffer_size / 1e6,
            "finish_pause_ms": max(finish) * 1000,
        }
    finally:
        if writer is not None:
            writer.kill()
            writer.wait()
        stop(proc)


def rewrite_pauses(runs: int, tmp: Path) -> dict[str, list[dict[str, float]]]:
    results = {}
    for name, load in LOADS.items():
        results[name] = [rewrite_once(load, tmp) for _ in range(runs)]
        print(f"rewrite with {name}: done", flush=True)
    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def report(rps: dict, pauses: dict, runs: int, commit: str) -> str:
    def med(rows: list[dict[str, float]], key: str, fmt: str) -> str:
        return format(statistics.median(r[key] for r in rows), fmt)

    lines = [
        f"# AOF benchmark {datetime.date.today()} at commit {commit}",
        "",
        f"Median of {runs} run(s).",
        "",
        *machine_info(),
        f"- AOF directory file system: {command_output('stat', '-f', '-c', '%T', tempfile.gettempdir())}",
        "",
        "## SET throughput per fsync policy",
        "",
        f"Command: `redis-benchmark -t set -n {REQUESTS} -c {CLIENTS} -P PIPELINE`, in ops/sec.",
        "",
        "| Pipeline | appendfsync | " + " | ".join(SERVERS) + " |",
        "| ---: | --- |" + " ---: |" * len(SERVERS),
    ]
    for pipeline in PIPELINES:
        for policy in POLICIES:
            lines.append(f"| {pipeline} | {policy} | "
                         + " | ".join(cell(rps[s, policy, pipeline]) for s in SERVERS) + " |")
    lines += [
        "",
        "## BGREWRITEAOF pauses",
        "",
        f"{KEYS:,} keys with {len(VALUE)}-byte values, appendfsync everysec. During the rewrite,",
        f"`redis-benchmark -t set -r {KEYS} -d {len(VALUE)}` writes with the load shown. A probe client",
        "sends one PING at a time.",
        "",
        "- Normal PING: the slowest PING in the second before BGREWRITEAOF.",
        "- Fork pause: how long the BGREWRITEAOF reply took.",
        "- Rewrite buffer: writes made while the child ran, which the parent writes to the new file.",
        "- Finish pause: the slowest PING in the 2 seconds before the new file was swapped in.",
        "",
        "| Writes during rewrite | Normal PING (ms) | Fork pause (ms) | Rewrite (s) "
        "| Rewrite buffer (MB) | Finish pause (ms) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, rows in pauses.items():
        lines.append(f"| {name} | {med(rows, 'baseline_max_ms', '.1f')} | {med(rows, 'fork_pause_ms', '.1f')} "
                     f"| {med(rows, 'rewrite_s', '.2f')} | {med(rows, 'buffer_mb', '.1f')} "
                     f"| {med(rows, 'finish_pause_ms', '.1f')} |")
    lines += ["", "## All runs", "", "| Writes during rewrite | Run | Normal PING (ms) | Fork pause (ms) "
              "| Rewrite (s) | Rewrite buffer (MB) | Finish pause (ms) |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for name, rows in pauses.items():
        for i, r in enumerate(rows, 1):
            lines.append(f"| {name} | {i} | {r['baseline_max_ms']:.1f} | {r['fork_pause_ms']:.1f} "
                         f"| {r['rewrite_s']:.2f} | {r['buffer_mb']:.1f} | {r['finish_pause_ms']:.1f} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=3, help="runs per case (default: 3)")
    runs = parser.parse_args().runs

    commit = command_output("git", "rev-parse", "--short", "HEAD")
    if command_output("git", "status", "--porcelain", "--", "mini_redis"):  # server code changed
        commit += "-dirty"

    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        rps = throughput(runs, tmp)
        pauses = rewrite_pauses(runs, tmp)

    text = report(rps, pauses, runs, commit)
    out = ROOT / "bench" / "results" / f"{datetime.date.today()}-{commit}-aof.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
    print(f"Saved to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
