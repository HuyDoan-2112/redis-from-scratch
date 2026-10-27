"""Benchmark mini_redis against its old thread-per-client design and real Redis.

Linux only (on Windows, run it in WSL). Needs redis-server and redis-benchmark.

Run from the project root:
    python bench/run.py            # 3 runs per case, about 10 minutes
    python bench/run.py --runs 1   # quick check

Writes the report to bench/results/<date>-<commit>.md.
"""

import argparse
import csv
import datetime
import io
import os
import platform
import resource
import signal
import socket
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OLD_COMMIT = "12e9de6"  # last thread-per-client version
REQUESTS = 100_000
CLIENTS = [50, 1000]
PIPELINES = [1, 16]
TESTS = ["SET", "GET"]
SERVERS = ["old thread-per-client", "mini_redis", "real Redis"]
BACKLOG = 511  # current ServerConfig.listen_backlog


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(proc: subprocess.Popen, port: int) -> None:
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited on startup: {proc.args}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server did not start: {proc.args}")


def start_server(name: str, port: int, aof: bool, tmp: Path, old_code: Path) -> subprocess.Popen:
    if name == "real Redis":
        cmd = ["redis-server", "--port", str(port), "--save", "", "--dir", str(tmp),
               "--appendonly", "yes" if aof else "no", "--appendfsync", "everysec"]
        cwd = tmp
    else:
        cmd = [sys.executable, "-m", "mini_redis", "--port", str(port)]
        if aof:
            cmd += ["--aof", "--aof-path", str(tmp / "appendonly.aof")]
        cwd = old_code if name == "old thread-per-client" else ROOT
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_port(proc, port)
    return proc


def stop_server(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:  # the old server does not stop on SIGINT
        proc.kill()
        proc.wait()


def benchmark(port: int, clients: int, pipeline: int) -> dict[str, float | None]:
    """Run redis-benchmark once and return ops/sec per test, or None if it failed."""
    try:
        out = subprocess.run(
            ["redis-benchmark", "-p", str(port), "-t", "set,get", "-n", str(REQUESTS),
             "-c", str(clients), "-P", str(pipeline), "--csv"],
            capture_output=True, text=True, timeout=300, check=True,
        ).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return {test: None for test in TESTS}
    return {row["test"]: float(row["rps"]) for row in csv.DictReader(io.StringIO(out))}


def extract_old_code(dest: Path) -> None:
    archive = subprocess.run(["git", "archive", OLD_COMMIT, "mini_redis"],
                             cwd=ROOT, capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(dest, filter="data")
    # The old listen backlog was 5. Use the current value, so the comparison
    # measures the design and not that one setting.
    config = dest / "mini_redis" / "config.py"
    text = config.read_text()
    assert "listen_backlog: int = 5" in text
    config.write_text(text.replace("listen_backlog: int = 5", f"listen_backlog: int = {BACKLOG}"))


def cell(values: list[float | None]) -> str:
    """Median ops/sec, and how many runs failed."""
    ok = [v for v in values if v is not None]
    if not ok:
        return "failed"
    text = f"{statistics.median(ok):,.0f}"
    return text if len(ok) == len(values) else f"{text} ({len(values) - len(ok)} failed)"


def command_output(*cmd: str) -> str:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True).stdout.strip()


def machine_info() -> list[str]:
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), "unknown")
    return [
        f"- CPU: {cpu}, {len(os.sched_getaffinity(0))} cores",
        f"- Kernel: {platform.release()}",
        f"- Python: {platform.python_version()}",
        f"- {command_output('redis-server', '--version').split(' sha=')[0]}",
    ]


def report(results: dict, runs: int, commit: str) -> str:
    lines = [
        f"# Benchmark {datetime.date.today()} at commit {commit}",
        "",
        f"Command: `redis-benchmark -t set,get -n {REQUESTS} -c CLIENTS -P PIPELINE`, "
        f"median of {runs} run(s), in ops/sec.",
        f"\"old thread-per-client\" is commit {OLD_COMMIT}, with its listen backlog raised "
        f"from 5 to {BACKLOG} like the current code. AOF on means fsync every second.",
        "",
        *machine_info(),
    ]
    for aof in [False, True]:
        lines += ["", f"## AOF {'on' if aof else 'off'}", "",
                  "| Clients | Pipeline | Test | " + " | ".join(SERVERS) + " | mini_redis / old |",
                  "| ---: | ---: | --- |" + " ---: |" * (len(SERVERS) + 1)]
        for clients in CLIENTS:
            for pipeline in PIPELINES:
                for test in TESTS:
                    values = {s: results[s, aof, clients, pipeline, test] for s in SERVERS}
                    new = [v for v in values["mini_redis"] if v is not None]
                    old = [v for v in values["old thread-per-client"] if v is not None]
                    ratio = f"{statistics.median(new) / statistics.median(old):.1f}x" if new and old else "-"
                    lines.append(f"| {clients} | {pipeline} | {test} | "
                                 + " | ".join(cell(values[s]) for s in SERVERS)
                                 + f" | {ratio} |")
    lines += ["", "## All runs", "", "| Server | AOF | Clients | Pipeline | Test | ops/sec |",
              "| --- | --- | ---: | ---: | --- | --- |"]
    for (server, aof, clients, pipeline, test), values in results.items():
        lines.append(f"| {server} | {'on' if aof else 'off'} | {clients} | {pipeline} | {test} | "
                     + ", ".join("failed" if v is None else f"{v:,.0f}" for v in values) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--runs", type=int, default=3, help="runs per case (default: 3)")
    runs = parser.parse_args().runs

    # 1000 clients need more than the common default of 1024 open files.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, max(soft, 4096)), hard))

    commit = command_output("git", "rev-parse", "--short", "HEAD")
    if command_output("git", "status", "--porcelain", "--", "mini_redis"):  # server code changed
        commit += "-dirty"

    results: dict[tuple, list[float | None]] = {}
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        extract_old_code(tmp / "old")
        for server in SERVERS:
            for aof in [False, True]:
                data_dir = tmp / f"{server}-{aof}".replace(" ", "-")
                data_dir.mkdir()
                port = free_port()
                proc = start_server(server, port, aof, data_dir, tmp / "old")
                try:
                    for clients in CLIENTS:
                        for pipeline in PIPELINES:
                            for _ in range(runs):
                                rps = benchmark(port, clients, pipeline)
                                for test in TESTS:
                                    results.setdefault((server, aof, clients, pipeline, test), []).append(rps[test])
                            print(f"{server}, AOF {'on' if aof else 'off'}, {clients} clients, "
                                  f"pipeline {pipeline}: done", flush=True)
                finally:
                    stop_server(proc)

    text = report(results, runs, commit)
    out = ROOT / "bench" / "results" / f"{datetime.date.today()}-{commit}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
    print(f"Saved to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
