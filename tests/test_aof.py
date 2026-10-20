"""Tests for AOF crash recovery.

Scenario we test: the server crashes while writing a command to the AOF,
so the last command in the file is only half written.

Expected behavior (same as real Redis with aof-load-truncated yes):
  1. On restart, every complete command is replayed.
  2. The broken last command is skipped.
  3. The broken bytes are cut off the file, so new commands are not
     glued onto them.
  4. The server can restart again and again after that.

Run from the project root:
    python3 -m unittest tests.test_aof -v
"""

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.aof import AOF
from mini_redis.config import AOFConfig
from mini_redis.resp import RESPType, Value

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_cmd(*parts: str) -> Value:
    """Build a RESP array command, e.g. make_cmd("SET", "a", "1")."""
    return Value(
        typ=RESPType.ARRAY,
        array=[Value(typ=RESPType.BULK_STRING, bulk=p.encode()) for p in parts],
    )


def cmd_bytes(*parts: str) -> bytes:
    """RESP bytes of a command, exactly as the AOF stores it."""
    return make_cmd(*parts).marshal()


def replay(path: Path) -> list[list[str]]:
    """Open the AOF, replay it, and return the commands as lists of strings.

    We collect commands in a list instead of using the global store,
    so tests do not affect each other.
    """
    seen: list[list[str]] = []
    aof = AOF(AOFConfig(path=path, enabled=True))
    try:
        aof.read(lambda v: seen.append([item.bulk.decode() for item in v.array]))
    finally:
        aof.close()
    return seen


# ---------------------------------------------------------------------------
# Unit tests: AOF class only (fast, no network)
# ---------------------------------------------------------------------------

class TestAOFRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "appendonly.aof"

    def tearDown(self):
        self.tmp.cleanup()

    def test_replay_complete_file(self):
        """A healthy file replays every command in order."""
        self.path.write_bytes(cmd_bytes("SET", "a", "1") + cmd_bytes("SET", "b", "2"))

        self.assertEqual(replay(self.path), [["SET", "a", "1"], ["SET", "b", "2"]])

    def test_broken_last_command_is_skipped(self):
        """A half-written last command is skipped; earlier ones still load."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]  # crash after 10 bytes
        self.path.write_bytes(good + broken)

        self.assertEqual(replay(self.path), [["SET", "a", "1"]])

    def test_broken_bytes_are_removed_from_file(self):
        """After replay, the file must contain only the complete commands."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]
        self.path.write_bytes(good + broken)

        replay(self.path)

        self.assertEqual(
            self.path.read_bytes(),
            good,
            "broken tail was not truncated from the AOF file",
        )

    def test_new_write_after_crash_survives_next_restart(self):
        """The full bug: crash -> restart -> write -> restart must still work."""
        good = cmd_bytes("SET", "a", "1")
        broken = cmd_bytes("SET", "b", "2")[:10]
        self.path.write_bytes(good + broken)

        # Restart #1: replay, then a client sends SET c 3
        aof = AOF(AOFConfig(path=self.path, enabled=True))
        aof.read(lambda v: None)
        aof.write(make_cmd("SET", "c", "3"))
        aof.close()

        # Restart #2: must not crash, and must see a and c
        try:
            commands = replay(self.path)
        except Exception as err:
            self.fail(f"server could not replay AOF on second restart: {err!r}")

        self.assertEqual(commands, [["SET", "a", "1"], ["SET", "c", "3"]])

    def test_cut_inside_length_line(self):
        """Same bug when the cut lands inside '$1\\r\\n' (like truncate -s -5)."""
        good = cmd_bytes("SET", "a", "1")
        full_b = cmd_bytes("SET", "b", "2")
        self.path.write_bytes(good + full_b[:-5])  # file ends with "...$1"

        aof = AOF(AOFConfig(path=self.path, enabled=True))
        aof.read(lambda v: None)
        aof.write(make_cmd("SET", "c", "3"))
        aof.close()

        try:
            commands = replay(self.path)
        except Exception as err:
            self.fail(f"server could not replay AOF on second restart: {err!r}")

        self.assertEqual(commands, [["SET", "a", "1"], ["SET", "c", "3"]])


# ---------------------------------------------------------------------------
# End-to-end test: real server process + real TCP client
# (the same steps you did by hand with redis-cli)
# ---------------------------------------------------------------------------

def free_port() -> int:
    """Ask the OS for a free port so we never clash with a real Redis on 6379."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    """A tiny RESP client so the test needs no extra packages."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.file = self.sock.makefile("rb")

    def call(self, *parts: str):
        self.sock.sendall(cmd_bytes(*parts))
        return self._read_reply()

    def _read_reply(self):
        line = self.file.readline().rstrip(b"\r\n")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            length = int(rest)
            if length == -1:
                return None  # this is what redis-cli shows as (nil)
            data = self.file.read(length + 2)
            return data[:-2].decode()
        raise RuntimeError(f"unexpected reply: {line!r}")

    def close(self):
        self.file.close()
        self.sock.close()


class Server:
    """Start and stop your server as a separate process, with AOF on."""

    def __init__(self, port: int, aof_path: Path):
        self.port = port
        self.aof_path = aof_path
        self.proc = None

    def start(self):
        code = (
            "from pathlib import Path\n"
            "from mini_redis.server import run\n"
            "from mini_redis.config import ServerConfig, AOFConfig\n"
            f"run(ServerConfig(port={self.port}, "
            f"aof=AOFConfig(enabled=True, path=Path({str(self.aof_path)!r}))))\n"
        )
        self.proc = subprocess.Popen(
            [sys.executable, "-c", code],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Wait until the port accepts connections, or the process dies.
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.proc.poll() is not None:
                _, err = self.proc.communicate()
                raise RuntimeError("server crashed on boot:\n" + err.decode()[-800:])
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError("server did not start within 5 seconds")

    def stop(self):
        if self.proc is None:
            return
        time.sleep(1.5)  # the AOF sync thread flushes once per second
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc.stdout.close()
        self.proc.stderr.close()
        self.proc = None


class TestAOFEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.aof_path = Path(self.tmp.name) / "appendonly.aof"
        self.server = Server(free_port(), self.aof_path)

    def tearDown(self):
        self.server.stop()
        self.tmp.cleanup()

    def _client(self) -> Client:
        c = Client(self.server.port)
        self.addCleanup(c.close)
        return c

    def test_crash_recovery_like_redis_cli(self):
        # Step 1-2: start, write two keys, stop
        self.server.start()
        c = self._client()
        self.assertEqual(c.call("SET", "a", "1"), "OK")
        self.assertEqual(c.call("SET", "b", "2"), "OK")
        c.close()
        self.server.stop()

        # Step 3: simulate a crash mid-write (same as: truncate -s -5 appendonly.aof)
        size = self.aof_path.stat().st_size
        os.truncate(self.aof_path, size - 5)

        # Step 4: restart; b is lost, a is still there; then write c
        self.server.start()
        c = self._client()
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertIsNone(c.call("GET", "b"))  # (nil)
        self.assertEqual(c.call("SET", "c", "3"), "OK")
        c.close()
        self.server.stop()

        # Step 5: restart again -> must boot, with a and c
        self.server.start()  # raises "server crashed on boot" if the bug is there
        c = self._client()
        self.assertEqual(c.call("GET", "a"), "1")
        self.assertIsNone(c.call("GET", "b"))
        self.assertEqual(c.call("GET", "c"), "3")


if __name__ == "__main__":
    unittest.main(verbosity=2)
