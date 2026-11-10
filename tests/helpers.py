import contextlib
import io
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from mini_redis.persistence import child
from mini_redis.persistence.aof import AOF
from mini_redis.config import AOFConfig
from mini_redis.resp import Value, RESPType
from mini_redis.storage import keyspace

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
        # read() prints a line when it cuts a broken tail. Keep test output clean.
        with contextlib.redirect_stdout(io.StringIO()):
            aof.read(lambda v: seen.append([item.bulk.decode() for item in v.array]))
    finally:
        aof.close()
    return seen


def reset_keyspace() -> None:
    """Empty the global store, for tests that run commands in this process."""
    keyspace.store.clear()
    keyspace.expires.clear()
    keyspace.ttl_keys.clear()
    keyspace.ttl_pos.clear()


def wait_for_child(timeout: float = 5.0) -> None:
    """Run child.check() the way the server's cron does, until no child runs."""
    deadline = time.time() + timeout
    while child.kind is not None:
        if time.time() > deadline:
            raise AssertionError(f"{child.kind} child did not finish")
        child.check()
        time.sleep(0.01)


@contextlib.contextmanager
def capture_fd2():
    """Send fd 2 (stderr) to a temp file, so output from a forked child is captured too."""
    with tempfile.TemporaryFile() as out:
        sys.stderr.flush()
        saved = os.dup(2)
        os.dup2(out.fileno(), 2)
        captured = bytearray()
        try:
            yield captured
        finally:
            sys.stderr.flush()
            os.dup2(saved, 2)
            os.close(saved)
            out.seek(0)
            captured += out.read()


def wait_until(condition, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def child_pids(pid: int) -> list[int]:
    """Process IDs whose parent is pid (Linux /proc)."""
    children = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            stat = Path(f"/proc/{entry}/stat").read_text()
        except OSError:
            continue  # the process exited while we looked
        # Field 4 is the parent PID. Field 2 (the name) may contain spaces,
        # so split after its closing parenthesis.
        if int(stat.rpartition(")")[2].split()[1]) == pid:
            children.append(int(entry))
    return children


def pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def free_port() -> int:
    """Ask the OS for a free port so we never clash with a real Redis on 6379."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    """A tiny RESP client so the tests need no extra packages."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.file = self.sock.makefile("rb")

    def call(self, *parts: str):
        self.sock.sendall(cmd_bytes(*parts))
        return self._read_reply()

    def pipeline(self, commands: list[tuple[str, ...]]) -> list:
        """Send all commands at once, then read one reply per command."""
        self.sock.sendall(b"".join(cmd_bytes(*c) for c in commands))
        return [self._read_reply() for _ in commands]

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
    """Run `python -m mini_redis` as a separate process, the way a user does."""

    def __init__(self, port: int, log_path: Path, *args: str):
        self.port = port
        self.args = args
        self.log_path = log_path  # stdout and stderr, shown if the server fails to boot
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        with open(self.log_path, "ab") as log:
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "mini_redis", "--port", str(self.port), *self.args],
                cwd=PROJECT_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        # Wait until the port accepts connections, or the process dies.
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.proc.poll() is not None:
                self.proc = None
                raise RuntimeError("server crashed on boot:\n" + self.log()[-800:])
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.kill()
        raise RuntimeError("server did not start within 5 seconds")

    def stop(self) -> int | None:
        """SIGTERM: a clean shutdown that flushes and fsyncs the AOF. Returns the exit code."""
        if self.proc is None:
            return None
        self.proc.terminate()
        try:
            code = self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            raise RuntimeError("server did not stop within 5 seconds")
        finally:
            self.proc = None
        return code

    def kill(self) -> None:
        """SIGKILL: a crash, with no chance to flush anything."""
        if self.proc is None:
            return
        self.proc.kill()
        self.proc.wait()
        self.proc = None

    def client(self) -> Client:
        return Client(self.port)

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")
