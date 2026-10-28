"""Server behavior tests with real processes and TCP clients.
Run from the project root:
    python -m unittest tests.test_server -v
"""

import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from tests.helpers import cmd_bytes
from tests.test_aof import free_port

PROJECT_ROOT = Path(__file__).resolve().parent.parent


STOP_SIGNALS = {"SIGINT": signal.SIGINT, "SIGTERM": signal.SIGTERM}


def start_server(port: int, *args: str) -> subprocess.Popen:
    """Start the server and wait until it accepts connections."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "mini_redis", "--port", str(port), *args],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited on startup")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    proc.wait()
    raise RuntimeError("server did not start within 5 seconds")


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.kill()
    proc.wait()


class TestShutdown(unittest.TestCase):
    def test_stop_signal_saves_aof_and_closes_clients(self):
        """Roadmap 1.5: save the AOF, close sockets, exit 0."""
        for name, sig in STOP_SIGNALS.items():
            with self.subTest(signal=name), tempfile.TemporaryDirectory() as tmp:
                aof_path = Path(tmp) / "appendonly.aof"
                port = free_port()
                proc = start_server(port, "--aof", "--aof-path", str(aof_path))
                self.addCleanup(stop_server, proc)

                client = socket.create_connection(("127.0.0.1", port), timeout=5)
                self.addCleanup(client.close)
                client.sendall(cmd_bytes("SET", "a", "1"))
                self.assertEqual(client.recv(100), b"+OK\r\n")

                # Stop before the 1-second AOF sync runs.
                proc.send_signal(sig)

                self.assertEqual(client.recv(100), b"", "server did not close the client")
                self.assertEqual(proc.wait(timeout=5), 0)
                self.assertEqual(aof_path.read_bytes(), cmd_bytes("SET", "a", "1"))


class TestProtocolErrors(unittest.TestCase):
    """Every item in a command must be a bulk string, as in Redis."""

    def test_non_bulk_item_is_rejected_and_connection_closed(self):
        cases = {
            "integer": (b":5\r\n", b":"),
            "simple string": (b"+a\r\n", b"+"),
            "nested array": (b"*1\r\n$1\r\na\r\n", b"*"),
        }
        port = free_port()
        proc = start_server(port)
        self.addCleanup(stop_server, proc)

        def connect() -> socket.socket:
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            self.addCleanup(sock.close)
            return sock

        setup = connect()
        setup.sendall(cmd_bytes("SET", "", "v"))
        self.assertEqual(setup.recv(100), b"+OK\r\n")

        for name, (item, prefix) in cases.items():
            with self.subTest(item=name):
                client = connect()
                client.sendall(b"*2\r\n$3\r\nDEL\r\n" + item)
                expected = b"-ERR Protocol error: expected '$', got '" + prefix + b"'\r\n"
                self.assertEqual(client.recv(100), expected)
                self.assertEqual(client.recv(100), b"", "server did not close the client")

        # The bad DELs must not have run as DEL "".
        setup.sendall(cmd_bytes("GET", ""))
        self.assertEqual(setup.recv(100), b"$1\r\nv\r\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
