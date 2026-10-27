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


if __name__ == "__main__":
    unittest.main(verbosity=2)
