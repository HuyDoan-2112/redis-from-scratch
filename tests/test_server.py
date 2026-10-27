"""Server behavior tests with real processes and TCP clients.

TODO: Verify two simultaneous clients, disconnects, and shutdown.
Use a temporary free port so tests do not depend on port 6379 being available.

Run from the project root:
    python -m unittest tests.test_server -v
"""

import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path

from tests.test_aof import free_port

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def start_server(port: int) -> subprocess.Popen:
    """Start `python -m mini_redis --port PORT` and wait until it accepts connections."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "mini_redis", "--port", str(port)],
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
    proc.kill()
    proc.wait()


class TestStartup(unittest.TestCase):
    def test_second_server_on_same_port_fails(self):
        """A port in use must make the second server exit, not share the port."""
        port = free_port()
        first = start_server(port)
        self.addCleanup(stop_server, first)

        try:
            second = subprocess.run(
                [sys.executable, "-m", "mini_redis", "--port", str(port)],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            self.fail("second server started on a port that was already in use")

        self.assertEqual(second.returncode, 1)
        self.assertIn(f"port {port} is already in use", second.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
