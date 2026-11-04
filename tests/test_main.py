"""Tests for the command-line options in mini_redis/__main__.py.

Run from the project root:
    python -m unittest tests.test_main -v
"""

import unittest
from pathlib import Path

from mini_redis.__main__ import parse_args
from mini_redis.config import AOFConfig

class TestParseArgs(unittest.TestCase):
    def test_defaults(self):
        config = parse_args([])
        self.assertFalse(config.aof.enabled)
        self.assertEqual(config.port, 6379)

    def test_aof_flag_turns_aof_on(self):
        config = parse_args(["--aof"])
        self.assertTrue(config.aof.enabled)

    def test_port(self):
        config = parse_args(["--port", "7000"])
        self.assertEqual(config.port, 7000)

    def test_aof_path(self):
        config = parse_args(["--aof-path", "data.aof"])
        self.assertEqual(config.aof.path, Path("data.aof"))

    def test_bad_port_exits(self):
        # argparse prints a usage message and exits with code 2.
        with self.assertRaises(SystemExit) as cm:
            parse_args(["--port", "0"])
        self.assertEqual(cm.exception.code, 2)

    def test_appendfsync_default_is_everysec(self):
        self.assertEqual(parse_args([]).aof.appendfsync, "everysec")

    def test_appendfsync_flag(self):
        config = parse_args(["--appendfsync", "always"])
        self.assertEqual(config.aof.appendfsync, "always")

    def test_bad_appendfsync_exits(self):
        with self.assertRaises(SystemExit) as cm:
            parse_args(["--appendfsync", "sometimes"])
        self.assertEqual(cm.exception.code, 2)

    def test_config_rejects_bad_appendfsync(self):
        # Covers code that builds AOFConfig directly, without argparse.
        with self.assertRaises(ValueError):
            AOFConfig(appendfsync="sometimes")

if __name__ == "__main__":
    unittest.main()
