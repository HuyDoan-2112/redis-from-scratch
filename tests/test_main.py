"""Tests for the command-line options in mini_redis/__main__.py.

Run from the project root:
    python -m unittest tests.test_main -v
"""

import contextlib
import io
import unittest
from pathlib import Path

from mini_redis.__main__ import parse_args
from mini_redis.config import AOFConfig, RDBConfig

class TestParseArgs(unittest.TestCase):
    def assert_usage_error(self, argv: list[str], message: str) -> None:
        """argparse prints a usage message to stderr and exits with code 2."""
        with contextlib.redirect_stderr(io.StringIO()) as err, self.assertRaises(SystemExit) as cm:
            parse_args(argv)
        self.assertEqual(cm.exception.code, 2)
        self.assertIn(message, err.getvalue())

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
        self.assert_usage_error(["--port", "0"], "port must be between 1 and 65535")

    def test_appendfsync_default_is_everysec(self):
        self.assertEqual(parse_args([]).aof.appendfsync, "everysec")

    def test_appendfsync_flag(self):
        config = parse_args(["--appendfsync", "always"])
        self.assertEqual(config.aof.appendfsync, "always")

    def test_bad_appendfsync_exits(self):
        self.assert_usage_error(["--appendfsync", "sometimes"], "invalid choice: 'sometimes'")

    def test_config_rejects_bad_appendfsync(self):
        # Covers code that builds AOFConfig directly, without argparse.
        with self.assertRaises(ValueError):
            AOFConfig(appendfsync="sometimes")

    def test_rdb_path_default(self):
        self.assertEqual(parse_args([]).rdb.path, Path("dump.rdb"))

    def test_rdb_path(self):
        config = parse_args(["--rdb-path", "data/snap.rdb"])
        self.assertEqual(config.rdb.path, Path("data/snap.rdb"))

    def test_save_points_are_off_by_default(self):
        self.assertEqual(parse_args([]).rdb.save_points, ())

    def test_save_points(self):
        config = parse_args(["--save", "60 1000 300 10"])
        self.assertEqual(config.rdb.save_points, ((60, 1000), (300, 10)))

    def test_save_needs_pairs(self):
        self.assert_usage_error(["--save", "60 1000 300"], "argument --save: needs pairs of <seconds> <changes>")

    def test_save_needs_integers(self):
        self.assert_usage_error(["--save", "60 lots"], "argument --save: not an integer: 'lots'")

    def test_save_values_must_be_at_least_one(self):
        self.assert_usage_error(["--save", "0 10"], "save point seconds and changes must be at least 1")

    def test_config_rejects_bad_save_points(self):
        with self.assertRaises(ValueError):
            RDBConfig(save_points=((60, 0),))

if __name__ == "__main__":
    unittest.main()
