"""Tests for the command table: lookup, arity, and the write flag.

These call execute() directly, so no server or socket is needed.

Run from the project root:
    python -m unittest tests.test_commands -v
"""

import tempfile
import unittest
from pathlib import Path

from mini_redis.config import AOFConfig
from mini_redis.persistence.aof import AOF
from mini_redis.server import execute, replay_command
from mini_redis.commands.registry import CommandError
from tests.helpers import cmd_bytes, make_cmd


class TestCommandTable(unittest.TestCase):
    def test_unknown_command(self):
        reply = execute(make_cmd("FOO"), aof=None)
        self.assertEqual(reply.marshal(), b"-ERR unknown command 'FOO'\r\n")

    def test_wrong_number_of_arguments(self):
        reply = execute(make_cmd("GET"), aof=None)
        self.assertEqual(
            reply.marshal(),
            b"-ERR wrong number of arguments for 'get' command\r\n",
        )

    def test_echo(self):
        reply = execute(make_cmd("ECHO", "hi"), aof=None)
        self.assertEqual(reply.marshal(), b"$2\r\nhi\r\n")

    def test_echo_without_message(self):
        reply = execute(make_cmd("ECHO"), aof=None)
        self.assertEqual(
            reply.marshal(),
            b"-ERR wrong number of arguments for 'echo' command\r\n",
        )


class TestWriteFlag(unittest.TestCase):
    def test_only_write_commands_go_to_aof(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "appendonly.aof"
            aof = AOF(AOFConfig(path=path, enabled=True))
            execute(make_cmd("SET", "a", "1"), aof)
            execute(make_cmd("GET", "a"), aof)
            aof.close()  # flush to disk

            self.assertEqual(path.read_bytes(), cmd_bytes("SET", "a", "1"))


class TestReplay(unittest.TestCase):
    def test_damaged_aof_command_is_rejected(self):
        with self.assertRaises(CommandError):
            replay_command(make_cmd("SET", "a"))  # value is missing


if __name__ == "__main__":
    unittest.main()
