"""
Needs the test dependencies: python -m pip install --group test

Run from the project root:
    python -m unittest tests.test_redis_py -v
"""

import unittest

import redis

from tests.test_aof import free_port
from tests.test_server import start_server, stop_server


class TestRedisPy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # One server for all tests, so each test uses its own key names.
        cls.port = free_port()
        cls.server = start_server(cls.port)
        cls.r = cls.connect()

    @classmethod
    def connect(cls) -> redis.Redis:
        # The server speaks RESP2 only. redis-py 8 asks for RESP3 unless told.
        return redis.Redis(port=cls.port, protocol=2, socket_timeout=5)

    @classmethod
    def tearDownClass(cls):
        cls.r.close()
        stop_server(cls.server)

    def test_ping(self):
        self.assertTrue(self.r.ping())

    def test_echo(self):
        self.assertEqual(self.r.echo("hi"), b"hi")

    def test_set_then_get(self):
        self.assertTrue(self.r.set("str:k", "v"))
        self.assertEqual(self.r.get("str:k"), b"v")

    def test_get_missing_key(self):
        self.assertIsNone(self.r.get("missing"))

    def test_hset_then_hget(self):
        self.assertEqual(self.r.hset("hash:h", "f", "1"), 1)  # new field
        self.assertEqual(self.r.hset("hash:h", "f", "2"), 0)  # updated field
        self.assertEqual(self.r.hget("hash:h", "f"), b"2")

    def test_hget_missing_field(self):
        self.r.hset("hash:h2", "f", "1")
        self.assertIsNone(self.r.hget("hash:h2", "nope"))

    def test_binary_value(self):
        """Roadmap 1.1: bytes that are not valid text come back unchanged."""
        self.r.set("bin:k", b"\xff\xfe")
        self.assertEqual(self.r.get("bin:k"), b"\xff\xfe")

    def test_wrong_type(self):
        self.r.set("type:str", "v")
        self.r.hset("type:hash", "f", "1")
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.hget("type:str", "f")
        with self.assertRaisesRegex(redis.ResponseError, "WRONGTYPE"):
            self.r.get("type:hash")

    def test_unknown_command(self):
        with self.assertRaisesRegex(redis.ResponseError, "unknown command 'FOO'"):
            self.r.execute_command("FOO")

    def test_wrong_number_of_arguments(self):
        with self.assertRaisesRegex(redis.ResponseError, "wrong number of arguments for 'get'"):
            self.r.execute_command("GET")

    def test_pipeline(self):
        """Roadmap 1.2: many commands in one send are all answered, in order."""
        pipe = self.r.pipeline(transaction=False)
        for i in range(100):
            pipe.set(f"pipe:{i}", i)
        for i in range(100):
            pipe.get(f"pipe:{i}")
        replies = pipe.execute()

        self.assertEqual(replies[:100], [True] * 100)
        self.assertEqual(replies[100:], [str(i).encode() for i in range(100)])

    def test_two_clients_share_data(self):
        other = self.connect()
        self.addCleanup(other.close)
        self.r.set("shared:k", "from first")
        self.assertEqual(other.get("shared:k"), b"from first")


if __name__ == "__main__":
    unittest.main(verbosity=2)
