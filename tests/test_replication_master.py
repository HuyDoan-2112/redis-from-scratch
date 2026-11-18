"""Tests for the master side of replication (step 2).

The unit tests drive Master, REPLCONF and the server's command path in this
process. The end-to-end tests start `python -m mini_redis` and play the
replica by hand over a raw socket: REPLCONF, PSYNC ? -1, then read
+FULLRESYNC, the RDB file and the command stream byte for byte. Replies to
compare against were taken from Redis 8.10.3.

Run from the project root:
    python3 -m unittest tests.test_replication_master -v
"""

import contextlib
import io
import os
import socket
import tempfile
import time
import unittest
from pathlib import Path

from mini_redis.client import Client as Conn
from mini_redis.config import RDBConfig, ReplicationConfig
from mini_redis.persistence.rdb import RDB, dump, load
from mini_redis.replication import master as master_module
from mini_redis.replication.backlog import Backlog
from mini_redis.replication.master import PING, Master, Replica, ReplicaState, new_replid
from mini_redis.resp import CommandParser
from mini_redis.server import execute, process_input
from mini_redis.storage.keyspace import now_ms
from tests.helpers import (
    Server,
    capture_fd2,
    cmd_bytes,
    free_port,
    make_cmd,
    replay,
    reset_keyspace,
    wait_for_child,
    wait_until,
)

FAR_FUTURE_MS = 4102444800000  # 2100-01-01


def conn() -> Conn:
    """A Client on one end of a socket pair, for tests that need no server."""
    a, b = socket.socketpair()
    c = Conn(sock=a)
    c._peer = b  # keep the other end open while the test runs
    return c


def close_conn(c: Conn) -> None:
    c.sock.close()
    c._peer.close()


def set_master(test: unittest.TestCase, m: Master | None) -> None:
    """Make `m` the server's Master for this test, and put the old one back after."""
    old = master_module.current
    master_module.current = m
    test.addCleanup(setattr, master_module, "current", old)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

class TestReplid(unittest.TestCase):
    def test_40_lowercase_hex_characters(self):
        replid = new_replid()
        self.assertRegex(replid, r"^[0-9a-f]{40}$")

    def test_new_on_every_start(self):
        self.assertNotEqual(Master(ReplicationConfig()).replid, Master(ReplicationConfig()).replid)


class TestReplicationConfig(unittest.TestCase):
    def test_defaults_match_redis(self):
        config = ReplicationConfig()
        self.assertEqual((config.backlog_size, config.ping_period), (1024 * 1024, 10))

    def test_limits(self):
        with self.assertRaisesRegex(ValueError, "at least 16 KB"):
            ReplicationConfig(backlog_size=16 * 1024 - 1)
        with self.assertRaisesRegex(ValueError, "at least 1 second"):
            ReplicationConfig(ping_period=0)


class MasterTest(unittest.TestCase):
    def setUp(self):
        self.master = Master(ReplicationConfig(ping_period=1))

    def replica(self, state: ReplicaState) -> Conn:
        c = conn()
        self.addCleanup(close_conn, c)
        c.replica = Replica(state=state)
        self.master.replicas.append(c)
        return c


class TestFeed(MasterTest):
    def test_no_backlog_means_no_stream(self):
        """Before any replica connects there is nothing to keep, as in Redis."""
        self.master.feed(b"*1\r\n$4\r\nping\r\n")
        self.assertEqual(self.master.offset, 0)
        self.assertIsNone(self.master.backlog)

    def test_each_state_gets_the_bytes_in_the_right_place(self):
        self.master.backlog = Backlog(self.master.config.backlog_size)
        start = self.replica(ReplicaState.WAIT_BGSAVE_START)
        end = self.replica(ReplicaState.WAIT_BGSAVE_END)
        bulk = self.replica(ReplicaState.SEND_BULK)
        online = self.replica(ReplicaState.ONLINE)
        data = cmd_bytes("SET", "a", "1")

        self.master.feed(data)

        self.assertEqual(self.master.offset, len(data))
        self.assertEqual(self.master.backlog.read_from(1), data)
        # Not yet forked: the coming RDB includes the change.
        self.assertEqual((bytes(start.outbuf), bytes(start.replica.pending)), (b"", b""))
        # Forked: held back until the RDB is sent.
        for c in (end, bulk):
            self.assertEqual((bytes(c.outbuf), bytes(c.replica.pending)), (b"", data))
        # Live.
        self.assertEqual((bytes(online.outbuf), bytes(online.replica.pending)), (data, b""))


class TestCron(MasterTest):
    def setUp(self):
        super().setUp()
        self.master.backlog = Backlog(self.master.config.backlog_size)

    def test_ping_goes_into_the_stream(self):
        online = self.replica(ReplicaState.ONLINE)
        end = self.replica(ReplicaState.WAIT_BGSAVE_END)
        self.master.cron(time.monotonic() + 1)
        self.assertEqual(self.master.offset, len(PING))
        self.assertEqual(bytes(online.outbuf), PING)
        self.assertEqual(bytes(end.replica.pending), PING)

    def test_ping_waits_for_the_period(self):
        self.replica(ReplicaState.ONLINE)
        self.master.cron(time.monotonic() + 0.5)
        self.assertEqual(self.master.offset, 0)

    def test_no_ping_without_replicas(self):
        self.master.cron(time.monotonic() + 1)
        self.assertEqual(self.master.offset, 0)

    def test_newline_only_to_replicas_waiting_for_their_snapshot(self):
        """A keepalive outside the stream: never counted, never inside the file."""
        end = self.replica(ReplicaState.WAIT_BGSAVE_END)
        bulk = self.replica(ReplicaState.SEND_BULK)
        online = self.replica(ReplicaState.ONLINE)
        self.master._last_ping = float("inf")  # no PING in this test, only the newline
        self.master.cron(time.monotonic() + 1)
        self.assertEqual(bytes(end.outbuf), b"\n")
        self.assertEqual((bytes(bulk.outbuf), bytes(online.outbuf)), (b"", b""))
        self.assertEqual(self.master.offset, 0)


class TestRemoveAndDrop(MasterTest):
    def test_remove_forgets_the_replica_and_closes_its_file(self):
        c = self.replica(ReplicaState.SEND_BULK)
        with tempfile.TemporaryFile() as f:
            fd = os.dup(f.fileno())
        c.replica.rdb_fd = fd
        self.master.remove(c)
        self.assertEqual(self.master.replicas, [])
        self.assertIsNone(c.replica)
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_remove_ignores_normal_clients(self):
        c = conn()
        self.addCleanup(close_conn, c)
        self.master.remove(c)  # no error

    def test_drop_marks_each_replica_once(self):
        c = self.replica(ReplicaState.WAIT_BGSAVE_END)
        self.master.drop(c)
        self.master.drop(c)
        self.assertEqual(self.master.close_asap, [c])


class TestInfo(unittest.TestCase):
    def setUp(self):
        set_master(self, Master(ReplicationConfig()))

    def info(self, *sections: str) -> bytes:
        return execute(make_cmd("INFO", *sections), None).marshal()

    def test_replication_section(self):
        m = master_module.current
        body = (
            "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"
            f"master_replid:{m.replid}\r\nmaster_repl_offset:0\r\n"
            "repl_backlog_active:0\r\nrepl_backlog_size:1048576\r\n"
            "repl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n"
        ).encode()
        self.assertEqual(self.info("replication"), b"$%d\r\n%s\r\n" % (len(body), body))

    def test_section_names_like_redis(self):
        one = self.info("replication")
        self.assertEqual(self.info("REPLICATION", "replication"), one)  # case, duplicates
        self.assertEqual(self.info(), one)                             # every section
        self.assertEqual(self.info("all"), one)
        self.assertEqual(self.info("foo"), b"$0\r\n\r\n")              # unknown: empty


class TestReplconf(unittest.TestCase):
    """REPLCONF replies, byte for byte as Redis 8.10.3 sends them."""

    def setUp(self):
        self.c = conn()
        self.addCleanup(close_conn, self.c)

    def reply(self, *parts: str) -> bytes | None:
        reply = execute(make_cmd("REPLCONF", *parts), None, self.c)
        return None if reply is None else reply.marshal()

    def test_replies(self):
        cases = {
            (): b"+OK\r\n",
            ("listening-port", "7001"): b"+OK\r\n",
            ("LISTENING-PORT", "7001"): b"+OK\r\n",
            ("listening-port", "70000"): b"+OK\r\n",   # Redis does no range check
            ("listening-port", "-5"): b"+OK\r\n",
            ("listening-port", "abc"): b"-ERR value is not an integer or out of range\r\n",
            ("listening-port",): b"-ERR syntax error\r\n",
            ("capa", "eof", "capa", "psync2"): b"+OK\r\n",
            ("capa", "whatever"): b"+OK\r\n",
            ("foo", "bar"): b"-ERR Unrecognized REPLCONF option: foo\r\n",
            ("listening-port", "7001", "foo", "bar"): b"-ERR Unrecognized REPLCONF option: foo\r\n",
            ("ack", "100"): None,
            ("getack", "*"): None,
        }
        for parts, want in cases.items():
            with self.subTest(parts=parts):
                self.assertEqual(self.reply(*parts), want)

    def test_stored_on_the_connection_that_sent_it(self):
        other = conn()
        self.addCleanup(close_conn, other)
        self.reply("listening-port", "7001", "capa", "EOF", "capa", "psync2")
        self.assertEqual((self.c.listening_port, self.c.capa), (7001, {"eof", "psync2"}))
        self.assertEqual((other.listening_port, other.capa), (0, set()))


class TestCommandPath(unittest.TestCase):
    def setUp(self):
        reset_keyspace()
        self.addCleanup(reset_keyspace)
        self.c = conn()
        self.addCleanup(close_conn, self.c)

    def test_replica_connections_get_no_replies(self):
        """Their connection carries only the stream: a reply would corrupt it."""
        self.c.replica = Replica(state=ReplicaState.ONLINE)
        process_input(self.c, cmd_bytes("PING") + cmd_bytes("GET", "a"), None)
        self.assertEqual(bytes(self.c.outbuf), b"")

    def test_normal_connections_still_get_replies(self):
        process_input(self.c, cmd_bytes("PING") + cmd_bytes("REPLCONF", "ack", "1") + cmd_bytes("PING"), None)
        self.assertEqual(bytes(self.c.outbuf), b"+PONG\r\n+PONG\r\n")  # ack: nothing in between

    def test_writes_reach_the_stream_encoded_once(self):
        m = Master(ReplicationConfig())
        m.backlog = Backlog(m.config.backlog_size)
        set_master(self, m)
        execute(make_cmd("SET", "a", "1"), None)
        execute(make_cmd("EXPIRE", "a", "100"), None)
        stream = m.backlog.read_from(1)
        self.assertTrue(stream.startswith(cmd_bytes("SET", "a", "1")))
        self.assertIn(b"PEXPIREAT", stream)  # the deterministic form, as for the AOF
        self.assertEqual(m.offset, len(stream))

    def test_no_backlog_no_stream(self):
        m = Master(ReplicationConfig())
        set_master(self, m)
        execute(make_cmd("SET", "a", "1"), None)
        self.assertEqual(m.offset, 0)


class TestSaveDoneCallback(unittest.TestCase):
    """RDB.start_save(on_done) tells replication when its BGSAVE ends."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        reset_keyspace()

    def test_success(self):
        done: list[bool] = []
        RDB(RDBConfig(path=self.dir / "dump.rdb")).start_save(on_done=done.append)
        with contextlib.redirect_stdout(io.StringIO()):
            wait_for_child()
        self.assertEqual(done, [True])

    def test_failure(self):
        (self.dir / "temp-bgsave.rdb").mkdir()  # the child cannot open its temp file
        done: list[bool] = []
        # Fork inside the capture, so the child's traceback goes there too.
        with contextlib.redirect_stdout(io.StringIO()), capture_fd2() as err:
            RDB(RDBConfig(path=self.dir / "dump.rdb")).start_save(on_done=done.append)
            wait_for_child()
        self.assertEqual(done, [False])
        self.assertIn(b"IsADirectoryError", err)


# ---------------------------------------------------------------------------
# End-to-end: a real server and a replica played by hand
# ---------------------------------------------------------------------------

class FakeReplica:
    """The replica side of the protocol over a raw socket."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = bytearray()
        self.parser = CommandParser()  # for the stream after the RDB file

    def send(self, *parts: str) -> None:
        self.sock.sendall(cmd_bytes(*parts))

    def _recv(self) -> None:
        data = self.sock.recv(1 << 20)
        if not data:
            raise EOFError("the master closed the connection")
        self.buf += data

    def line(self) -> bytes:
        while b"\r\n" not in self.buf:
            self._recv()
        line, _, rest = bytes(self.buf).partition(b"\r\n")
        self.buf = bytearray(rest)
        return line

    def exact(self, n: int) -> bytes:
        while len(self.buf) < n:
            self._recv()
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    def skip_newlines(self, limit: float = 30.0) -> int:
        """Drop the \\n keepalives sent before +FULLRESYNC and the file.

        Keepalives reset the socket timeout, so a sync that never ends would
        loop here forever: fail after `limit` seconds instead.
        """
        deadline = time.monotonic() + limit
        count = 0
        while True:
            while not self.buf:
                if time.monotonic() > deadline:
                    raise AssertionError(f"only keepalives for {limit} s: the sync never went on")
                self._recv()
            if self.buf[:1] != b"\n":
                return count
            del self.buf[:1]
            count += 1

    def handshake(self, port: int) -> None:
        self.send("REPLCONF", "listening-port", str(port))
        assert self.line() == b"+OK"
        self.send("REPLCONF", "capa", "eof", "capa", "psync2")
        assert self.line() == b"+OK"

    def psync(self) -> tuple[str, int]:
        self.send("PSYNC", "?", "-1")
        self.skip_newlines()
        kind, replid, offset = self.line().split()
        assert kind == b"+FULLRESYNC", kind
        return replid.decode(), int(offset)

    def rdb(self) -> bytes:
        """The $<size> header, then exactly size bytes: no CRLF follows the file."""
        self.skip_newlines()
        header = self.line()
        assert header[:1] == b"$", header
        return self.exact(int(header[1:]))

    def command(self) -> list[bytes]:
        """The next command of the stream."""
        while True:
            if self.buf:
                self.parser.feed(bytes(self.buf))
                self.buf.clear()
            cmd = self.parser.next_command()
            if cmd is not None:
                return [item.bulk for item in cmd.array]
            self._recv()

    def close(self) -> None:
        self.sock.close()


class MasterServerTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.rdb_path = self.dir / "dump.rdb"
        self.aof_path = self.dir / "appendonly.aof"

    def start(self, *flags: str, keys: int = 0) -> Server:
        """Start a server. With keys, it loads that many keys from dump.rdb at startup,
        which is much faster than sending them."""
        if keys:
            store = {f"key:{i}".encode(): ("string", b"x" * 20) for i in range(keys)}
            with open(self.rdb_path, "wb") as f:
                dump(f, store, {}, now_ms())
        server = Server(free_port(), self.dir / "server.log", "--rdb-path", str(self.rdb_path), *flags)
        self.addCleanup(server.kill)
        server.start()
        self.c = server.client()
        self.addCleanup(self.c.close)
        return server

    def replica(self, server: Server, port: int = 7001) -> FakeReplica:
        r = FakeReplica(server.port)
        self.addCleanup(r.close)
        r.handshake(port)
        return r

    def info(self) -> dict[str, str]:
        text = self.c.call("INFO", "replication")
        return dict(line.split(":", 1) for line in text.split("\r\n") if ":" in line)

    def sync(self, server: Server, port: int = 7001) -> tuple[FakeReplica, int, bytes]:
        """A replica through PSYNC and the whole file. Returns it, the FULLRESYNC offset and the file."""
        r = self.replica(server, port)
        _, offset = r.psync()
        return r, offset, r.rdb()


class TestFullSync(MasterServerTest):
    def test_handshake_replies(self):
        server = self.start()
        r = FakeReplica(server.port)
        self.addCleanup(r.close)
        r.send("PING")
        self.assertEqual(r.line(), b"+PONG")
        r.send("REPLCONF", "ack", "5")  # never a reply
        r.send("PING")
        self.assertEqual(r.line(), b"+PONG")

    def test_rdb_then_stream(self):
        server = self.start()
        self.c.call("SET", "a", "1")
        self.c.call("HSET", "h", "f", "v")
        self.c.call("SET", "t", "v", "PXAT", str(FAR_FUTURE_MS))
        r = self.replica(server)

        replid, offset = r.psync()
        self.assertEqual(replid, self.info()["master_replid"])
        self.assertEqual(offset, 0)  # the first replica creates the backlog at offset 0

        body = r.rdb()
        self.assertEqual(body, self.rdb_path.read_bytes())
        store, expires = load(io.BytesIO(body), now_ms())
        self.assertEqual(store, {b"a": ("string", b"1"), b"h": ("hash", {b"f": b"v"}), b"t": ("string", b"v")})
        self.assertEqual(expires, {b"t": FAR_FUTURE_MS})

        self.c.call("SET", "b", "2")
        self.assertEqual(r.command(), [b"SET", b"b", b"2"])
        self.c.call("EXPIRE", "a", "100")
        self.assertEqual(r.command()[:2], [b"PEXPIREAT", b"a"])  # as Redis propagates it

        info = self.info()
        self.assertEqual(info["connected_slaves"], "1")
        self.assertIn("port=7001,state=online", info["slave0"])
        # Every stream byte the replica got is counted in the master's offset.
        self.assertEqual(int(info["master_repl_offset"]), offset + r.parser.bytes_used)
        self.assertEqual(info["repl_backlog_active"], "1")

    def test_writes_during_the_sync_arrive_right_after_the_file(self):
        server = self.start(keys=300_000)
        r = self.replica(server)
        r.psync()
        writes = [("SET", f"w{i}", str(i)) for i in range(10)]
        self.c.pipeline(writes)
        # The save of 300,000 keys is still running: the writes were held back.
        self.assertIn(self.info()["slave0"].split("state=")[1].split(",")[0], ("wait_bgsave", "send_bulk"))

        store, _ = load(io.BytesIO(r.rdb()), now_ms())
        self.assertEqual(len(store), 300_000)
        self.assertNotIn(b"w0", store)
        for w in writes:
            self.assertEqual(r.command(), [p.encode() for p in w])

    def test_replica_gets_no_replies(self):
        server = self.start()
        r, _, _ = self.sync(server)
        r.send("PING")
        r.send("GET", "a")
        r.send("REPLCONF", "ack", "0")
        self.c.call("SET", "x", "1")
        self.assertEqual(r.command(), [b"SET", b"x", b"1"])  # not +PONG or $-1 first

    def test_two_replicas(self):
        server = self.start()
        self.c.call("SET", "a", "1")
        a, _, file_a = self.sync(server, 7001)
        b, _, file_b = self.sync(server, 7002)
        self.assertEqual(load(io.BytesIO(file_a), now_ms()), load(io.BytesIO(file_b), now_ms()))

        self.c.call("SET", "b", "2")
        self.assertEqual(a.command(), [b"SET", b"b", b"2"])
        self.assertEqual(b.command(), [b"SET", b"b", b"2"])
        info = self.info()
        self.assertEqual(info["connected_slaves"], "2")
        self.assertIn("port=7001,state=online", info["slave0"])
        self.assertIn("port=7002,state=online", info["slave1"])

    def test_replica_that_leaves_mid_transfer(self):
        server = self.start(keys=300_000)
        fd_dir = Path(f"/proc/{server.proc.pid}/fd")
        fds_before = len(os.listdir(fd_dir))
        r = FakeReplica(server.port)
        r.handshake(7001)
        r.psync()
        r.skip_newlines()
        size = int(r.line()[1:])
        r.exact(size // 3)
        r.close()

        self.assertTrue(wait_until(lambda: self.info()["connected_slaves"] == "0"))
        self.assertEqual(self.c.call("PING"), "PONG")
        # Its socket and its open dump.rdb are both closed.
        self.assertTrue(wait_until(lambda: len(os.listdir(fd_dir)) == fds_before))

    def test_sync_waits_for_a_running_aof_rewrite(self):
        server = self.start("--aof", "--aof-path", str(self.aof_path), keys=300_000)
        self.assertEqual(self.c.call("BGREWRITEAOF"), "Background append only file rewriting started")
        r = self.replica(server)
        r.send("PSYNC", "?", "-1")
        self.assertIn("state=wait_bgsave", self.info()["slave0"])

        r.skip_newlines()
        self.assertTrue(r.line().startswith(b"+FULLRESYNC "))
        store, _ = load(io.BytesIO(r.rdb()), now_ms())
        self.assertEqual(len(store), 300_000)
        self.assertTrue(wait_until(lambda: "state=online" in self.info()["slave0"]))


class TestKeepalive(MasterServerTest):
    def test_ping_every_period_counts_in_the_offset_but_not_in_the_aof(self):
        server = self.start("--aof", "--aof-path", str(self.aof_path), "--repl-ping-replica-period", "1")
        r, offset, _ = self.sync(server)

        start = time.monotonic()
        pings = 0
        while pings < 2:
            self.assertEqual(r.command(), [b"ping"])
            pings += 1
        self.assertLess(time.monotonic() - start, 3.5)

        self.assertEqual(int(self.info()["master_repl_offset"]), offset + r.parser.bytes_used)
        self.assertEqual(r.parser.bytes_used % len(PING), 0)
        server.stop()
        self.assertNotIn(["ping"], [[p.lower() for p in c] for c in replay(self.aof_path)])

    def test_no_replica_no_stream(self):
        self.start("--repl-ping-replica-period", "1")
        self.c.call("SET", "a", "1")
        time.sleep(1.2)
        info = self.info()
        self.assertEqual((info["master_repl_offset"], info["repl_backlog_active"]), ("0", "0"))


if __name__ == "__main__":
    unittest.main()
