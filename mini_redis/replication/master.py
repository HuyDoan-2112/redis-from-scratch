"""
The master side of replication: the replication ID, the offset, the backlog,
and the replicas connected to this server.
"""
from dataclasses import dataclass, field
from enum import Enum, auto
import os
import time

from mini_redis.client import Client
from mini_redis.config import ReplicationConfig
from mini_redis.persistence import child, rdb
from mini_redis.replication.backlog import Backlog

# Sent to replicas every ping_period seconds, as part of the stream. Lowercase,
# byte for byte what Redis sends.
PING = b"*1\r\n$4\r\nping\r\n"

def new_replid() -> str:
    """A random replication ID: 40 hex characters, like Redis."""
    return os.urandom(20).hex()

class ReplicaState(Enum):
    """Where one replica is in its sync, as seen by the master."""

    WAIT_BGSAVE_START = auto()   # wants a full sync, waiting for the child slot
    WAIT_BGSAVE_END = auto()     # its BGSAVE is running
    SEND_BULK = auto()           # sending it dump.rdb
    ONLINE = auto()              # caught up, receiving the live stream

# State names as Redis shows them in INFO.
INFO_STATE = {
    ReplicaState.WAIT_BGSAVE_START: "wait_bgsave",
    ReplicaState.WAIT_BGSAVE_END: "wait_bgsave",
    ReplicaState.SEND_BULK: "send_bulk",
    ReplicaState.ONLINE: "online",
}

@dataclass(eq=False)
class Replica:
    """What the master knows about one replica. Lives on its Client."""

    state: ReplicaState = ReplicaState.WAIT_BGSAVE_START
    pending: bytearray = field(default_factory=bytearray)  # stream held back until the RDB is sent
    rdb_fd: int | None = None    # dump.rdb, open while sending it
    rdb_size: int = 0
    rdb_pos: int = 0             # bytes of it sent so far
    ack_offset: int = 0          # from REPLCONF ACK
    ack_time: float = field(default_factory=time.time)

class Master:
    """This server's replication state as a master. One per server."""

    def __init__(self, config: ReplicationConfig):
        self.config = config
        self.replid = new_replid()            # new on every start
        self.backlog: Backlog | None = None   # created when the first replica connects
        self.replicas: list[Client] = []      # connections that sent PSYNC
        self.close_asap: list[Client] = []    # replicas to disconnect at the end of this loop turn
        self._last_ping = time.monotonic()
        self._last_newline = time.monotonic()

    @property
    def offset(self) -> int:
        """Bytes of stream produced so far (master_repl_offset)."""
        return self.backlog.offset if self.backlog is not None else 0

    def feed(self, data: bytes) -> None:
        """Add bytes to the replication stream. The only way bytes enter it."""
        if self.backlog is None:
            return  # no replica has ever connected: nothing to keep
        self.backlog.feed(data)
        for c in self.replicas:
            state = c.replica.state
            if state is ReplicaState.ONLINE:
                c.outbuf += data
            elif state is not ReplicaState.WAIT_BGSAVE_START:   # WAIT_BGSAVE_END, SEND_BULK
                c.replica.pending += data
            # WAIT_BGSAVE_START: skip, the coming RDB will include it

    def info(self) -> list[str]:
        """Lines of the INFO replication section, with Redis's field names."""
        backlog = self.backlog
        now = time.time()
        lines = ["role:master", f"connected_slaves:{len(self.replicas)}"]
        for i, c in enumerate(self.replicas):
            try:
                ip = c.sock.getpeername()[0]
            except OSError:
                ip = "?"   # the connection is already going away
            lines.append(
                f"slave{i}:ip={ip},port={c.listening_port},state={INFO_STATE[c.replica.state]},"
                f"offset={c.replica.ack_offset},lag={int(now - c.replica.ack_time)}"
            )
        return lines + [
            f"master_replid:{self.replid}",
            f"master_repl_offset:{self.offset}",
            f"repl_backlog_active:{int(backlog is not None)}",
            f"repl_backlog_size:{self.config.backlog_size}",
            f"repl_backlog_first_byte_offset:{backlog.first_byte_offset if backlog else 0}",
            f"repl_backlog_histlen:{backlog.histlen if backlog else 0}",
        ]

    def cron(self, now: float) -> None:
        """Periodic replication work, every server tick. `now` is time.monotonic().

        A dead link (a pulled cable, a crashed machine) sends no FIN, so a
        replica only notices it when the master goes quiet. These two signals
        keep the link from going quiet while the master is alive.
        """
        self.start_waiting_syncs()
        if now - self._last_ping >= self.config.ping_period:
            self._last_ping = now
            if self.replicas:
                self.feed(PING)  # part of the stream: backlog, offset, pending
        if now - self._last_newline >= 1:
            self._last_newline = now
            for c in self.replicas:
                if c.replica.state in (ReplicaState.WAIT_BGSAVE_START, ReplicaState.WAIT_BGSAVE_END):
                    # Keepalive while the snapshot is being made. Not part of
                    # the stream: no backlog, no offset. Never during SEND_BULK,
                    # where it would land inside the file.
                    c.outbuf += b"\n"

    def psync(self, client: Client) -> None:
        """PSYNC: full sync only for now. Step 4 tries a partial resync first."""
        if client.replica is not None:
            return  # already a replica: ignore, as Redis does
        if self.backlog is None:
            self.backlog = Backlog(self.config.backlog_size, self.offset)
        client.replica = Replica()
        self.replicas.append(client)
        self.start_waiting_syncs()

    def start_waiting_syncs(self) -> None:
        """Start one BGSAVE for every replica waiting for one. From psync() and cron."""
        waiting = [c for c in self.replicas if c.replica.state is ReplicaState.WAIT_BGSAVE_START]
        if not waiting or child.kind is not None:
            return  # nothing to do, or the slot is busy: cron retries
        try:
            rdb.current.start_save(on_done=self.save_done)
        except OSError:
            for c in waiting:
                self.drop(c)  # fork failed: they reconnect and retry
            return
        # Read right after the fork: the RDB holds bytes 1..offset, pending starts at offset + 1.
        line = f"+FULLRESYNC {self.replid} {self.offset}\r\n".encode()
        for c in waiting:
            c.outbuf += line
            c.replica.state = ReplicaState.WAIT_BGSAVE_END

    def save_done(self, ok: bool) -> None:
        """The replication BGSAVE ended. Queue the file for every replica that waited for it."""
        for c in [c for c in self.replicas if c.replica.state is ReplicaState.WAIT_BGSAVE_END]:
            if not ok:
                self.drop(c)
                continue
            try:
                fd = os.open(rdb.current.config.path, os.O_RDONLY)
            except OSError:
                self.drop(c)
                continue
            c.replica.rdb_fd, c.replica.rdb_size = fd, os.fstat(fd).st_size
            c.outbuf += f"${c.replica.rdb_size}\r\n".encode()
            c.replica.state = ReplicaState.SEND_BULK

    def send_bulk(self, client: Client) -> None:
        """Send the next piece of dump.rdb. Called when the socket can take
        more and outbuf is empty. Raises OSError if the connection broke.

        Asks for everything left: on a non-blocking socket the kernel sends
        only what fits right now, so one call never stalls the loop.
        """
        r = client.replica
        try:
            sent = os.sendfile(client.sock.fileno(), r.rdb_fd, r.rdb_pos, r.rdb_size - r.rdb_pos)
        except BlockingIOError:
            return  # socket full: wait for the next writable event
        if sent == 0:
            raise OSError("dump.rdb ended early")  # the open file cannot shrink; fail safe
        r.rdb_pos += sent
        if r.rdb_pos < r.rdb_size:
            return  # more on the next writable event
        # The whole file is out: no "\r\n" follows it. Release the file, then
        # send the writes held back since the fork, and go live.
        os.close(r.rdb_fd)
        r.rdb_fd = None
        r.state = ReplicaState.ONLINE
        client.outbuf += r.pending
        r.pending = bytearray()
        r.ack_time = time.time()  # lag counts from here until ACKs arrive

    def drop(self, client: Client) -> None:
        """Disconnect a replica at the end of this loop turn."""
        if client not in self.close_asap:
            self.close_asap.append(client)

    def remove(self, client: Client) -> None:
        """The connection is closing: forget the replica and close its file."""
        if client.replica is None:
            return
        self.replicas.remove(client)
        if client.replica.rdb_fd is not None:
            os.close(client.replica.rdb_fd)
        client.replica = None

current: "Master | None" = None
