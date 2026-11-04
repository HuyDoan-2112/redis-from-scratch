"""Server configuration."""

from dataclasses import dataclass, field
from pathlib import Path

FSYNC_POLICIES = ("always", "everysec", "no")

@dataclass(frozen=True)
class AOFConfig:
    """Configuration for the AOF feature of REDIS."""

    path: Path = Path("appendonly.aof")  # Default AOF file path
    enabled: bool = False  # Whether AOF is enabled
    sync_interval_secs: int = 1  # Interval in seconds to sync AOF to disk
    appendfsync: str = "everysec" # When to fsync: see FSYNC_POLICIES
    
    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("AOF file path must be specified")
        if self.sync_interval_secs <= 0:
            raise ValueError("AOF sync interval must be at least 1 second")
        if self.appendfsync not in FSYNC_POLICIES:
            raise ValueError(f"appendfsync must be one of {', '.join(FSYNC_POLICIES)}, got {self.appendfsync!r}")

@dataclass(frozen=True)
class ServerConfig:
    """Configuration values needed to start the server."""

    host: str = "127.0.0.1"
    port: int = 6379
    listen_backlog: int = 511  # Same default as Redis tcp-backlog
    recv_buffer_size: int = 16 * 1024  # Max bytes read per recv() call, same as Redis PROTO_IOBUF_LEN
    hz: int = 10  # How often to run the server's periodic tasks, in times per second
    aof: AOFConfig = field(default_factory=AOFConfig)

    def __post_init__(self) -> None:
        if not 0 < self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.listen_backlog < 1:
            raise ValueError("listen_backlog must be at least 1")
        if self.recv_buffer_size < 1:
            raise ValueError("recv_buffer_size must be at least 1")
        if not 1 <= self.hz <= 500:
            raise ValueError("hz must be between 1 and 500")

DEFAULT_CONFIG = ServerConfig()
