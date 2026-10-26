"""Server configuration."""

from dataclasses import dataclass, field
from pathlib import Path

@dataclass(frozen=True)
class AOFConfig:
    """Configuration for the AOF feature of REDIS."""

    path: Path = Path("appendonly.aof")  # Default AOF file path
    enabled: bool = False  # Whether AOF is enabled
    sync_interval_secs: int = 1  # Interval in seconds to sync AOF to disk

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("AOF file path must be specified")
        if self.sync_interval_secs <= 0:
            raise ValueError("AOF sync interval must be at least 1 second")
        
@dataclass(frozen=True)
class ServerConfig:
    """Configuration values needed to start the server."""

    host: str = "127.0.0.1"
    port: int = 6379
    listen_backlog: int = 511  # Same default as Redis tcp-backlog
    recv_buffer_size: int = 1024
    aof: AOFConfig = field(default_factory=AOFConfig)

    def __post_init__(self) -> None:
        if not 0 < self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.listen_backlog < 1:
            raise ValueError("listen_backlog must be at least 1")
        if self.recv_buffer_size < 1:
            raise ValueError("recv_buffer_size must be at least 1")


DEFAULT_CONFIG = ServerConfig()
