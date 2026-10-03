"""Server configuration."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ServerConfig:
    """Configuration values needed to start the server."""

    host: str = "127.0.0.1"
    port: int = 6379
    listen_backlog: int = 5
    recv_buffer_size: int = 1024

    def __post_init__(self) -> None:
        if not 0 < self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.listen_backlog < 1:
            raise ValueError("listen_backlog must be at least 1")
        if self.recv_buffer_size < 1:
            raise ValueError("recv_buffer_size must be at least 1")


DEFAULT_CONFIG = ServerConfig()
