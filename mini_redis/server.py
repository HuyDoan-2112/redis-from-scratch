"""
Server lifecycle and event loop.
"""

import errno
import io
import socket # Import the socket module for network communication
import selectors
import signal
import sys

from mini_redis.config import DEFAULT_CONFIG, ServerConfig
from mini_redis.resp import RESPReader, Value, RESPType
from mini_redis.commands.registry import CommandError, lookup
from mini_redis.persistence.aof import AOF
from mini_redis.client import Client


def replay_command(request: Value) -> None:
    cmd = lookup(request)
    if not cmd.write:
        raise ValueError(f"Unexpected command in AOF: {cmd.name}")

    response = cmd.handler(request.array[1:])
    if response.typ == RESPType.ERROR:
        raise ValueError(f"Failed to replay {cmd.name}: {response.string}")

# Signals that stop the server cleanly. SIGBREAK is Ctrl+Break on Windows.
SHUTDOWN_SIGNALS = [signal.SIGINT, signal.SIGTERM]
if hasattr(signal, "SIGBREAK"):
    SHUTDOWN_SIGNALS.append(signal.SIGBREAK)

def listen(config: ServerConfig) -> socket.socket:
    """Open the listening socket, or exit with a short message."""
    # No SO_REUSEADDR on Windows: there it lets two servers share a port.
    try:
        return socket.create_server(
            (config.host, config.port), backlog=config.listen_backlog
        )
    except OSError as err:
        if err.errno == errno.EADDRINUSE:
            sys.exit(f"Could not listen on {config.host}:{config.port}: "
                     f"port {config.port} is already in use")
        sys.exit(f"Could not listen on {config.host}:{config.port}: {err}")

def run(config: ServerConfig = DEFAULT_CONFIG) -> None:
    # Bind first, so a second server exits before touching the AOF.
    lsock = listen(config)

    aof = AOF(config.aof) if config.aof.enabled else None
    if aof is not None:
        try:
            aof.read(replay_command)
        except Exception:
            aof.close()
            lsock.close()
            raise
    sel = selectors.DefaultSelector()
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ)

    # Signals write a byte to wake_w, so select() returns at once (even on Windows).
    wake_r, wake_w = socket.socketpair()
    wake_r.setblocking(False)
    wake_w.setblocking(False)
    sel.register(wake_r, selectors.EVENT_READ)

    received: list[int] = []
    old_handlers = {
        sig: signal.signal(sig, lambda signum, frame: received.append(signum))
        for sig in SHUTDOWN_SIGNALS
    }
    old_wakeup_fd = signal.set_wakeup_fd(wake_w.fileno())

    print(f"Server listening on {config.host}:{config.port}")

    try:
        while not received:
            for key, mask in sel.select():
                if key.fileobj is lsock:
                    accept_wrapper(lsock, sel)
                elif key.fileobj is wake_r:
                    drain(wake_r)
                else:
                    service_connection(key, mask, sel, aof, config)
        print(f"Received {signal.Signals(received[0]).name}, shutting down")
    finally:
        signal.set_wakeup_fd(old_wakeup_fd)
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        shutdown(sel, aof)
        wake_w.close()


def drain(sock: socket.socket) -> None:
    """Read and drop all waiting bytes."""
    try:
        while sock.recv(4096):
            pass
    except BlockingIOError:
        pass


def shutdown(sel: selectors.BaseSelector, aof: AOF | None) -> None:
    """Save the AOF, then close all sockets."""
    if aof is not None:
        aof.close()  # flush and fsync
    for key in list(sel.get_map().values()):
        sel.unregister(key.fileobj)
        key.fileobj.close()
    sel.close()

def accept_wrapper(sock: socket.socket, sel: selectors.BaseSelector) -> None:
    try:
        conn, addr = sock.accept()
    except BlockingIOError:
        return 
    except ConnectionAbortedError:
        return

    print(f"[+] Accepted connection from {addr[0]}:{addr[1]}")
    conn.setblocking(False)
    client = Client(sock=conn)
    events = selectors.EVENT_READ 
    sel.register(conn, events, data=client)

def service_connection(
        key: selectors.SelectorKey, 
        mask: int, 
        sel: selectors.BaseSelector, 
        aof: AOF | None,
        config: ServerConfig
) -> None:
    
    client: Client = key.data
    sock = client.sock
    if mask & selectors.EVENT_READ:
        try:
            recv_data = sock.recv(config.recv_buffer_size)
        except BlockingIOError:
            return
        except OSError:
            close_client(sel, client)
            return
        
        if not recv_data:
            close_client(sel, client)
            return
        client.inbuf += recv_data
        process_input(client, aof)
        flush_output(sel, client)
    elif mask & selectors.EVENT_WRITE:
        flush_output(sel, client)

def process_input(client: Client, aof: AOF | None) -> None:
    if client.closing:
        return
    
    while True:
        try:
            bio = io.BytesIO(client.inbuf)
            request = RESPReader(bio).read()
        except EOFError:
            break
        except ValueError:
            client.outbuf += b"-ERR Protocol error\r\n"
            client.closing = True
            break

        del client.inbuf[:bio.tell()]
        client.outbuf += execute(request, aof).marshal()


def close_client(sel: selectors.BaseSelector, client: Client) -> None:
    sel.unregister(client.sock)
    client.sock.close()

def execute(request: Value, aof: AOF | None) -> Value:
    try:
        cmd = lookup(request)
    except CommandError as e:
        return Value(typ=RESPType.ERROR, string=str(e))
    
    args = request.array[1:]
    try:
        response = cmd.handler(args)
        if cmd.write and aof is not None and response.typ != RESPType.ERROR:
            aof.write(request)
    except Exception as e:
        return Value(typ=RESPType.ERROR, string=f"ERR {str(e)}")

    return response

def flush_output(sel: selectors.BaseSelector, client: Client) -> None:
    # Send what the socket accepts right now.
    if client.outbuf:
        try:
            sent = client.sock.send(client.outbuf)
            del client.outbuf[:sent]
        except BlockingIOError:
            pass # socket full: nothing sent this time
        except OSError:
            close_client(sel, client)
            return

    # Decide what to wait for next
    if client.outbuf:
        if client.closing:
            set_events(sel, client, selectors.EVENT_WRITE)  # only wait to send
        else:
            set_events(sel, client, selectors.EVENT_READ | selectors.EVENT_WRITE)
    elif client.closing:
        close_client(sel, client)
    else:
        set_events(sel, client, selectors.EVENT_READ)

def set_events(sel: selectors.BaseSelector, client: Client, events: int) -> None:
    if sel.get_key(client.sock).events != events:
        sel.modify(client.sock, events, data=client)

if __name__ == "__main__":
    run()
