"""
Server lifecycle and event loop.
"""

import io
import socket # Import the socket module for network communication
import selectors
import signal

from mini_redis.config import DEFAULT_CONFIG, ServerConfig
from mini_redis.resp import RESPReader, Value, RESPType
from mini_redis.commands.registry import CommandError, lookup
from mini_redis.persistence.aof import AOF
from mini_redis.client import Client


def replay_command(request: Value) -> None:
    """Run one command read back from the AOF at startup.

    Only write commands are ever logged, and every logged command succeeded
    the first time, so anything else means the file is broken: stop instead of
    starting with wrong data.
    """
    cmd = lookup(request)
    if not cmd.write:
        raise ValueError(f"Unexpected command in AOF: {cmd.name}")

    response = cmd.handler(request.array[1:])
    if response.typ == RESPType.ERROR:
        raise ValueError(f"Failed to replay {cmd.name}: {response.string}")

# Signals that stop the server cleanly.
SHUTDOWN_SIGNALS = [signal.SIGINT, signal.SIGTERM]

def run(config: ServerConfig = DEFAULT_CONFIG) -> None:
    """Load the AOF, then serve clients until SIGINT or SIGTERM arrives."""
    aof = AOF(config.aof) if config.aof.enabled else None
    if aof is not None:
        # Rebuild the data before accepting clients, so nobody sees a
        # half-loaded store.
        try:
            aof.read(replay_command)
        except Exception:
            aof.close()
            raise
    sel = selectors.DefaultSelector()
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET = IPv4, SOCK_STREAM = TCP
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # allow immediate rebinding of the port after restart
    lsock.bind((config.host, config.port)) # bind the socket to the given host and port

    # a maximum backlog of connections set in config
    lsock.listen(config.listen_backlog)
    # Non-blocking, so accept() can never stall the loop for other clients.
    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ)

    # Signals write a byte to wake_w, so select() returns at once.
    wake_r, wake_w = socket.socketpair()
    wake_r.setblocking(False)
    wake_w.setblocking(False)
    sel.register(wake_r, selectors.EVENT_READ)

    # Signal handlers only record the signal. The loop below checks
    # `received` and does the real shutdown work outside the handler.
    received: list[int] = []
    old_handlers = {
        sig: signal.signal(sig, lambda signum, frame: received.append(signum))
        for sig in SHUTDOWN_SIGNALS
    }
    old_wakeup_fd = signal.set_wakeup_fd(wake_w.fileno())

    print(f"Server listening on {config.host}:{config.port}")

    try:
        while not received:
            # Block until at least one socket is ready. `mask` says whether it
            # is ready to read, to write, or both.
            for key, mask in sel.select():
                if key.fileobj is lsock:
                    accept_wrapper(lsock, sel)
                elif key.fileobj is wake_r:
                    drain(wake_r)
                else:
                    service_connection(key, mask, sel, aof, config)
        print(f"Received {signal.Signals(received[0]).name}, shutting down")
    finally:
        # Put the old signal setup back, so tests that call run() in a
        # thread or several times do not leak handlers.
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
    """Accept one new connection and start watching it for incoming data."""
    try:
        conn, addr = sock.accept()
    except BlockingIOError:
        return  # another wakeup already took the connection
    except ConnectionAbortedError:
        return  # the client hung up before we accepted it

    print(f"[+] Accepted connection from {addr[0]}:{addr[1]}")
    conn.setblocking(False)
    client = Client(sock=conn)
    # Watch for reads only. EVENT_WRITE is added later, only while there are
    # unsent replies, otherwise select() would wake up all the time.
    events = selectors.EVENT_READ
    sel.register(conn, events, data=client)

def service_connection(
        key: selectors.SelectorKey, 
        mask: int, 
        sel: selectors.BaseSelector, 
        aof: AOF | None,
        config: ServerConfig
) -> None:
    """Handle one ready client: read and run commands, or send pending replies."""
    client: Client = key.data
    sock = client.sock
    if mask & selectors.EVENT_READ:
        try:
            recv_data = sock.recv(config.recv_buffer_size)
        except BlockingIOError:
            return  # woke up, but no data yet
        except OSError:
            close_client(sel, client)  # e.g. connection reset by the client
            return

        if not recv_data:
            close_client(sel, client)  # b"" means the client closed its side
            return
        # One recv() can hold half a command or several commands, so always
        # add to the buffer and let process_input() cut out whole commands.
        client.inbuf += recv_data
        process_input(client, aof)
        flush_output(sel, client)
    elif mask & selectors.EVENT_WRITE:
        flush_output(sel, client)

def process_input(client: Client, aof: AOF | None) -> None:
    """Run every complete command in the input buffer, in order.

    Replies go to the output buffer. An incomplete command at the end stays in
    the buffer until more bytes arrive.
    """
    if client.closing:
        return  # after a protocol error, ignore anything else the client sends

    while True:
        try:
            bio = io.BytesIO(client.inbuf)
            request = RESPReader(bio).read()
        except EOFError:
            break  # the buffer ends mid-command: wait for the rest
        except ValueError:
            # Bad bytes. We cannot know where the next command starts, so
            # reply once and close, as Redis does.
            client.outbuf += b"-ERR Protocol error\r\n"
            client.closing = True
            break

        # bio.tell() is how many bytes the parser used for this one command.
        del client.inbuf[:bio.tell()]
        client.outbuf += execute(request, aof).marshal()


def close_client(sel: selectors.BaseSelector, client: Client) -> None:
    # Unregister first: the selector cannot look up a socket that is closed.
    sel.unregister(client.sock)
    client.sock.close()

def execute(request: Value, aof: AOF | None) -> Value:
    """Run one command and return its reply. Never raises."""
    try:
        cmd = lookup(request)
    except CommandError as e:
        return Value(typ=RESPType.ERROR, string=str(e))

    args = request.array[1:]  # drop the command name
    try:
        response = cmd.handler(args)
        # Log only writes that succeeded. A failed write changed nothing, so
        # replaying it would only fail again at startup.
        if cmd.write and aof is not None and response.typ != RESPType.ERROR:
            aof.write(request)
    except Exception as e:
        # A bug in one handler must not crash the server for every client.
        return Value(typ=RESPType.ERROR, string=f"ERR {str(e)}")

    return response

def flush_output(sel: selectors.BaseSelector, client: Client) -> None:
    """Send what we can now, then choose which events to wait for next."""
    # Send what the socket accepts right now.
    if client.outbuf:
        try:
            # send() may take only part of the buffer (a partial write).
            # Keep the rest for the next EVENT_WRITE.
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
    # modify() is a system call (epoll_ctl), so skip it when nothing changes.
    if sel.get_key(client.sock).events != events:
        sel.modify(client.sock, events, data=client)

if __name__ == "__main__":
    run()
