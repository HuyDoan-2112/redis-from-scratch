"""
Server lifecycle and event loop.
"""

import errno
import io
import socket # Import the socket module for network communication
import selectors
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

def listen(config: ServerConfig) -> socket.socket:
    """Open the listening socket, or exit with a short message."""
    # create_server sets SO_REUSEADDR on Linux and macOS only. On Windows that
    # option lets a second process bind the same port, so it must stay off.
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
    # Bind before loading the AOF, so a second server on the same port exits
    # before it reads or truncates the first server's AOF file.
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

    print(f"Server listening on {config.host}:{config.port}")

    lsock.setblocking(False)
    sel.register(lsock, selectors.EVENT_READ, data=None)

    try:
        while True:
            events = sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    accept_wrapper(key.fileobj, sel)
                else:
                    service_connection(key, mask, sel, aof, config)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
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
