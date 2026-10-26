"""
Server lifecycle and event loop.
"""

import io
import socket # Import the socket module for network communication
import selectors

from mini_redis.config import DEFAULT_CONFIG, ServerConfig
from mini_redis.resp import RESPReader, Value, RESPType
from mini_redis.commands.registry import COMMANDS
from mini_redis.persistence.aof import AOF
from mini_redis.client import Client


def replay_command(request: Value) -> None:
    if request.typ != RESPType.ARRAY or not request.array:
        raise ValueError("Invalid command in AOF")

    command = request.array[0].bulk.decode(errors="replace").upper()
    cmd =  COMMANDS.get(command)
    if cmd is None or not cmd.write:
        raise ValueError(f"Unexpected command in AOF: {command}")

    response = cmd.handler(request.array[1:])
    if response.typ == RESPType.ERROR:
        raise ValueError(f"Failed to replay {command}: {response.string}")

def run(config: ServerConfig = DEFAULT_CONFIG) -> None:
    aof = AOF(config.aof) if config.aof.enabled else None
    if aof is not None:
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
        except ConnectionResetError:
            close_client(sel, client)
            return
        
        if not recv_data:
            close_client(sel, client)
            return
        client.inbuf += recv_data
        process_input(client, aof)
        flush_output(sel, client)

    if mask & selectors.EVENT_WRITE:
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

    if request.typ != RESPType.ARRAY or not request.array:
        return Value(typ=RESPType.ERROR, string="ERR empty command")
        

    command_value = request.array[0]
    command = command_value.bulk.decode(errors="replace").upper()
    args = request.array[1:]

    if not command:
        return Value(typ=RESPType.ERROR, string="ERR empty command")
        
    cmd = COMMANDS.get(command)
    if cmd is None:
        return Value(typ=RESPType.ERROR, string=f"ERR unknown command '{command}'")
        
    if not cmd.arity_ok(len(request.array)):
        return Value(typ=RESPType.ERROR, string=f"ERR wrong number of arguments for '{command.lower()}' command")
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
        except (BrokenPipeError, ConnectionResetError):
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
