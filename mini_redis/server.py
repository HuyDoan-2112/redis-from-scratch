"""
Server lifecycle and event loop.
"""

import socket # Import the socket module for network communication
import threading

from mini_redis.config import DEFAULT_CONFIG, ServerConfig
from mini_redis.resp import RESPReader, RESPWriter, Value, RESPType
from mini_redis.handler import handlers, store, store_lock, COMMANDS
from mini_redis.aof import AOF

# handle one client connection: keep reading requests until the client disconnects
def handle_client(client_socket, config: ServerConfig, aof: AOF | None, write_lock: threading.Lock):
    # loop so a single thread can serve multiple pings on the same connection
    client_file = client_socket.makefile("rwb") # create a file-like object for reading and writing bytes
    reader = RESPReader(client_file) # create a RESPReader to parse incoming commands
    writer = RESPWriter(client_file) # create a RESPWriter to send responses back to the client

    while True:
        try:
            request = reader.read()
        except (EOFError, ConnectionResetError, BrokenPipeError):
            # The client disconnected, so stop handling this connection.
            break
        except Exception:
            # The bytes were not valid RESP, so return a Redis error and close this connection.
            writer.write(Value(typ=RESPType.ERROR, string="ERR malformed RESP"))
            break

        if request.typ != RESPType.ARRAY or not request.array:
            writer.write(Value(typ=RESPType.ERROR, string="ERR empty command"))
            continue

        command_value = request.array[0]
        command = command_value.bulk.decode(errors="replace").upper()
        args = request.array[1:]

        if not command:
            writer.write(Value(typ=RESPType.ERROR, string="ERR empty command"))
            continue
        
        cmd = COMMANDS.get(command)
        if cmd is None:
            writer.write(Value(typ=RESPType.ERROR, string=f"ERR unknown command '{command}'"))
            continue
        if not cmd.arity_ok(len(request.array)):
            writer.write(Value(typ=RESPType.ERROR, string=f"ERR wrong number of arguments for '{command}' command"))
            continue

        try:
            if cmd.write:
                # Same lock for running and logging, so the AOF order matches the real order.
                with write_lock:
                    response = cmd.handler(args)
                    if aof is not None and response.typ != RESPType.ERROR:
                        aof.write(request)
            else:
                response = cmd.handler(args)
        except Exception as e:
            writer.write(Value(typ=RESPType.ERROR, string=f"ERR {str(e)}"))
            continue

        writer.write(response) # send the marshaled response back to the client

    client_file.close() # close the file-like object to release resources
    client_socket.close()  # close the socket once the client has disconnected

def replay_command(request: Value) -> None:
    if request.typ != RESPType.ARRAY or not request.array:
        raise ValueError("Invalid command in AOF")

    command = request.array[0].bulk.decode(errors="replace").upper()
    cmd =  COMMANDS.get(command)
    if cmd is None and not cmd.write:
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
    write_lock = threading.Lock()      
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET = IPv4, SOCK_STREAM = TCP
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # allow immediate rebinding of the port after restart
    server.bind((config.host, config.port)) # bind the socket to the given host and port
    # we tell the server to start listening with
    # a maximum backlog of connections set in config
    server.listen(config.listen_backlog)

    print(f"Server listening on {config.host}:{config.port}")

    while True: # accept loop: handle each new client in its own thread
        # When a client connects we receive the
        # client socket into the client variable, and
        # the remote connection details into the addr variable
        client, addr = server.accept()
        print(f"[+] Accepted connection from {addr[0]}:{addr[1]}")
        # spin up our client thread to handle the new connection
        client_handler = threading.Thread(target=handle_client, args=(client, config, aof, write_lock))
        client_handler.start() 


if __name__ == "__main__":
    run()
