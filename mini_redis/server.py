"""
Server lifecycle and event loop.
"""

import socket # Import the socket module for network communication
import threading

from mini_redis.config import DEFAULT_CONFIG, ServerConfig
from mini_redis.resp import RESPReader, RESPWriter, Value, RESPType
from mini_redis.handler import handlers

# handle one client connection: keep reading requests until the client disconnects
def handle_client(client_socket, config: ServerConfig):
    # loop so a single thread can serve multiple pings on the same connection
    client_file = client_socket.makefile("rwb") # create a file-like object for reading and writing bytes
    reader = RESPReader(client_file) # create a RESPReader to parse incoming commands
    writer = RESPWriter(client_file) # create a RESPWriter to send responses back to the client

    while True:
        try:
            request = reader.read()
        except EOFError or ConnectionResetError or BrokenPipeError:
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
        command = command_value.bulk.upper()
        args = request.array[1:]

        if not command:
            writer.write(Value(typ=RESPType.ERROR, string="ERR empty command"))
            continue

        handler = handlers.get(command)
        if handler is None:
            writer.write(Value(typ=RESPType.ERROR, string=f"ERR unknown command '{command}'"))
            continue

        try:
            response = handler(args) # call the handler with the command arguments
        except Exception as e:
            writer.write(Value(typ=RESPType.ERROR, string=f"ERR {str(e)}"))
            continue

        writer.write(response) # send the marshaled response back to the client

    client_file.close() # close the file-like object to release resources
    client_socket.close()  # close the socket once the client has disconnected


def run(config: ServerConfig = DEFAULT_CONFIG) -> None:
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
        client_handler = threading.Thread(target=handle_client, args=(client, config))
        client_handler.start() 


if __name__ == "__main__":
    run()
