"""Server lifecycle and event loop.

TODO: Create, bind, and listen on a non-blocking TCP socket.
Use a selector to accept clients, receive bytes, and send pending replies.
Own the client registry and clean up resources on failure or shutdown.
"""

import socket # Import the socket module for network communication
import threading # Import the threading module to handle multiple clients concurrently

from mini_redis.config import DEFAULT_CONFIG, ServerConfig


# handle one client connection: keep reading requests until the client disconnects
def handle_client(client_socket, config: ServerConfig):
    # loop so a single thread can serve multiple pings on the same connection
    while True:
        # receive data from the client; recv blocks until bytes arrive
        request = client_socket.recv(config.recv_buffer_size) # recv method receives data from the client socket, with a maximum buffer size of recv_buffer_size bytes
        # an empty read means the client closed the connection, so stop looping
        if not request:
            break
        print(f"[+] Received: {request}")
        # redis-cli expects a RESP simple string reply for PING: +PONG\r\n
        client_socket.send(b"+PONG\r\n")
    # close the socket once the client has disconnected
    client_socket.close()


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
