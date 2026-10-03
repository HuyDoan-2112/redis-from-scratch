"""State for one connected client.

TODO: Track its socket, input buffer, output buffer, and connection state.
Preserve incomplete requests and unsent reply bytes between events.
Keep every client's buffers separate.
"""
