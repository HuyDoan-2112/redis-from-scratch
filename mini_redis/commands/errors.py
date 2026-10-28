class CommandError(Exception):
    """A request that cannot run. The message is the RESP error to send."""