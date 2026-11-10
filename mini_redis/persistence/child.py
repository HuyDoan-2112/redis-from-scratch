"""
The background child: an AOF rewrite or an RDB save, run in a forked process.

Only one runs at a time, like Redis. Two at once would double the
copy-on-write memory and compete for the disk.
"""
import os
import signal
import traceback
from typing import Callable, Literal

Kind = Literal["aof", "rdb"]

kind: Kind | None = None
_pid: int | None = None
_on_done: Callable[[bool], None] | None = None

def start(child_kind: Kind, work: Callable[[], None], on_done: Callable[[bool], None]) -> None:
    """
    Fork a child that runs work() and exits. Later, check() or kill() calls
    on_done(True) in the parent if work() finished, or on_done(False) if it
    failed or was killed. The caller makes sure no child is running.
    """
    global kind, _pid, _on_done
    assert kind is None, f"a {kind} child is already running"
    pid = os.fork()
    if pid == 0:
        # Child. It must never return into the server code.
        code = 1
        try:
            work()
            code = 0
        except BaseException:
            traceback.print_exc()
        finally:
            os._exit(code)
    kind, _pid, _on_done = child_kind, pid, on_done

def check() -> None:
    """From cron: if the child has exited, clear the slot and run its on_done."""
    if _pid is None:
        return
    pid, status = os.waitpid(_pid, os.WNOHANG)
    if pid != 0:
        code = os.waitstatus_to_exitcode(status)
        if code != 0:  # negative = killed by a signal, e.g. -9 from the kernel's out-of-memory killer
            print(f"Background {kind} child failed (exit code {code})")
        _finish(code == 0)

def kill() -> None:
    """At shutdown: stop the running child, if any, wait for it, and run its on_done(False)."""
    if _pid is None:
        return
    os.kill(_pid, signal.SIGKILL)
    os.waitpid(_pid, 0)
    _finish(False)

def _finish(ok: bool) -> None:
    global kind, _pid, _on_done
    on_done = _on_done
    kind, _pid, _on_done = None, None, None
    on_done(ok)