#!/usr/bin/env python3
"""Linux TCP echo worker used by the FD/EMFILE drill API.

The process intentionally lowers RLIMIT_NOFILE, keeps one reserved descriptor,
and opens sample log files until its descriptor table is full.  A full table
makes a real accept(2) call fail with EMFILE.  The network I/O thread then uses
its reserved descriptor to accept and immediately close the one probe
connection, giving the client EOF/RST promptly without a polling busy loop.

Concurrency model (drill requirement: at most 4 worker threads):

* exactly one network I/O thread multiplexes the listener and every TCP
  connection through ``select(2)`` -- no per-connection threads, so dozens of
  simultaneous sampled connections cost only one thread;
* the main thread is the blocking JSON control loop on stdin/stdout;
* every connection owns an ordered send queue.  A connection that stops
  reading only applies backpressure to itself: ``send(2)`` returns EAGAIN, the
  unsent bytes stay in that connection's queue in their original order, and
  every other connection keeps being serviced.  When the slow reader drains
  its socket again it receives the complete echo in the original order.

The only control channel is line-delimited JSON on stdin/stdout.  All logs go
to stderr so stdout remains a protocol stream.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import resource
import select
import socket
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


EMFILE = errno.EMFILE

# Hard contractual upper bound on the number of threads this process may
# create.  The actual steady-state count is two: control thread + I/O thread.
MAX_WORKER_THREADS = int(os.getenv("DRILL_MAX_THREADS", "4"))


def log(message: str) -> None:
    print(f"[echo-worker pid={os.getpid()}] {message}", file=sys.stderr, flush=True)


def lower_nofile_limit(limit: int) -> dict[str, int]:
    """Lower this process's NOFILE hard and soft limits."""
    soft_before, hard_before = resource.getrlimit(resource.RLIMIT_NOFILE)

    if limit < 8:
        raise ValueError("nofile limit must be at least 8")

    if hard_before == resource.RLIM_INFINITY:
        hard_after = limit
    else:
        hard_after = min(int(hard_before), limit)
    soft_after = min(limit, hard_after)

    resource.setrlimit(resource.RLIMIT_NOFILE, (soft_after, hard_after))
    soft_now, hard_now = resource.getrlimit(resource.RLIMIT_NOFILE)
    return {
        "requested": limit,
        "soft_before": int(soft_before),
        "hard_before": int(hard_before),
        "soft": int(soft_now),
        "hard": int(hard_now),
    }


def count_open_fds() -> tuple[int, str | None]:
    """Return the number of open descriptors.

    Listing /proc/self/fd itself needs a descriptor.  If that fails with
    EMFILE, the table is full and its capacity is the soft RLIMIT_NOFILE.
    """
    try:
        return len(os.listdir("/proc/self/fd")), None
    except OSError as exc:
        if exc.errno == EMFILE:
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            return int(soft), "procfs listing returned EMFILE; table is full"
        return -1, f"procfs listing failed with errno={exc.errno}"


@dataclass
class Metrics:
    lock: threading.Lock = field(default_factory=threading.Lock)
    started_at: float = field(default_factory=time.time)
    emfile_total: int = 0
    accepted_total: int = 0
    reaped_after_emfile_total: int = 0
    recovery_accept_emfile_total: int = 0
    active_connections: int = 0
    max_active_connections: int = 0
    connections_closed_total: int = 0
    echo_bytes_total: int = 0
    echo_chunks_total: int = 0
    last_emfile_at: float | None = None
    last_emfile_errno: int | None = None
    last_recovery_error: str | None = None
    other_accept_errors_total: int = 0

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "started_at_unix": self.started_at,
                "uptime_seconds": round(time.time() - self.started_at, 3),
                "emfile_total": self.emfile_total,
                "accepted_total": self.accepted_total,
                "reaped_after_emfile_total": self.reaped_after_emfile_total,
                "recovery_accept_emfile_total": self.recovery_accept_emfile_total,
                "active_connections": self.active_connections,
                "max_active_connections": self.max_active_connections,
                "connections_closed_total": self.connections_closed_total,
                "echo_bytes_total": self.echo_bytes_total,
                "echo_chunks_total": self.echo_chunks_total,
                "last_emfile_at_unix": self.last_emfile_at,
                "seconds_since_last_emfile": (
                    None
                    if self.last_emfile_at is None
                    else round(time.time() - self.last_emfile_at, 6)
                ),
                "last_emfile_errno": self.last_emfile_errno,
                "last_recovery_error": self.last_recovery_error,
                "other_accept_errors_total": self.other_accept_errors_total,
            }


@dataclass
class ConnectionState:
    """Per-connection state owned exclusively by the network I/O thread."""

    conn: socket.socket
    addr: tuple[str, int]
    conn_id: int
    created_at: float = field(default_factory=time.time)
    # Ordered queue of chunks waiting to be echoed.  ``send_offset`` is the
    # number of bytes already written from the head chunk.  Byte order is
    # therefore preserved exactly, including across EAGAIN backpressure.
    send_queue: deque[bytes] = field(default_factory=deque)
    send_offset: int = 0
    read_closed: bool = False
    rx_bytes: int = 0
    tx_bytes: int = 0
    recv_chunks: int = 0
    echo_chunks: int = 0

    def queued_bytes(self) -> int:
        if not self.send_queue:
            return 0
        return sum(len(chunk) for chunk in self.send_queue) - self.send_offset


class EchoWorker:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        sample_dir: Path,
        backlog: int = 64,
        sample_content_bytes: int = 512,
    ) -> None:
        self.host = host
        self.requested_port = port
        self.sample_dir = sample_dir
        self.backlog = backlog
        self.sample_content_bytes = sample_content_bytes

        self.metrics = Metrics()
        self.stop_event = threading.Event()
        self.fd_lock = threading.Lock()
        self.tid_ready = threading.Event()
        # Key name kept for API/procfs compatibility; the single network I/O
        # thread now performs accept *and* all connection echo I/O.
        self.accept_thread_tid: int | None = None

        self.server: socket.socket | None = None
        self.bound_port: int | None = None
        self.rescue_fd: int | None = None

        # fd -> ConnectionState; touched only by the I/O thread outside close().
        self.connections: dict[int, ConnectionState] = {}
        self._next_conn_id = 1

        self.sample_handles: list[Any] = []
        self.sample_records: list[dict[str, Any]] = []
        self.fill_error: dict[str, Any] | None = None

    def start_socket(self) -> dict[str, Any]:
        self.sample_dir.mkdir(parents=True, exist_ok=True)

        server = socket.socket(socket.AF_INET6 if ":" in self.host else socket.AF_INET,
                               socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Dual-stack when binding "::"; harmless for the default 0.0.0.0.
        if server.family == socket.AF_INET6:
            try:
                server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except OSError:
                pass
        server.bind((self.host, self.requested_port))
        server.listen(self.backlog)
        server.setblocking(False)
        self.server = server
        self.bound_port = server.getsockname()[1]

        # This descriptor both proves that the table is full and provides the
        # one slot used to accept and close an EMFILE probe.
        self.rescue_fd = os.open(os.devnull, os.O_RDONLY | os.O_CLOEXEC)

        threading.current_thread().name = "fd-drill-control"
        thread = threading.Thread(
            target=self._io_loop, name="fd-drill-network-io", daemon=True
        )
        thread.start()
        if not self.tid_ready.wait(timeout=2.0):
            raise RuntimeError("network I/O thread did not report its thread id")

        return {
            "host": self.host,
            "port": self.bound_port,
            "listener_fd": server.fileno(),
            "rescue_fd": self.rescue_fd,
            "accept_thread_tid": self.accept_thread_tid,
            "io_thread_tid": self.accept_thread_tid,
        }

    def close(self) -> None:
        self.stop_event.set()
        try:
            if self.server is not None:
                self.server.close()
        finally:
            # The I/O thread observes stop_event and exits its select() loop;
            # give it a moment, then close any connections still registered.
            deadline = time.monotonic() + 0.5
            while self.connections and time.monotonic() < deadline:
                time.sleep(0.02)
            for state in list(self.connections.values()):
                self._close_connection(state)
            self._close_samples()
            if self.rescue_fd is not None:
                try:
                    os.close(self.rescue_fd)
                except OSError:
                    pass
                self.rescue_fd = None

    # ---------- sample log descriptors ----------

    def _write_sample_file(self, index: int, size_bytes: int) -> Path:
        # A nanosecond stamp keeps repeated exhaust/release cycles on distinct
        # paths without deleting log evidence from the mounted directory.
        batch = f"{time.time_ns()}-{os.getpid()}"
        name = f"sample-{batch}-{index:04d}.log"
        path = self.sample_dir / name
        line = (
            f"{time.strftime('%Y-%m-%dT%H:%M:%S')} sample logger "
            f"batch={batch} index={index} "
        ).encode()
        payload = line + (b"x" * max(0, size_bytes - len(line) - 1)) + b"\n"
        if size_bytes >= len(line) + 1:
            path.write_bytes(payload)
        else:
            path.write_bytes(line[: max(0, size_bytes)])
        return path

    def _open_one_sample(self, *, size_bytes: int) -> dict[str, Any]:
        index = len(self.sample_handles) + 1
        path = self._write_sample_file(index, size_bytes)
        # Serialize descriptor allocation with EMFILE rescue handling.
        with self.fd_lock:
            handle = open(path, "r+", encoding="utf-8", errors="replace")
            fd = handle.fileno()
            self.sample_handles.append(handle)
            record = {
                "index": index,
                "path": str(path),
                "fd": fd,
                "size_bytes": path.stat().st_size,
            }
            self.sample_records.append(record)
            return record

    def open_samples(
        self,
        *,
        count: int | None = None,
        fill_to_limit: bool = False,
        file_size_bytes: int | None = None,
    ) -> dict[str, Any]:
        size = (
            self.sample_content_bytes
            if file_size_bytes is None
            else max(0, int(file_size_bytes))
        )
        opened: list[dict[str, Any]] = []
        self.fill_error = None

        if count is not None:
            for _ in range(max(0, int(count))):
                try:
                    opened.append(self._open_one_sample(size_bytes=size))
                except OSError as exc:
                    self.fill_error = {
                        "operation": "open",
                        "errno": exc.errno,
                        "error": exc.strerror,
                    }
                    if exc.errno != EMFILE:
                        raise
                    break

        if fill_to_limit:
            # RLIMIT_NOFILE bounds descriptor numbers/count; this loop cannot
            # usefully exceed it and the extra cap guards against odd limits.
            soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            safety_cap = int(soft_limit) + 8
            while len(self.sample_handles) < safety_cap:
                try:
                    opened.append(self._open_one_sample(size_bytes=size))
                except OSError as exc:
                    self.fill_error = {
                        "operation": "open",
                        "errno": exc.errno,
                        "error": exc.strerror,
                    }
                    if exc.errno != EMFILE:
                        raise
                    break

        fd_count, fd_note = count_open_fds()
        return {
            "opened_this_call": len(opened),
            "open_sample_files": len(self.sample_handles),
            "sample_files": list(self.sample_records),
            "fill_error": self.fill_error,
            "open_fd_count": fd_count,
            "fd_count_note": fd_note,
            "fd_table_full": fd_note is not None and "EMFILE" in fd_note,
        }

    def _close_samples(self) -> dict[str, Any]:
        closed = len(self.sample_handles)
        errors: list[str] = []
        with self.fd_lock:
            for handle in self.sample_handles:
                try:
                    handle.close()
                except OSError as exc:
                    errors.append(str(exc))
            self.sample_handles.clear()
            records = self.sample_records
            self.sample_records = []
            self.fill_error = None
        return {"closed_sample_files": closed, "errors": errors, "closed_paths": [r["path"] for r in records]}

    def release_samples(self) -> dict[str, Any]:
        result = self._close_samples()
        fd_count, note = count_open_fds()
        result.update(
            {
                "open_sample_files": 0,
                "open_fd_count": fd_count,
                "fd_count_note": note,
                "listener_fd": self.server.fileno() if self.server is not None else None,
                "listener_accepting": self.server is not None,
            }
        )
        return result

    # ---------- single-threaded multiplexed TCP echo ----------

    def _close_connection(self, state: ConnectionState) -> None:
        """Remove and close a connection.  Safe from the I/O thread and close()."""
        self.connections.pop(state.conn.fileno(), None)
        try:
            state.conn.close()
        except OSError:
            pass
        with self.metrics.lock:
            self.metrics.active_connections -= 1
            self.metrics.connections_closed_total += 1

    def _accept_pending(self) -> None:
        """Drain the listener backlog; handle a real EMFILE exactly as before."""
        assert self.server is not None
        server = self.server
        while not self.stop_event.is_set():
            try:
                conn, addr = server.accept()
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    return
                if exc.errno == EMFILE:
                    with self.metrics.lock:
                        self.metrics.emfile_total += 1
                        self.metrics.last_emfile_at = time.time()
                        self.metrics.last_emfile_errno = exc.errno
                    self._recover_from_emfile()
                    # Always return to select(); see _recover_from_emfile and
                    # the comment in the previous threaded implementation:
                    # retrying accept() immediately on a full table can spin.
                    return
                if self.stop_event.is_set():
                    return
                with self.metrics.lock:
                    self.metrics.other_accept_errors_total += 1
                    self.metrics.last_recovery_error = f"accept: {exc}"
                self.stop_event.wait(0.1)
                return

            conn.setblocking(False)
            try:
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            state = ConnectionState(
                conn=conn,
                addr=addr,
                conn_id=self._next_conn_id,
            )
            self._next_conn_id += 1
            self.connections[conn.fileno()] = state
            with self.metrics.lock:
                self.metrics.accepted_total += 1
                self.metrics.active_connections += 1
                self.metrics.max_active_connections = max(
                    self.metrics.max_active_connections,
                    self.metrics.active_connections,
                )

    def _handle_readable(self, state: ConnectionState) -> None:
        """Drain everything currently readable into the ordered send queue."""
        sock = state.conn
        # Per-wakeup budget: a flooding peer must not starve the other
        # connections multiplexed by this single I/O thread.  Remaining bytes
        # stay in the kernel socket buffer and trigger another readable event.
        budget = 32
        while budget > 0:
            try:
                chunk = sock.recv(65536)
            except BlockingIOError:
                # Kernel receive buffer drained; wait for the next wakeup.
                return
            except OSError:
                self._close_connection(state)
                return
            if not chunk:
                # Peer half-closed.  Flush queued echo first, then close.
                state.read_closed = True
                if state.queued_bytes() == 0:
                    self._close_connection(state)
                return
            budget -= 1
            state.rx_bytes += len(chunk)
            state.recv_chunks += 1
            # Enqueue the exact bytes; FIFO order guarantees an in-order,
            # complete echo even if this connection is currently write-blocked.
            state.send_queue.append(chunk)

    def _handle_writable(self, state: ConnectionState) -> None:
        """Push as much of the queued echo as the socket currently accepts."""
        sock = state.conn
        while state.queued_bytes() > 0:
            head = state.send_queue[0]
            view = memoryview(head)[state.send_offset :]
            try:
                written = sock.send(view)
            except BlockingIOError:
                # Backpressure: leave the queue untouched and try next wakeup.
                return
            except OSError:
                # Reset/peer gone: drop this connection only.
                self._close_connection(state)
                return
            if written <= 0:
                return
            state.send_offset += written
            state.tx_bytes += written
            if state.send_offset >= len(head):
                state.send_queue.popleft()
                state.send_offset = 0
                state.echo_chunks += 1
                with self.metrics.lock:
                    self.metrics.echo_chunks_total += 1
                    self.metrics.echo_bytes_total += len(head)

        if state.read_closed:
            # All queued data echoed after the peer's FIN: close gracefully.
            self._close_connection(state)

    def _io_loop(self) -> None:
        """One thread multiplexes listener readiness and all connection I/O."""
        self.accept_thread_tid = threading.get_native_id()
        self.tid_ready.set()
        assert self.server is not None
        server = self.server

        while not self.stop_event.is_set():
            read_list: list[socket.socket] = [server]
            write_list: list[socket.socket] = []
            for state in self.connections.values():
                if not state.read_closed:
                    read_list.append(state.conn)
                if state.queued_bytes() > 0:
                    write_list.append(state.conn)

            try:
                # Blocking wait with a short timeout so stop_event is noticed
                # promptly; with no events the thread sleeps in the kernel and
                # consumes no CPU (no polling / no busy-wait).
                readable, writable, errored = select.select(
                    read_list, write_list, read_list + write_list, 0.25
                )
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                if self.stop_event.is_set():
                    break
                with self.metrics.lock:
                    self.metrics.other_accept_errors_total += 1
                    self.metrics.last_recovery_error = f"select: {exc}"
                self.stop_event.wait(0.25)
                continue

            if server in readable:
                self._accept_pending()

            for sock in readable:
                if sock is server:
                    continue
                state = self.connections.get(sock.fileno())
                if state is not None:
                    self._handle_readable(state)

            for sock in writable:
                state = self.connections.get(sock.fileno())
                if state is not None:
                    self._handle_writable(state)

            for sock in errored:
                state = self.connections.get(sock.fileno())
                if state is not None:
                    self._close_connection(state)

    def _recover_from_emfile(self) -> str:
        """Use the reserved slot to reap and close one waiting probe.

        Returns "reaped", "empty", or "emfile".  The caller returns to
        select(); a pathological "emfile" recovery also backs off briefly so a
        lost rescue descriptor can never create a tight retry loop.
        """
        assert self.server is not None
        outcome = "empty"
        with self.fd_lock:
            if self.rescue_fd is not None:
                try:
                    os.close(self.rescue_fd)
                except OSError:
                    pass
                self.rescue_fd = None

            try:
                try:
                    conn, _addr = self.server.accept()
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        outcome = "empty"
                    elif exc.errno == EMFILE:
                        outcome = "emfile"
                        with self.metrics.lock:
                            # A real recovery accept(2) EMFILE is still counted.
                            self.metrics.emfile_total += 1
                            self.metrics.recovery_accept_emfile_total += 1
                            self.metrics.last_emfile_at = time.time()
                            self.metrics.last_emfile_errno = exc.errno
                            self.metrics.last_recovery_error = (
                                "accept with rescue slot returned EMFILE"
                            )
                    else:
                        raise
                else:
                    outcome = "reaped"
                    with self.metrics.lock:
                        self.metrics.reaped_after_emfile_total += 1
                    try:
                        # No read/echo: a descriptor is unavailable for a normal
                        # long-lived session.  A plain close produces EOF; unread
                        # probe data can make Linux send RST, also accepted.
                        conn.close()
                    except OSError:
                        pass
            finally:
                if self.rescue_fd is None:
                    try:
                        self.rescue_fd = os.open(
                            os.devnull, os.O_RDONLY | os.O_CLOEXEC
                        )
                    except OSError as exc:
                        outcome = "emfile"
                        with self.metrics.lock:
                            self.metrics.last_recovery_error = (
                                f"could not recreate rescue fd: {exc}"
                            )
            if outcome == "emfile":
                # Defensive backoff if the reserved slot could not be restored.
                self.stop_event.wait(0.05)
        return outcome

    # ---------- introspection ----------

    def _thread_info(self) -> tuple[int, list[dict[str, Any]]]:
        # threading.enumerate() needs no descriptor, so thread accounting keeps
        # working even when the FD table is completely full (unlike /proc).
        threads = [
            {"tid": thread.native_id, "name": thread.name}
            for thread in threading.enumerate()
            if thread.native_id is not None
        ]
        return len(threads), threads

    def _connection_summary(self) -> dict[str, Any]:
        states = list(self.connections.values())
        queued = [state.queued_bytes() for state in states]
        details = [
            {
                "conn_id": state.conn_id,
                "fd": state.conn.fileno(),
                "peer": f"{state.addr[0]}:{state.addr[1]}",
                "rx_bytes": state.rx_bytes,
                "tx_bytes": state.tx_bytes,
                "queued_bytes": state.queued_bytes(),
                "read_closed": state.read_closed,
                "age_seconds": round(time.time() - state.created_at, 3),
            }
            for state in sorted(states, key=lambda item: item.conn_id)[:30]
        ]
        return {
            "active": len(states),
            "send_queue_bytes_total": sum(queued),
            "send_queue_max_bytes": max(queued, default=0),
            "details": details,
        }

    # ---------- control plane ----------

    def status(self, rlimit_info: dict[str, int]) -> dict[str, Any]:
        fd_count, note = count_open_fds()
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        with self.fd_lock:
            sample_count = len(self.sample_handles)
            records = list(self.sample_records[-20:])
            fill_error = self.fill_error
            rescue_fd = self.rescue_fd
        thread_count, threads = self._thread_info()
        return {
            "ok": True,
            "type": "status",
            "pid": os.getpid(),
            "rlimit_nofile": {
                "soft": int(soft),
                "hard": int(hard),
                "startup": rlimit_info,
            },
            "listener": {
                "host": self.host,
                "port": self.bound_port,
                "requested_port": self.requested_port,
                "backlog": self.backlog,
                "fd": self.server.fileno() if self.server is not None else None,
            },
            "rescue_fd": rescue_fd,
            "accept_thread_tid": self.accept_thread_tid,
            "io_thread_tid": self.accept_thread_tid,
            "thread_count": thread_count,
            "thread_limit": MAX_WORKER_THREADS,
            "within_thread_limit": thread_count <= MAX_WORKER_THREADS,
            "threads": threads,
            "connections": self._connection_summary(),
            "sample_dir": str(self.sample_dir),
            "open_sample_files": sample_count,
            "sample_files_tail": records,
            "fill_error": fill_error,
            "open_fd_count": fd_count,
            "fd_count_note": note,
            "fd_table_full": note is not None and "EMFILE" in note,
            "metrics": self.metrics.snapshot(),
        }


def emit(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FD exhaustion TCP echo worker")
    parser.add_argument("--host", default=os.getenv("ECHO_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("ECHO_PORT", "9000")))
    parser.add_argument(
        "--nofile-limit",
        type=int,
        default=int(os.getenv("DRILL_NOFILE_LIMIT", "48")),
    )
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=Path(os.getenv("DRILL_SAMPLE_DIR", "/tmp/fd-emfile-drill-samples")),
    )
    parser.add_argument("--backlog", type=int, default=64)
    args = parser.parse_args(argv)

    rlimit_info = lower_nofile_limit(args.nofile_limit)
    worker = EchoWorker(
        host=args.host,
        port=args.port,
        sample_dir=args.sample_dir,
        backlog=args.backlog,
    )
    socket_info = worker.start_socket()
    emit(
        {
            "ok": True,
            "type": "ready",
            "pid": os.getpid(),
            "rlimit_nofile": rlimit_info,
            **socket_info,
        }
    )
    log(f"listening on {args.host}:{worker.bound_port}, RLIMIT_NOFILE={rlimit_info}")

    for line in sys.stdin:
        request_id = None
        try:
            request = json.loads(line)
            request_id = request.get("id")
            command = request.get("cmd")
            if command == "ping":
                result: dict[str, Any] = {"ok": True, "type": "pong"}
            elif command == "status":
                result = worker.status(rlimit_info)
            elif command == "open_samples":
                result = {
                    "ok": True,
                    "type": "open_samples",
                    **worker.open_samples(
                        count=request.get("count"),
                        fill_to_limit=bool(request.get("fill_to_limit", False)),
                        file_size_bytes=request.get("file_size_bytes"),
                    ),
                }
            elif command == "exhaust":
                result = {
                    "ok": True,
                    "type": "exhaust",
                    **worker.open_samples(
                        fill_to_limit=True,
                        file_size_bytes=request.get("file_size_bytes"),
                    ),
                }
            elif command == "release_samples":
                result = {
                    "ok": True,
                    "type": "release_samples",
                    **worker.release_samples(),
                }
            else:
                result = {"ok": False, "error": f"unknown command: {command!r}"}
        except Exception as exc:  # noqa: BLE001 - control protocol must report
            result = {
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        result["id"] = request_id
        emit(result)

    worker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
