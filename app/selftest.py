"""Server-side end-to-end check for the EMFILE drill.

Covers the full drill contract:

* at least 20 simultaneously held TCP echo connections under RLIMIT_NOFILE=32;
* the echo process never exceeds its 4-thread cap (two threads in practice);
* one connection that stops reading only backpressures itself: other
  connections keep echoing, and once the slow reader drains again it receives
  its complete echo in the original send order;
* sample-log exhaustion makes a real accept(2) return EMFILE while every old
  connection keeps echoing; a single new probe gets EOF/RST within 1 second;
* the accept/I/O thread does not busy-wait while the fault is idle;
* releasing the sample logs lets the unchanged listener accept again.
"""

from __future__ import annotations

import socket
import time
from typing import Any

from .controller import EchoController
from .probes import run_tcp_probe

CONCURRENT_CONNECTIONS = 20
# Larger than the loopback socket buffers so the worker's per-connection send
# queue must accumulate while the peer is not reading.
SLOW_PAYLOAD_BYTES = 4 * 1024 * 1024
SLOW_CHUNK_BYTES = 32 * 1024
SLOW_DRAIN_TIMEOUT = 15.0


def _assert_echo(sock: socket.socket, payload: bytes) -> None:
    sock.settimeout(2.0)
    sock.sendall(payload)
    received = bytearray()
    while len(received) < len(payload):
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF while waiting for echo")
        received.extend(chunk)
    if bytes(received) != payload:
        raise AssertionError("echo mismatch")


def _recv_exactly(sock: socket.socket, size: int, timeout: float) -> bytes:
    sock.settimeout(timeout)
    received = bytearray()
    while len(received) < size:
        chunk = sock.recv(min(65536, size - len(received)))
        if not chunk:
            raise ConnectionError(
                f"unexpected EOF after {len(received)}/{size} bytes"
            )
        received.extend(chunk)
    return bytes(received)


def _close_quietly(sock: socket.socket | None) -> None:
    if sock is not None:
        try:
            sock.close()
        except OSError:
            pass


def _check_concurrency(
    host: str, port: int
) -> tuple[list[socket.socket], dict[str, Any]]:
    """Open and verify 20 simultaneous connections; return them for reuse."""
    sockets: list[socket.socket] = []
    try:
        for index in range(CONCURRENT_CONNECTIONS):
            sock = socket.create_connection((host, port), timeout=2.0)
            sock.settimeout(2.0)
            sockets.append(sock)
            _assert_echo(sock, f"concurrent-{index}-ping\n".encode())
        return sockets, {
            "concurrent_connections": len(sockets),
            "each_echoed": True,
        }
    except Exception:
        for sock in sockets:
            _close_quietly(sock)
        raise


def _check_slow_connection(
    host: str,
    port: int,
    *,
    peers: list[socket.socket],
    controller: EchoController,
) -> tuple[socket.socket, dict[str, Any]]:
    """A non-reading peer must not block other connections and must get an
    ordered full echo once it drains."""
    slow = socket.create_connection((host, port), timeout=2.0)
    marker = f"slow-{time.time_ns()}-".encode()
    payload = (marker * (SLOW_PAYLOAD_BYTES // len(marker) + 1))[:SLOW_PAYLOAD_BYTES]

    try:
        # Send everything without reading.  Flow control blocks the sender in
        # the kernel while the worker queues whatever it accepts; the point is
        # that the worker's send queue grows for this one connection only.
        slow.settimeout(SLOW_DRAIN_TIMEOUT)
        sent = 0
        queued = 0
        chunks_sent = 0
        while sent < len(payload):
            sent += slow.send(payload[sent : sent + SLOW_CHUNK_BYTES])
            chunks_sent += 1
            # Poll while data is still in flight: once the kernel socket
            # buffers fill, the worker must start queueing for this peer.
            if queued == 0 and chunks_sent % 8 == 0:
                status = controller.request("status")
                queued = int(status["connections"]["send_queue_bytes_total"])

        # Wait until the worker reports queued bytes for a blocked connection.
        if queued == 0:
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                status = controller.request("status")
                queued = int(status["connections"]["send_queue_bytes_total"])
                if queued > 0:
                    break
                time.sleep(0.02)
        if queued <= 0:
            raise AssertionError("worker never queued data for the slow reader")

        # Other connections stay fully responsive while the slow one is blocked.
        for index, peer in enumerate(peers):
            _assert_echo(peer, f"while-slow-blocked-{index}\n".encode())

        # The slow reader now drains: the complete echo must arrive in order.
        received = _recv_exactly(slow, len(payload), SLOW_DRAIN_TIMEOUT)
        if received != payload:
            raise AssertionError("slow connection echo was incomplete or reordered")

        status = controller.request("status")
        return slow, {
            "slow_payload_bytes": len(payload),
            "worker_send_queue_observed_bytes": queued,
            "other_connections_echoed_while_blocked": True,
            "drained_full_echo_in_order": True,
            "queue_after_drain_bytes": int(
                status["connections"]["send_queue_bytes_total"]
            ),
        }
    except Exception:
        _close_quietly(slow)
        raise


def run_self_test(
    controller: EchoController,
    *,
    host: str,
    port: int,
    cpu_sample_seconds: float = 0.5,
) -> dict[str, Any]:
    """Exercise concurrency, slow-reader isolation, EMFILE and recovery."""
    report: dict[str, Any] = {
        "ok": False,
        "host": host,
        "port": port,
        "started_unix": time.time(),
        "steps": {},
        "checks": {},
    }
    peers: list[socket.socket] = []
    slow: socket.socket | None = None
    released = False

    try:
        initial = controller.request("status")
        before_emfile = int(initial["metrics"]["emfile_total"])
        report["steps"]["initial"] = {
            "thread_count": initial.get("thread_count"),
            "thread_limit": initial.get("thread_limit"),
            "rlimit_soft": initial["rlimit_nofile"]["soft"],
            "emfile_total": before_emfile,
        }

        # ---- Phase A: many concurrent connections on a small FD table ----
        peers, concurrency = _check_concurrency(host, port)
        report["steps"]["concurrency"] = concurrency
        report["checks"][f"at_least_{CONCURRENT_CONNECTIONS}_simultaneous_connections"] = (
            len(peers) >= CONCURRENT_CONNECTIONS
        )

        status = controller.request("status")
        report["steps"]["status_with_conns"] = {
            "thread_count": status["thread_count"],
            "thread_limit": status["thread_limit"],
            "threads": status["threads"],
            "active_connections": status["metrics"]["active_connections"],
            "max_active_connections": status["metrics"]["max_active_connections"],
            "open_fd_count": status["open_fd_count"],
            "rlimit_soft": status["rlimit_nofile"]["soft"],
        }
        report["checks"]["active_connection_count_matches"] = (
            status["metrics"]["active_connections"] == CONCURRENT_CONNECTIONS
        )
        report["checks"]["worker_threads_within_limit"] = bool(
            status["within_thread_limit"]
            and status["thread_count"] <= status["thread_limit"]
        )
        if not report["checks"]["worker_threads_within_limit"]:
            raise AssertionError(
                f"worker uses {status['thread_count']} threads, "
                f"limit is {status['thread_limit']}"
            )

        # ---- Phase B: one blocked reader isolates backpressure ------------
        slow, slow_report = _check_slow_connection(
            host, port, peers=peers, controller=controller
        )
        report["steps"]["slow_connection"] = slow_report
        report["checks"]["other_conns_echo_while_one_blocks_sending"] = True
        report["checks"]["slow_reader_gets_full_ordered_echo"] = True

        # A small payload still echoes on the drained slow connection.
        _assert_echo(slow, b"slow-recovered-ping\n")

        # ---- Phase C: real EMFILE while old connections survive -----------
        exhaust = controller.request("exhaust", timeout=15.0)
        report["steps"]["exhaust"] = {
            key: exhaust.get(key)
            for key in (
                "open_sample_files",
                "open_fd_count",
                "fd_table_full",
                "fill_error",
            )
        }
        if not exhaust.get("fd_table_full"):
            raise AssertionError("descriptor table was not reported full")

        # Every previously established connection, including the formerly slow
        # one, must keep echoing while accept(2) fails with EMFILE.
        for index, peer in enumerate(peers):
            _assert_echo(peer, f"old-conn-during-emfile-{index}\n".encode())
        _assert_echo(slow, b"slow-conn-during-emfile\n")
        report["checks"]["all_old_connections_echo_during_emfile"] = True

        probe = run_tcp_probe(
            host,
            port,
            send_payload=False,
            timeout_seconds=1.0,
        )
        report["steps"]["new_probe_during_fault"] = probe
        report["checks"]["probe_eof_or_reset_within_one_second"] = (
            probe.get("outcome") in ("eof", "reset")
            and bool(probe.get("within_one_second"))
        )
        if not report["checks"]["probe_eof_or_reset_within_one_second"]:
            raise AssertionError(f"bad EMFILE probe result: {probe}")

        status = controller.request("status")
        emfile_total = int(status["metrics"]["emfile_total"])
        report["steps"]["emfile_total"] = emfile_total
        report["checks"]["real_emfile_count_increased"] = emfile_total > before_emfile
        if not report["checks"]["real_emfile_count_increased"]:
            raise AssertionError("EMFILE counter did not increase")

        cpu = controller.accept_loop_sample(cpu_sample_seconds)
        report["steps"]["io_loop_cpu"] = cpu
        report["checks"]["io_loop_not_busy_waiting"] = not cpu["busy_spin_suspected"]
        if cpu["busy_spin_suspected"]:
            raise AssertionError("I/O loop appears to busy-wait")

        release = controller.request("release_samples")
        released = True
        report["steps"]["release"] = {
            key: release.get(key)
            for key in ("closed_sample_files", "listener_fd", "open_fd_count")
        }

        recovery_payload = f"selftest-recovery-{time.time()}".encode()
        recovery = run_tcp_probe(
            host,
            port,
            payload=recovery_payload.decode(),
            send_payload=True,
            expect_echo=True,
            timeout_seconds=2.0,
        )
        report["steps"]["recovery_probe"] = recovery
        report["checks"]["new_connection_echoes_after_release"] = bool(
            recovery.get("echo_matches")
        )
        if not recovery.get("echo_matches"):
            raise AssertionError(f"recovery echo failed: {recovery}")

        # The original listener descriptor must survive the whole drill.
        report["checks"]["listener_fd_unchanged"] = (
            report["steps"]["release"]["listener_fd"]
            == initial["listener"]["fd"]
        )

        report["ok"] = all(report["checks"].values())
        return report

    except Exception as exc:  # noqa: BLE001 - report failure to API caller
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        return report
    finally:
        _close_quietly(slow)
        for peer in peers:
            _close_quietly(peer)
        if not released:
            try:
                controller.request("release_samples")
            except Exception as exc:  # noqa: BLE001
                report["cleanup_error"] = repr(exc)
        report["finished_unix"] = time.time()
