#!/usr/bin/env python3
"""End-to-end demo for the FD/EMFILE drill.

Stdlib only.  Run after `docker compose up --build`:

    python3 scripts/demo.py

The script verifies the full drill contract:

1. 20 TCP connections are held open simultaneously and each echoes;
2. the worker status reports the real connection count and at most 4 threads;
3. one connection stops reading: other connections keep echoing, and when the
   slow reader drains again it receives its complete echo in original order;
4. sample-log exhaustion makes accept(2) return a real EMFILE while every old
   connection keeps echoing;
5. one new probe receives EOF/RST within one second and the EMFILE counter
   reflects actual failed accepts;
6. the I/O thread does not busy-wait;
7. releasing sample descriptors restores the unchanged listener automatically.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from typing import Any

CONCURRENT_CONNECTIONS = 20
# Larger than loopback socket buffers so the worker must queue for the
# non-reading peer instead of flushing the echo immediately.
SLOW_PAYLOAD_BYTES = 4 * 1024 * 1024
SLOW_CHUNK_BYTES = 32 * 1024


def api_call(base: str, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} -> {exc.code}: {detail}") from exc


def assert_echo(sock: socket.socket, payload: bytes) -> None:
    sock.settimeout(2.0)
    sock.sendall(payload)
    received = bytearray()
    while len(received) < len(payload):
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF while waiting for echo")
        received.extend(chunk)
    if bytes(received) != payload:
        raise AssertionError(f"echo mismatch: {received!r} != {payload!r}")


def probe_eof(host: str, port: int, timeout: float = 1.0) -> tuple[str, float]:
    started = time.perf_counter()
    sock = socket.create_connection((host, port), timeout=timeout)
    with sock:
        connected = time.perf_counter()
        sock.settimeout(timeout)
        try:
            first = sock.recv(1)
        except ConnectionResetError:
            return "RST", time.perf_counter() - connected
        except socket.timeout as exc:
            elapsed = time.perf_counter() - connected
            raise AssertionError(f"probe did not receive EOF/RST within {elapsed:.3f}s") from exc
        elapsed = time.perf_counter() - connected
        if first == b"":
            return "EOF", elapsed
        raise AssertionError(
            f"probe received unexpected byte {first!r} after {elapsed:.3f}s "
            f"(total wait {time.perf_counter() - started:.3f}s)"
        )


def successful_echo_connection(host: str, port: int, payload: bytes, deadline_s: float = 3.0) -> None:
    deadline = time.monotonic() + deadline_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5) as sock:
                assert_echo(sock, payload)
                return
        except (OSError, ConnectionError, AssertionError) as exc:
            last_error = exc
            time.sleep(0.05)
    raise AssertionError(f"new connection did not recover and echo: {last_error!r}")


def wait_emfile_increment(base: str, before: int, timeout_s: float = 1.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = api_call(base, "GET", "/api/v1/status")
        if int(last["metrics"]["emfile_total"]) > before:
            return last
        time.sleep(0.02)
    raise AssertionError(f"EMFILE counter did not increase from {before}: {last}")


def recv_exactly(sock: socket.socket, size: int, timeout_s: float) -> bytes:
    sock.settimeout(timeout_s)
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(min(65536, size - len(data)))
        if not chunk:
            raise ConnectionError(f"unexpected EOF after {len(data)}/{size} bytes")
        data.extend(chunk)
    return bytes(data)


def demonstrate_concurrency(
    base: str, host: str, port: int
) -> list[socket.socket]:
    """Hold 20 connections at once and verify each one echoes."""
    sockets: list[socket.socket] = []
    try:
        for index in range(CONCURRENT_CONNECTIONS):
            sock = socket.create_connection((host, port), timeout=2.0)
            sockets.append(sock)
            assert_echo(sock, f"concurrent-{index}-alive\n".encode())
    except Exception:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass
        raise

    status = api_call(base, "GET", "/api/v1/status")
    active = int(status["metrics"]["active_connections"])
    thread_count = int(status["thread_count"])
    thread_limit = int(status["thread_limit"])
    fd_count = int(status["open_fd_count"])
    print(
        f"      held={len(sockets)} active={active} fds={fd_count} "
        f"(RLIMIT_NOFILE={status['rlimit_nofile']['soft']}), "
        f"threads={thread_count}/{thread_limit} "
        f"({', '.join(t['name'] for t in status['threads'])})"
    )
    if active < CONCURRENT_CONNECTIONS:
        raise AssertionError(f"expected >= {CONCURRENT_CONNECTIONS} active, got {active}")
    if thread_count > thread_limit:
        raise AssertionError(f"worker uses {thread_count} threads, limit {thread_limit}")
    return sockets


def demonstrate_slow_reader(
    base: str, host: str, port: int, peers: list[socket.socket]
) -> socket.socket:
    """One non-reading peer must not block others; its echo stays complete and
    in order once it drains."""
    slow = socket.create_connection((host, port), timeout=2.0)
    marker = b"slow-ordered-"
    payload = (marker * (SLOW_PAYLOAD_BYTES // len(marker) + 1))[:SLOW_PAYLOAD_BYTES]
    try:
        slow.settimeout(15.0)
        sent = 0
        queued = 0
        chunks_sent = 0
        while sent < len(payload):
            sent += slow.send(payload[sent : sent + SLOW_CHUNK_BYTES])
            chunks_sent += 1
            if queued == 0 and chunks_sent % 8 == 0:
                status = api_call(base, "GET", "/api/v1/status")
                queued = int(status["connections"]["send_queue_bytes_total"])

        if queued == 0:
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                status = api_call(base, "GET", "/api/v1/status")
                queued = int(status["connections"]["send_queue_bytes_total"])
                if queued > 0:
                    break
                time.sleep(0.02)
        if queued <= 0:
            raise AssertionError("worker never queued data for the slow reader")
        print(f"      slow reader blocked; worker queued {queued} bytes for it alone")

        for index, peer in enumerate(peers):
            assert_echo(peer, f"while-slow-blocked-{index}\n".encode())
        print("      other 20 connections keep echoing while the slow one stalls")

        echoed = recv_exactly(slow, len(payload), timeout_s=15.0)
        if echoed != payload:
            raise AssertionError("slow connection echo was reordered or truncated")
        print(
            f"      slow reader drained {len(payload)} bytes in original order "
            "after resuming recv()"
        )
        assert_echo(slow, b"slow-reader-recovered\n")
        return slow
    except Exception:
        try:
            slow.close()
        except OSError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--echo-host", default="127.0.0.1")
    parser.add_argument("--echo-port", type=int, default=9000)
    parser.add_argument("--keep-fault", action="store_true", help="Do not release files in finally")
    parser.add_argument("--skip-cpu-check", action="store_true")
    parser.add_argument(
        "--skip-concurrency",
        action="store_true",
        help="Skip the 20-connection and slow-reader phases",
    )
    args = parser.parse_args()

    released_by_finally = False
    old: socket.socket | None = None
    peers: list[socket.socket] = []
    slow: socket.socket | None = None
    try:
        health = api_call(args.api_base, "GET", "/healthz")
        print(f"[1/9] API health: {health}")
        if not health.get("ok"):
            raise AssertionError("API/echo process is unhealthy")

        before_metric = api_call(args.api_base, "GET", "/api/v1/metrics/emfile")
        before_emfile = int(before_metric["emfile_total"])
        print(f"[2/9] Initial EMFILE count: {before_emfile}")

        if not args.skip_concurrency:
            print(f"[3/9] Opening {CONCURRENT_CONNECTIONS} simultaneous sampled connections")
            peers = demonstrate_concurrency(
                args.api_base, args.echo_host, args.echo_port
            )
            print("      slow-reader isolation and ordered replay:")
            slow = demonstrate_slow_reader(
                args.api_base, args.echo_host, args.echo_port, peers
            )
        else:
            print("[3/9] Concurrency phase skipped (--skip-concurrency)")

        old = socket.create_connection((args.echo_host, args.echo_port), timeout=2)
        assert_echo(old, b"old-connection-before-fault\n")
        print("[4/9] Old connection established and echoed baseline payload")

        exhaust = api_call(args.api_base, "POST", "/api/v1/drill/exhaust")
        print(
            "[5/9] Sample files filled descriptor table: "
            f"open_sample_files={exhaust['open_sample_files']}, "
            f"open_fd_count={exhaust['open_fd_count']}, "
            f"table_full={exhaust['fd_table_full']}"
        )
        if not exhaust["fd_table_full"]:
            raise AssertionError(f"descriptor table was not full: {exhaust}")

        assert_echo(old, b"old-connection-during-emfile\n")
        if slow is not None:
            assert_echo(slow, b"slow-connection-during-emfile\n")
        for index, peer in enumerate(peers):
            assert_echo(peer, f"concurrent-during-emfile-{index}\n".encode())
        print("[6/9] Every previously held connection still echoes under EMFILE")

        outcome, elapsed = probe_eof(args.echo_host, args.echo_port, timeout=1.0)
        print(f"[7/9] Single new probe got {outcome} {elapsed * 1000:.1f} ms after connect")
        if elapsed > 1.0:
            raise AssertionError(f"EOF/RST took {elapsed:.3f}s, over one second")

        metric = wait_emfile_increment(args.api_base, before_emfile)
        print(
            "      API observed real accept EMFILE: "
            f"emfile_total={metric['metrics']['emfile_total']} "
            f"(+{int(metric['metrics']['emfile_total']) - before_emfile}), "
            f"reaped={metric['metrics']['reaped_after_emfile_total']}"
        )

        if not args.skip_cpu_check:
            cpu = api_call(
                args.api_base,
                "GET",
                "/api/v1/drill/accept-loop-check?seconds=1.0",
            )
            print(
                "[8/9] I/O-loop CPU sample: "
                f"cpu_seconds={cpu['cpu_seconds']}, state_after={cpu['state_after']}, "
                f"wchan={cpu['wchan']!r}, busy_suspected={cpu['busy_spin_suspected']}"
            )
            if cpu["busy_spin_suspected"]:
                raise AssertionError(f"I/O loop appears to busy-wait: {cpu}")
        else:
            print("[8/9] CPU check skipped (--skip-cpu-check)")

        release = api_call(args.api_base, "POST", "/api/v1/drill/release")
        released_by_finally = True
        print(f"[9/9] Released {release['closed_sample_files']} sample descriptors via API")

        payload = f"new-connection-after-release-{time.time()}".encode()
        successful_echo_connection(args.echo_host, args.echo_port, payload)
        print("      New connection echoed successfully; listener auto-recovered")
        print("DEMO PASSED")
        return 0

    finally:
        for sock in peers:
            try:
                sock.close()
            except OSError:
                pass
        if slow is not None:
            try:
                slow.close()
            except OSError:
                pass
        if old is not None:
            try:
                old.close()
            except OSError:
                pass
        if not args.keep_fault and not released_by_finally:
            try:
                api_call(args.api_base, "POST", "/api/v1/drill/release")
                print("cleanup: released sample descriptors")
            except Exception as exc:
                print(f"cleanup warning: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
