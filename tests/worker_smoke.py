#!/usr/bin/env python3
"""Stdlib-only integration check for echo_worker.py.

This does not exercise FastAPI; it validates the Linux FD/EMFILE behavior and
the multiplexed, bounded-thread echo design and is usable in minimal
environments where FastAPI is not installed.

Checks:
  * many simultaneous connections on RLIMIT_NOFILE=32 (at least 7 here; the
    full API self-test uses 20);
  * the worker process stays at two threads (control + one network I/O);
  * a connection that stops reading only backpressures itself, keeps the data
    in order, and receives the complete echo once it drains;
  * sample-log exhaustion -> real accept(2) EMFILE, old connections keep
    echoing, one new probe gets EOF/RST within a second, no CPU busy-wait;
  * releasing sample logs restores the unchanged listener automatically.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "app" / "echo_worker.py"

CONCURRENT_CONNECTIONS = 7
# Larger than the loopback socket buffers so the worker must accumulate data
# for the non-reading peer instead of flushing it immediately.
SLOW_PAYLOAD_BYTES = 4 * 1024 * 1024
SLOW_CHUNK_BYTES = 16 * 1024


def send(proc: subprocess.Popen[bytes], payload: dict) -> dict:
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write((json.dumps(payload) + "\n").encode())
    proc.stdin.flush()
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError("worker closed stdout")
    return json.loads(line)


def wait_echo(sock: socket.socket, payload: bytes, timeout: float = 2.0) -> bytes:
    sock.settimeout(timeout)
    sock.sendall(payload)
    chunks = []
    remaining = len(payload)
    while remaining:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF while expecting echo")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_exactly(sock: socket.socket, size: int, timeout: float) -> bytes:
    sock.settimeout(timeout)
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(min(65536, size - len(data)))
        if not chunk:
            raise ConnectionError("unexpected EOF while draining slow connection")
        data.extend(chunk)
    return bytes(data)


def probe_eof(port: int, timeout: float = 1.0) -> tuple[str, float]:
    started = time.perf_counter()
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        connected = time.perf_counter()
        s.settimeout(timeout)
        try:
            data = s.recv(1)
            elapsed = time.perf_counter() - connected
            if data == b"":
                return "eof", elapsed
            return f"data:{data!r}", elapsed
        except ConnectionResetError:
            return "reset", time.perf_counter() - connected
        except socket.timeout:
            return "timeout", time.perf_counter() - started


def thread_cpu(pid: int, tid: int) -> float:
    raw = Path(f"/proc/{pid}/task/{tid}/stat").read_text()
    rest = raw[raw.rfind(")") + 2 :].split()
    utime = int(rest[11])
    stime = int(rest[12])
    hz = os.sysconf("SC_CLK_TCK")
    return (utime + stime) / hz


def main() -> int:
    sample_dir = Path("/tmp/fd-emfile-worker-selftest")
    sample_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [
            sys.executable,
            "-I",
            str(WORKER),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--nofile-limit",
            "32",
            "--sample-dir",
            str(sample_dir),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    peers: list[socket.socket] = []
    slow: socket.socket | None = None
    try:
        assert proc.stdout is not None
        ready = json.loads(proc.stdout.readline())
        print("ready:", json.dumps(ready, indent=2))
        port = ready["port"]
        tid = ready["accept_thread_tid"]

        # ---- many simultaneous sampled connections, all echoed -------------
        for index in range(CONCURRENT_CONNECTIONS):
            sock = socket.create_connection(("127.0.0.1", port), timeout=2)
            peers.append(sock)
            assert wait_echo(sock, f"concurrent-{index}\n".encode()) == (
                f"concurrent-{index}\n".encode()
            )
        print(f"{CONCURRENT_CONNECTIONS} simultaneous connections echo: OK")

        status = send(proc, {"id": "status-1", "cmd": "status"})
        thread_count = status["thread_count"]
        print(
            "threads:",
            thread_count,
            json.dumps(status["threads"]),
            "limit:",
            status["thread_limit"],
        )
        assert thread_count <= 4, status
        assert status["metrics"]["active_connections"] == CONCURRENT_CONNECTIONS
        print("thread cap (<=4) and active connection count: OK")

        # ---- one connection stops reading; others must keep echoing --------
        slow = socket.create_connection(("127.0.0.1", port), timeout=2)
        marker = b"slow-order-"
        payload = (marker * (SLOW_PAYLOAD_BYTES // len(marker) + 1))[
            :SLOW_PAYLOAD_BYTES
        ]
        slow.settimeout(5.0)
        sent = 0
        queued = 0
        chunks_sent = 0
        while sent < len(payload):
            sent += slow.send(payload[sent : sent + SLOW_CHUNK_BYTES])
            chunks_sent += 1
            # Poll while data is still in flight; once the kernel socket
            # buffers fill, the worker must start queueing for this peer.
            if queued == 0 and chunks_sent % 8 == 0:
                st = send(proc, {"id": "status-q", "cmd": "status"})
                queued = st["connections"]["send_queue_bytes_total"]

        if queued == 0:
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                st = send(proc, {"id": "status-q", "cmd": "status"})
                queued = st["connections"]["send_queue_bytes_total"]
                if queued > 0:
                    break
                time.sleep(0.02)
        assert queued > 0, "worker never queued data for the blocked reader"
        print(f"slow reader blocked; worker queued {queued} bytes for it alone")

        for index, peer in enumerate(peers):
            assert wait_echo(peer, f"while-slow-blocked-{index}\n".encode()) == (
                f"while-slow-blocked-{index}\n".encode()
            )
        print("other connections keep echoing while one reader is blocked: OK")

        echoed = recv_exactly(slow, len(payload), timeout=10.0)
        assert echoed == payload, "slow connection echo reordered or truncated"
        print("slow reader received the complete echo in original order: OK")
        st = send(proc, {"id": "status-2", "cmd": "status"})
        assert st["connections"]["send_queue_bytes_total"] == 0

        # ---- EMFILE drill: old connections survive, probe gets EOF/RST ------
        old = peers[0]
        assert wait_echo(old, b"before-exhaust\n") == b"before-exhaust\n"

        result = send(proc, {"id": "exhaust", "cmd": "exhaust"})
        print(
            "exhaust:",
            json.dumps(
                {
                    k: result[k]
                    for k in (
                        "ok",
                        "opened_this_call",
                        "open_sample_files",
                        "open_fd_count",
                        "fd_count_note",
                        "fd_table_full",
                        "fill_error",
                    )
                },
                indent=2,
            ),
        )
        assert result["ok"], result
        assert result["fd_table_full"], result

        # With 8 long-lived connections (7 peers + the slow one) the table can
        # still be filled at RLIMIT_NOFILE=32; every old connection survives.
        assert wait_echo(old, b"old-conn-still-live\n") == b"old-conn-still-live\n"
        assert wait_echo(slow, b"slow-conn-still-live\n") == b"slow-conn-still-live\n"
        print("old connections (incl. formerly slow) echo during exhaustion: OK")

        outcome, elapsed = probe_eof(port, timeout=1.0)
        print(f"new probe: {outcome!r} after {elapsed*1000:.1f} ms")
        assert outcome in ("eof", "reset"), (outcome, elapsed)
        assert elapsed <= 1.0, elapsed

        status = send(proc, {"id": "status-3", "cmd": "status"})
        print("metrics:", json.dumps(status["metrics"], indent=2))
        print("rescue_fd:", status["rescue_fd"], "fd_count:", status["open_fd_count"])
        emfile = status["metrics"]["emfile_total"]
        print("emfile_total:", emfile)
        assert emfile >= 1, status
        assert status["thread_count"] <= 4, status

        cpu0 = thread_cpu(proc.pid, tid)
        time.sleep(1.0)
        cpu1 = thread_cpu(proc.pid, tid)
        burn = cpu1 - cpu0
        print(f"I/O thread CPU over idle fault second: {burn*1000:.1f} ms")
        assert burn < 0.05, burn

        released = send(proc, {"id": "release", "cmd": "release_samples"})
        print("release:", released["closed_sample_files"], "files")
        assert released["ok"]
        assert released["listener_fd"] == ready["listener_fd"]

        new = socket.create_connection(("127.0.0.1", port), timeout=2)
        payload = f"after-release-{time.time()}".encode()
        assert wait_echo(new, payload) == payload
        new.close()
        print("new connection echo after release: OK (listener unchanged)")

        print("WORKER SELFTEST PASSED")
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
        try:
            if proc.stdin is not None:
                proc.stdin.close()
            proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
