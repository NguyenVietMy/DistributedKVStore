"""Fail over around an uncommitted write, then recover the old leader."""

import os
import re
import selectors
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib
from pathlib import Path


def free_ports():
    reserved = [socket.socket() for _ in range(3)]
    try:
        for sock in reserved:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in reserved]
    finally:
        for sock in reserved:
            sock.close()


def main(server_binary, client_binary):
    ports = free_ports()
    selector = selectors.DefaultSelector()
    processes = {}
    active = set()
    lines = {1: [], 2: [], 3: []}
    pending = {1: b"", 2: b"", 3: b""}
    rescuing = None

    with tempfile.TemporaryDirectory(prefix="dkv-failover-") as directory:
        def start(node_id):
            process = subprocess.Popen(
                [str(server_binary), str(node_id),
                 str(Path(directory) / str(node_id)),
                 *(str(port) for port in ports)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            processes[node_id] = process
            active.add(node_id)
            lines[node_id] = []
            pending[node_id] = b""
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ, node_id)

        def stop(node_id):
            process = processes[node_id]
            process.terminate()
            process.wait(timeout=2)
            selector.unregister(process.stdout)
            process.stdin.close()
            process.stdout.close()
            active.remove(node_id)

        def collect_until(predicate, timeout=10):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if predicate():
                    return
                for key, _ in selector.select(
                    max(0, min(0.1, deadline - time.monotonic()))
                ):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    pending[key.data] += chunk
                    while b"\n" in pending[key.data]:
                        line, pending[key.data] = pending[key.data].split(b"\n", 1)
                        lines[key.data].append(line.decode(errors="replace").strip())
                for node_id in active:
                    if processes[node_id].poll() is not None:
                        raise AssertionError(
                            f"node {node_id} exited: {lines[node_id]}")
            raise AssertionError(f"timed out; output: {lines}")

        def console(node_id, text):
            processes[node_id].stdin.write(text + "\n")
            processes[node_id].stdin.flush()

        def client(node_id, *arguments):
            return subprocess.run(
                [str(client_binary), str(ports[node_id - 1]), *arguments],
                text=True, capture_output=True, timeout=8,
            )

        def elected_leader(candidates):
            collect_until(lambda: all(any("listening" in line for line in lines[i])
                                      for i in candidates))
            collect_until(lambda: any("applied index" in line
                                      for i in candidates for line in lines[i]))
            snapshots = {i: len(lines[i]) for i in candidates}
            for node_id in candidates:
                console(node_id, "status")
            collect_until(lambda: any(
                "role leader" in line
                for i in candidates for line in lines[i][snapshots[i]:]
            ))
            return next(
                i for i in candidates
                if any("role leader" in line for line in lines[i][snapshots[i]:])
            )

        try:
            for node_id in (1, 2, 3):
                start(node_id)
            old_leader = elected_leader({1, 2, 3})
            # Drop the connection immediately after sending a write. The client
            # never receives the commit reply, but retains the request ID.
            original_id = "00112233445566778899aabbccddeeff"
            key, value = b"x", b"old"
            command = struct.pack(">BBII", 2, 1, len(key), len(value))
            command += bytes.fromhex(original_id) + key + value
            request = bytes((2, 5)) + command
            request += struct.pack(">I", zlib.crc32(request))
            with socket.create_connection(("127.0.0.1", ports[old_leader - 1]), 2) as sock:
                sock.sendall(struct.pack(">I", len(request)) + request)
            collect_until(lambda: all("applied index 2" in lines[i] for i in active))

            survivors = {1, 2, 3} - {old_leader}
            for node_id in survivors:
                stop(node_id)
            uncertain = subprocess.Popen(
                [str(client_binary), str(ports[old_leader - 1]),
                 "put", "ghost", "yes"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                collect_until(lambda: "proposed index 3" in lines[old_leader])
                assert uncertain.poll() is None, "unreplicated write was acknowledged"
                rescue_id = "fedcba98765432100123456789abcdef"
                rescuing = subprocess.Popen(
                    [str(client_binary), *(str(port) for port in ports),
                     "put", "rescued", "yes", rescue_id],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
                collect_until(lambda: "proposed index 4" in lines[old_leader])
                assert rescuing.poll() is None, "write was acknowledged without a majority"
                stop(old_leader)
                output, error = uncertain.communicate(timeout=4)
                assert uncertain.returncode == 4 and "outcome unknown" in error, (
                    uncertain.returncode, output, error)
            finally:
                if uncertain.poll() is None:
                    uncertain.kill()
                    uncertain.wait()

            for node_id in survivors:
                start(node_id)
            new_leader = elected_leader(survivors)
            rescue_output, rescue_error = rescuing.communicate(timeout=10)
            assert rescuing.returncode == 0 and "committed term" in rescue_output, (
                rescuing.returncode, rescue_output, rescue_error, lines)
            rescued = client(new_leader, "get", "rescued")
            assert rescued.returncode == 0 and rescued.stdout == "yes\n", rescued
            old_value = client(new_leader, "get", "x")
            assert old_value.returncode == 0 and old_value.stdout == "old\n", old_value
            absent = client(new_leader, "get", "ghost")
            assert absent.returncode == 0 and absent.stdout == "(not found)\n", absent
            new_value = client(new_leader, "put", "x", "new")
            assert new_value.returncode == 0, new_value
            index = int(re.search(r"index (\d+)", new_value.stdout).group(1))
            retried = client(new_leader, "put", "x", "old", original_id)
            assert retried.returncode == 0 and "index 2" in retried.stdout, retried
            conflicting = client(new_leader, "put", "x", "wrong", original_id)
            assert conflicting.returncode == 2 and "invalid request" in conflicting.stdout, conflicting
            before_status = len(lines[new_leader])
            console(new_leader, "status")
            collect_until(lambda: any("role leader" in line
                                      for line in lines[new_leader][before_status:]))
            assert any(f" log {index} " in line
                       for line in lines[new_leader][before_status:]), lines
            still_new = client(new_leader, "get", "x")
            assert still_new.returncode == 0 and still_new.stdout == "new\n", still_new

            start(old_leader)
            collect_until(lambda: f"applied index {index}" in lines[old_leader])
            console(old_leader, "inspect ghost")
            collect_until(lambda: "(not found)" in lines[old_leader])
            rejected = client(old_leader, "get", "x")
            assert rejected.returncode == 3 and "not leader" in rejected.stdout, rejected
            latest = client(new_leader, "get", "x")
            assert latest.returncode == 0 and latest.stdout == "new\n", latest

            for node_id in list(active):
                stop(node_id)
            for node_id in (1, 2, 3):
                start(node_id)
            restarted_leader = elected_leader({1, 2, 3})
            recovered = client(restarted_leader, "get", "x")
            assert recovered.returncode == 0 and recovered.stdout == "new\n", recovered
            no_ghost = client(restarted_leader, "get", "ghost")
            assert no_ghost.returncode == 0 and no_ghost.stdout == "(not found)\n", no_ghost
            retried_after_restart = client(
                restarted_leader, "put", "x", "old", original_id)
            assert retried_after_restart.returncode == 0 and (
                retried_after_restart.stdout == retried.stdout), retried_after_restart
            unchanged = client(restarted_leader, "get", "x")
            assert unchanged.returncode == 0 and unchanged.stdout == "new\n", unchanged
            recovered_rescue = client(restarted_leader, "get", "rescued")
            assert recovered_rescue.returncode == 0 and (
                recovered_rescue.stdout == "yes\n"), recovered_rescue
        finally:
            if rescuing is not None and rescuing.poll() is None:
                rescuing.kill()
                rescuing.communicate()
            for node_id in list(active):
                process = processes[node_id]
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                if not process.stdin.closed:
                    process.stdin.close()
                if not process.stdout.closed:
                    process.stdout.close()
            selector.close()


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
