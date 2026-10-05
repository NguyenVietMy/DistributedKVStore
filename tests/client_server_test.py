"""End-to-end writes and linearizable reads through the client protocol."""

import os
import re
import selectors
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def free_ports():
    reservations = [socket.socket() for _ in range(3)]
    try:
        for item in reservations:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in reservations]
    finally:
        for item in reservations:
            item.close()


def main(server_executable, client_executable):
    ports = free_ports()
    selector = selectors.DefaultSelector()
    processes = {}
    seen = {1: [], 2: [], 3: []}
    pending = {1: b"", 2: b"", 3: b""}
    active = set()
    with tempfile.TemporaryDirectory(prefix="dkv-client-test-") as temporary:
        try:
            for node_id in (1, 2, 3):
                process = subprocess.Popen(
                    [str(server_executable), str(node_id),
                     str(Path(temporary) / str(node_id)),
                     *(str(port) for port in ports)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                processes[node_id] = process
                active.add(node_id)
                os.set_blocking(process.stdout.fileno(), False)
                selector.register(process.stdout, selectors.EVENT_READ, node_id)

            def collect_until(predicate, timeout=8):
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
                            seen[key.data].append(line.decode(errors="replace").strip())
                    for node_id in active:
                        if processes[node_id].poll() is not None:
                            raise AssertionError(
                                f"node {node_id} exited: {seen[node_id]}")
                raise AssertionError(f"timed out; output: {seen}")

            def client(node_id, *arguments):
                return subprocess.run(
                    [str(client_executable), str(ports[node_id - 1]), *arguments],
                    text=True, capture_output=True, timeout=8,
                )

            collect_until(lambda: all(any("listening" in line for line in seen[i])
                                      for i in active))
            collect_until(lambda: all("applied index 1" in seen[i] for i in active))
            for node_id in active:
                processes[node_id].stdin.write("status\n")
                processes[node_id].stdin.flush()
            collect_until(lambda: any("role leader" in line
                                      for lines in seen.values() for line in lines))
            leader = next(i for i in active
                          if any("role leader" in line for line in seen[i]))
            follower = next(i for i in active if i != leader)

            rejected = client(follower, "put", "wrong", "value")
            assert rejected.returncode == 3 and "not leader" in rejected.stdout, rejected
            committed = client(leader, "put", "x", "10")
            assert committed.returncode == 0 and "committed term" in committed.stdout, committed
            term = int(re.search(r"committed term (\d+)", committed.stdout).group(1))
            collect_until(lambda: all("applied index 2" in seen[i] for i in active))
            stale_read = client(follower, "get", "x")
            assert stale_read.returncode == 3 and "not leader" in stale_read.stdout, stale_read
            found = client(leader, "get", "x")
            assert found.returncode == 0 and found.stdout == "10\n", found
            missing = client(leader, "get", "missing")
            assert missing.returncode == 0 and missing.stdout == "(not found)\n", missing
            deleted = client(leader, "delete", "x")
            assert deleted.returncode == 0 and "index 3" in deleted.stdout, deleted
            collect_until(lambda: all("applied index 3" in seen[i] for i in active))
            after_delete = client(leader, "get", "x")
            assert after_delete.returncode == 0 and after_delete.stdout == "(not found)\n", after_delete
            before_status = len(seen[leader])
            processes[leader].stdin.write("status\n")
            processes[leader].stdin.flush()
            collect_until(lambda: any("role leader" in line
                                      for line in seen[leader][before_status:]))
            assert any(" log 3 " in line for line in seen[leader][before_status:]), seen

            for node_id in list(active):
                if node_id == leader:
                    continue
                processes[node_id].terminate()
                processes[node_id].wait(timeout=2)
                selector.unregister(processes[node_id].stdout)
                active.remove(node_id)

            waiting_read = subprocess.Popen(
                [str(client_executable), str(ports[leader - 1]), "get", "x"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            waiting_write = subprocess.Popen(
                [str(client_executable), str(ports[leader - 1]),
                 "put", "x", "uncertain"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            try:
                collect_until(lambda: "proposed index 4" in seen[leader])
                assert waiting_read.poll() is None, "GET completed without a majority"
                assert waiting_write.poll() is None, "PUT was acknowledged without a majority"

                # The reported peer has a newer term but a stale log: the vote
                # is denied, yet the old leader must step down and fail pending writes.
                sender = follower
                vote = struct.pack(">BBQQQQQ", 2, 1, sender, term + 1, sender, 0, 0)
                with socket.create_connection(("127.0.0.1", ports[leader - 1]), 2) as sock:
                    sock.sendall(struct.pack(">I", len(vote)) + vote)
                read_output, read_error = waiting_read.communicate(timeout=4)
                assert waiting_read.returncode == 3 and "not leader" in read_output, (
                    waiting_read.returncode, read_output, read_error, seen)
                output, error = waiting_write.communicate(timeout=4)
                assert waiting_write.returncode == 4 and "outcome unknown" in output, (
                    waiting_write.returncode, output, error, seen)
                rejected_again = client(leader, "put", "x", "after-stepdown")
                assert rejected_again.returncode == 3, rejected_again
            finally:
                for waiting in (waiting_read, waiting_write):
                    if waiting.poll() is None:
                        waiting.kill()
                        waiting.wait()
        finally:
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                process.stdin.close()
                process.stdout.close()
            selector.close()


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
