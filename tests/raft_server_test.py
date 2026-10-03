"""Exercise three local Raft processes over TCP without a client protocol."""

import selectors
import os
import socket
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


def main(executable):
    ports = free_ports()
    selector = selectors.DefaultSelector()
    processes = {}
    seen = {1: [], 2: [], 3: []}
    pending = {1: b"", 2: b"", 3: b""}
    with tempfile.TemporaryDirectory(prefix="dkv-server-test-") as temporary:
        try:
            for node_id in (1, 2, 3):
                process = subprocess.Popen(
                    [str(executable), str(node_id), str(Path(temporary) / str(node_id)),
                     *(str(port) for port in ports)],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                processes[node_id] = process
                os.set_blocking(process.stdout.fileno(), False)
                selector.register(process.stdout, selectors.EVENT_READ, node_id)

            def command(node_id, line):
                process = processes[node_id]
                if process.poll() is not None:
                    raise AssertionError(f"node {node_id} exited: {seen[node_id]}")
                process.stdin.write(line + "\n")
                process.stdin.flush()

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
                    for node_id, process in processes.items():
                        if process.poll() is not None:
                            raise AssertionError(f"node {node_id} exited: {seen[node_id]}")
                raise AssertionError(f"timed out; output: {seen}")

            collect_until(lambda: all(any("listening" in line for line in seen[i])
                                      for i in (1, 2, 3)))
            collect_until(lambda: any("applied index 1" in line
                                      for lines in seen.values() for line in lines))

            for node_id in (1, 2, 3):
                command(node_id, "status")
            collect_until(lambda: any("role leader" in line
                                      for lines in seen.values() for line in lines))
            leader = next(i for i in (1, 2, 3)
                          if any("role leader" in line for line in seen[i]))

            command(leader, "put x 10")
            collect_until(lambda: all("applied index 2" in seen[i]
                                      for i in (1, 2, 3)))
            for node_id in (1, 2, 3):
                command(node_id, "inspect x")
            collect_until(lambda: all("10" in seen[i] for i in (1, 2, 3)))

            # Several commands in one pipe read exercise batching and stdin framing.
            for index in range(6):
                command(leader, f"put bulk{index} value{index}")
            collect_until(lambda: all("applied index 8" in seen[i]
                                      for i in (1, 2, 3)))
            for node_id in (1, 2, 3):
                command(node_id, "inspect bulk5")
            collect_until(lambda: all("value5" in seen[i] for i in (1, 2, 3)))
        finally:
            for process in processes.values():
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
    main(Path(sys.argv[1]))
