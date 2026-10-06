"""End-to-end client writes, reads, and status across Raft roles."""

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


def recv_exact(sock, size):
    result = b""
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        assert chunk, "connection closed before complete reply"
        result += chunk
    return result


def check_isolated_candidate(server_executable, client_executable):
    ports = free_ports()
    with tempfile.TemporaryDirectory(prefix="dkv-candidate-status-") as temporary:
        process = subprocess.Popen(
            [str(server_executable), "1", temporary,
             *(str(port) for port in ports)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                status = subprocess.run(
                    [str(client_executable), str(ports[0]), "status"],
                    text=True, capture_output=True, timeout=2,
                )
                if status.returncode == 0 and re.fullmatch(
                    r"node 1 role candidate term [1-9]\d* leader unknown "
                    r"log 0 commit 0 applied 0 "
                    r"peer 2 progress unknown peer 3 progress unknown\n",
                    status.stdout,
                ):
                    return
                assert process.poll() is None, "isolated candidate exited"
                time.sleep(0.05)
            raise AssertionError(f"isolated node never reported candidate status: {status}")
        finally:
            process.terminate()
            process.wait(timeout=2)
            process.stderr.close()


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

            for node_id in sorted(active):
                status = client(node_id, "status")
                assert status.returncode == 0, status
                line = status.stdout.strip()
                expected_role = "leader" if node_id == leader else "follower"
                assert re.search(
                    rf"^node {node_id} role {expected_role} term \d+ "
                    rf"leader {leader} log \d+ commit \d+ applied \d+",
                    line,
                ), status
                for peer_id in active - {node_id}:
                    if node_id == leader:
                        assert re.search(
                            rf"peer {peer_id} next \d+ match \d+", line
                        ), status
                    else:
                        assert f"peer {peer_id} progress unknown" in line, status
            cluster_status = subprocess.run(
                [str(client_executable), *(str(port) for port in ports), "status"],
                text=True, capture_output=True, timeout=8,
            )
            assert cluster_status.returncode == 0, cluster_status
            assert [line.split()[1] for line in cluster_status.stdout.splitlines()] == [
                "1", "2", "3"
            ], cluster_status
            with socket.create_connection(("127.0.0.1", ports[follower - 1]), 2) as sock:
                malformed_status = b"\x01\x09\x00"
                sock.sendall(struct.pack(">I", len(malformed_status)) + malformed_status)
                reply_size = struct.unpack(">I", recv_exact(sock, 4))[0]
                reply = recv_exact(sock, reply_size)
                assert reply_size == 100 and reply[:4] == b"\x01\x0a\x02\x00", reply

            rejected = client(follower, "put", "wrong", "value")
            assert rejected.returncode == 3 and "not leader" in rejected.stdout, rejected
            committed = client(leader, "put", "x", "10")
            assert committed.returncode == 0 and "committed term" in committed.stdout, committed
            term = int(re.search(r"committed term (\d+)", committed.stdout).group(1))
            collect_until(lambda: all("applied index 2" in seen[i] for i in active))
            updated_status = client(leader, "status")
            assert updated_status.returncode == 0 and (
                " log 2 commit 2 applied 2" in updated_status.stdout
            ), updated_status
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
    check_isolated_candidate(server_executable, client_executable)


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
