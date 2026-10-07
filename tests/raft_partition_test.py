"""Partition a live three-process cluster, then heal and compare durable logs."""

import os
import re
import selectors
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


class Cluster:
    def __init__(self, server_binary, client_binary, root):
        self.client_binary = client_binary
        self.root = root
        self.ports = free_ports()
        self.selector = selectors.DefaultSelector()
        self.processes = {}
        self.lines = {node_id: [] for node_id in (1, 2, 3)}
        self.pending = {node_id: b"" for node_id in (1, 2, 3)}
        for node_id in (1, 2, 3):
            process = subprocess.Popen(
                [str(server_binary), str(node_id), str(root / str(node_id)),
                 *(str(port) for port in self.ports)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.processes[node_id] = process
            os.set_blocking(process.stdout.fileno(), False)
            self.selector.register(process.stdout, selectors.EVENT_READ, node_id)

    def close(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdin.close()
            process.stdout.close()
        self.selector.close()

    def collect(self, timeout=0.05):
        for key, _ in self.selector.select(timeout):
            chunk = os.read(key.fileobj.fileno(), 65536)
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            node_id = key.data
            self.pending[node_id] += chunk
            while b"\n" in self.pending[node_id]:
                line, self.pending[node_id] = self.pending[node_id].split(b"\n", 1)
                self.lines[node_id].append(line.decode(errors="replace").strip())
        for node_id, process in self.processes.items():
            assert process.poll() is None, f"node {node_id} exited: {self.lines}"

    def wait(self, predicate, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.collect()
            if predicate():
                return
        raise AssertionError(f"timed out; output: {self.lines}")

    def command(self, node_id, command):
        process = self.processes[node_id]
        process.stdin.write((command + "\n").encode())
        process.stdin.flush()

    def links(self, pairs, blocked):
        before = {node_id: len(self.lines[node_id]) for node_id, _ in pairs}
        verb = "block" if blocked else "unblock"
        for node_id, peer_id in pairs:
            self.command(node_id, f"{verb} {peer_id}")
        result = "blocked" if blocked else "unblocked"
        self.wait(lambda: all(
            f"peer {peer_id} {result}" in self.lines[node_id][before[node_id]:]
            for node_id, peer_id in pairs
        ))

    def client(self, node_id, *arguments):
        return subprocess.run(
            [str(self.client_binary), str(self.ports[node_id - 1]), *arguments],
            capture_output=True, text=True, timeout=8,
        )

    def status(self, node_id):
        result = self.client(node_id, "status")
        assert result.returncode == 0, (node_id, result)
        match = re.match(
            rf"node {node_id} role (follower|candidate|leader) term (\d+) "
            r"leader (\d+|unknown) log (\d+) commit (\d+) applied (\d+)",
            result.stdout,
        )
        assert match, (node_id, result)
        role, term, leader, last, commit, applied = match.groups()
        return {
            "role": role, "term": int(term), "leader": leader,
            "log": int(last), "commit": int(commit), "applied": int(applied),
        }

    def wait_leader(self, node_ids):
        elected = []

        def found():
            elected[:] = [node_id for node_id in node_ids
                          if self.status(node_id)["role"] == "leader"]
            return len(elected) == 1

        self.wait(found)
        return elected[0]

    def inspect(self, node_id, key, expected):
        before = len(self.lines[node_id])
        self.command(node_id, f"inspect {key}")
        self.wait(lambda: expected in self.lines[node_id][before:])


def committed_index(result):
    assert result.returncode == 0, result
    match = re.fullmatch(r"committed term \d+ index (\d+)\n", result.stdout)
    assert match, result
    return int(match.group(1))


def main(server_binary, client_binary):
    with tempfile.TemporaryDirectory(prefix="dkv-partition-") as directory:
        cluster = Cluster(server_binary, client_binary, Path(directory))
        pending_write = None
        pending_read = None
        try:
            cluster.wait(lambda: all(
                any("listening" in line for line in cluster.lines[node_id])
                for node_id in (1, 2, 3)
            ))
            old_leader = cluster.wait_leader((1, 2, 3))
            baseline = committed_index(cluster.client(old_leader, "put", "base", "kept"))
            cluster.wait(lambda: all(
                cluster.status(node_id)["applied"] >= baseline
                for node_id in (1, 2, 3)
            ))

            survivors = sorted({1, 2, 3} - {old_leader})
            blocked_links = ([(old_leader, node_id) for node_id in survivors] +
                             [(node_id, old_leader) for node_id in survivors])
            cluster.links(blocked_links, blocked=True)
            pending_write = subprocess.Popen(
                [str(client_binary), str(cluster.ports[old_leader - 1]),
                 "put", "ghost", "lost"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            cluster.wait(lambda: cluster.status(old_leader)["log"] > baseline)
            ghost_index = cluster.status(old_leader)["log"]
            pending_read = subprocess.Popen(
                [str(client_binary), str(cluster.ports[old_leader - 1]),
                 "get", "base"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )

            new_leader = cluster.wait_leader(survivors)
            assert new_leader != old_leader
            winner = committed_index(cluster.client(new_leader, "put", "winner", "majority"))
            assert winner >= ghost_index
            assert cluster.client(new_leader, "get", "base").stdout == "kept\n"
            assert cluster.client(new_leader, "get", "winner").stdout == "majority\n"
            assert cluster.client(new_leader, "get", "ghost").stdout == "(not found)\n"
            isolated = cluster.status(old_leader)
            assert isolated["role"] == "leader" and (
                isolated["commit"] == baseline and isolated["applied"] == baseline
            ), isolated
            assert pending_write.poll() is None, "minority write completed"
            assert pending_read.poll() is None, "minority read completed"

            cluster.links(blocked_links, blocked=False)
            write_output, write_error = pending_write.communicate(timeout=5)
            assert pending_write.returncode == 4 and "outcome unknown" in write_output, (
                write_output, write_error, cluster.lines
            )
            read_output, read_error = pending_read.communicate(timeout=5)
            assert pending_read.returncode == 3 and "not leader" in read_output, (
                read_output, read_error, cluster.lines
            )
            cluster.wait(lambda: all(
                cluster.status(node_id)["log"] >= winner and
                cluster.status(node_id)["commit"] >= winner and
                cluster.status(node_id)["applied"] >= winner
                for node_id in (1, 2, 3)
            ))
            assert cluster.status(old_leader)["role"] == "follower"
            for node_id in (1, 2, 3):
                cluster.inspect(node_id, "base", "kept")
                cluster.inspect(node_id, "winner", "majority")
                cluster.inspect(node_id, "ghost", "(not found)")
            logs = [(cluster.root / str(node_id) / "raft.log").read_bytes()
                    for node_id in (1, 2, 3)]
            assert logs[0] == logs[1] == logs[2], "durable logs did not converge"
        finally:
            for pending in (pending_write, pending_read):
                if pending is not None and pending.poll() is None:
                    pending.kill()
                    pending.communicate()
            cluster.close()


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
