"""Exercise leader hints and uncertain replies with one stable write request."""

import re
import socket
import struct
import subprocess
import sys
import threading
from pathlib import Path


def receive_exact(connection, length):
    chunks = bytearray()
    while len(chunks) < length:
        part = connection.recv(length - len(chunks))
        if not part:
            raise AssertionError("client closed before sending its request")
        chunks.extend(part)
    return bytes(chunks)


def serve(listener, node_id, response, received, order, errors):
    try:
        listener.settimeout(12)
        with listener.accept()[0] as connection:
            connection.settimeout(3)
            length = struct.unpack(">I", receive_exact(connection, 4))[0]
            body = receive_exact(connection, length)
            received[node_id] = body
            order.append(node_id)
            if response is not None:
                connection.sendall(struct.pack(">I", len(response)) + response)
    except Exception as error:
        errors.append(error)
    finally:
        listener.close()


def redirect_loop(listener, node_id, leader_hint, stop, order, errors):
    reply = struct.pack(">BBBQQQ", 1, 6, 2, 7, 0, leader_hint)
    try:
        listener.settimeout(0.1)
        while not stop.is_set():
            try:
                connection, _ = listener.accept()
            except socket.timeout:
                continue
            with connection:
                connection.settimeout(3)
                length = struct.unpack(">I", receive_exact(connection, 4))[0]
                receive_exact(connection, length)
                order.append(node_id)
                connection.sendall(struct.pack(">I", len(reply)) + reply)
    except Exception as error:
        errors.append(error)
    finally:
        listener.close()


def test_write_retry(client_binary):
    listeners = []
    for _ in range(3):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listeners.append(listener)
    ports = [listener.getsockname()[1] for listener in listeners]
    received = {}
    order = []
    errors = []
    # Node 1 redirects directly to node 3. Node 3 drops the reply after
    # receiving the write. The retry then finds node 2.
    responses = {
        1: struct.pack(">BBBQQQ", 1, 6, 2, 7, 0, 3),
        2: struct.pack(">BBBQQQ", 1, 6, 1, 7, 9, 2),
        3: None,
    }
    threads = [threading.Thread(
        target=serve,
        args=(listeners[index - 1], index, responses[index],
              received, order, errors),
        daemon=True,
    ) for index in (1, 2, 3)]
    for thread in threads:
        thread.start()
    result = subprocess.run(
        [str(client_binary), *(str(port) for port in ports), "put", "x", "10"],
        text=True, capture_output=True, timeout=12,
    )
    for thread in threads:
        thread.join(timeout=1)
    assert not errors, errors
    assert all(not thread.is_alive() for thread in threads), order
    assert result.returncode == 0 and result.stdout == "committed term 7 index 9\n", result
    assert order == [1, 3, 2], order
    assert received[1] == received[2] == received[3], "retry changed the write request"
    match = re.search(r"request id ([0-9a-f]{32})", result.stderr)
    assert match, result.stderr
    assert received[1][:3] == bytes((1, 5, 2)), received[1][:3]
    assert received[1][12:28].hex() == match.group(1), "printed ID differs from sent ID"


def test_read_routing(client_binary):
    listeners = []
    for _ in range(3):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listeners.append(listener)
    ports = [listener.getsockname()[1] for listener in listeners]
    listeners[1].close()
    received = {}
    order = []
    errors = []
    value = b"10"
    responses = {
        1: struct.pack(">BBBQQQI", 1, 8, 3, 7, 0, 3, 0),
        3: struct.pack(">BBBQQQI", 1, 8, 1, 7, 9, 3, len(value)) + value,
    }
    threads = [threading.Thread(
        target=serve,
        args=(listeners[index - 1], index, responses[index],
              received, order, errors),
        daemon=True,
    ) for index in (1, 3)]
    for thread in threads:
        thread.start()
    result = subprocess.run(
        [str(client_binary), *(str(port) for port in ports), "get", "x"],
        text=True, capture_output=True, timeout=12,
    )
    for thread in threads:
        thread.join(timeout=1)
    assert not errors, errors
    assert all(not thread.is_alive() for thread in threads), order
    assert result.returncode == 0 and result.stdout == "10\n", result
    assert order == [1, 3], order
    assert received[1] == received[3], "GET changed while following leader hint"


def test_stale_hint_cycle(client_binary):
    listeners = []
    for _ in range(3):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listeners.append(listener)
    ports = [listener.getsockname()[1] for listener in listeners]
    stop = threading.Event()
    order = []
    errors = []
    received = {}
    committed = struct.pack(">BBBQQQ", 1, 6, 1, 7, 9, 3)
    threads = [
        threading.Thread(target=redirect_loop,
                         args=(listeners[0], 1, 2, stop, order, errors), daemon=True),
        threading.Thread(target=redirect_loop,
                         args=(listeners[1], 2, 1, stop, order, errors), daemon=True),
        threading.Thread(target=serve,
                         args=(listeners[2], 3, committed, received, order, errors),
                         daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        result = subprocess.run(
            [str(client_binary), *(str(port) for port in ports), "put", "x", "10"],
            text=True, capture_output=True, timeout=12,
        )
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=1)
    assert not errors, errors
    assert all(not thread.is_alive() for thread in threads), order
    assert result.returncode == 0, result
    assert order[:3] == [1, 2, 3], order


if __name__ == "__main__":
    test_write_retry(Path(sys.argv[1]))
    test_read_routing(Path(sys.argv[1]))
    test_stale_hint_cycle(Path(sys.argv[1]))
