#!/usr/bin/env python3
"""A TCP proxy that can drop its clients without the server noticing.

Usage: tcp_proxy.py LISTEN_PORT TARGET_PORT

On SIGUSR1, it closes the connections of its clients, and leaves those to
the server open and silent, as a network that fails between the two does.
The server then only notices when the connection times out.

On SIGUSR2, it stops letting clients connect, or lets them again.
"""
import signal
import socket
import sys
import threading

dropped = []
live = []
lock = threading.Lock()
refusing = False


def pipe(source, sink):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            sink.sendall(data)
    except OSError:
        pass
    # The other way round stops on its own: a client dropped by SIGUSR1
    # leaves the server's side open on purpose
    with lock:
        if (source, sink) in live:
            live.remove((source, sink))


def drop(*_):
    with lock:
        for client, server in list(live):
            # Keep the server's side open, and silent
            dropped.append(server)
            try:
                client.shutdown(socket.SHUT_RDWR)
                client.close()
            except OSError:
                pass
        live.clear()


def refuse(*_):
    global refusing
    refusing = not refusing


def main() -> int:
    listen_port, target_port = int(sys.argv[1]), int(sys.argv[2])
    signal.signal(signal.SIGUSR1, drop)
    signal.signal(signal.SIGUSR2, refuse)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", listen_port))
    listener.listen()
    while True:
        try:
            client, _ = listener.accept()
        except InterruptedError:
            continue
        if refusing:
            client.close()
            continue
        server = socket.create_connection(("127.0.0.1", target_port))
        with lock:
            live.append((client, server))
        threading.Thread(target=pipe, args=(client, server), daemon=True).start()
        threading.Thread(target=pipe, args=(server, client), daemon=True).start()


if __name__ == "__main__":
    sys.exit(main())
