#!/usr/bin/env python3
"""A bare RTSP client: plays a stream over TCP, then reads until the server
closes the connection. Like some restreamers, it ignores RTCP BYE: only a
closed connection tells it that the stream is over.

Usage: rtsp_client.py URL SECONDS

Exits 0 once the server closed the connection, 1 if it was still open after
SECONDS, and 2 if the stream could not be played.
"""
import re
import socket
import sys
import time
from urllib.parse import urlparse


def reply(sock):
    """Reads one RTSP reply: its status, headers and body"""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("the server closed the connection")
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    head = head.decode(errors="replace")
    length = re.search(r"(?im)^content-length:\s*(\d+)", head)
    length = int(length.group(1)) if length else 0
    while len(body) < length:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("the server closed the connection")
        body += chunk
    status = int(head.split()[1])
    return status, head, body[:length].decode(errors="replace")


def request(sock, method, url, cseq, *headers):
    lines = [f"{method} {url} RTSP/1.0", f"CSeq: {cseq}", *headers, "", ""]
    sock.sendall("\r\n".join(lines).encode())
    status, head, body = reply(sock)
    if status != 200:
        raise RuntimeError(f"{method} got {status}")
    return head, body


def main() -> int:
    url, seconds = sys.argv[1], float(sys.argv[2])
    parsed = urlparse(url)
    sock = socket.create_connection((parsed.hostname, parsed.port or 554), timeout=30)
    try:
        _, sdp = request(sock, "DESCRIBE", url, 1, "Accept: application/sdp")
        control = re.search(r"(?m)^a=control:(\S+)", sdp.split("m=", 1)[1]).group(1)
        track = control if "://" in control else f"{url.rstrip('/')}/{control}"
        head, _ = request(
            sock, "SETUP", track, 2, "Transport: RTP/AVP/TCP;unicast;interleaved=0-1"
        )
        session = re.search(r"(?im)^session:\s*([^;\r\n]+)", head).group(1)
        request(sock, "PLAY", url, 3, f"Session: {session}")
    except (OSError, RuntimeError, AttributeError, IndexError) as e:
        print(f"could not play {url}: {e}", file=sys.stderr)
        return 2
    print("playing", file=sys.stderr)

    start = time.monotonic()
    deadline = start + seconds
    while time.monotonic() < deadline:
        sock.settimeout(max(0.1, deadline - time.monotonic()))
        try:
            if not sock.recv(65536):
                print(f"closed after {time.monotonic() - start:.1f}s", file=sys.stderr)
                return 0
        except socket.timeout:
            break
        except OSError as e:
            print(f"closed after {time.monotonic() - start:.1f}s: {e}", file=sys.stderr)
            return 0
    print(f"still open after {seconds:.0f}s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
