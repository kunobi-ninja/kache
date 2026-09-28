"""Read-only loopback S3 fixture with controlled response latency."""

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape


class ConcurrentHTTPServer(ThreadingHTTPServer):
    # The default backlog of five can add TCP retry delays to concurrent GETs.
    request_queue_size = 128


class ReadOnlyRemote:
    def __init__(self, root, delay_ms, log):
        if not 0 <= delay_ms <= 1000:
            raise ValueError("Response delay must be between 0 and 1000 ms")
        self.root = Path(root).resolve()
        self.delay_ms = delay_ms
        self.log = Path(log)
        self.lock = threading.Lock()
        # A fixed inventory makes pagination stable and never exposes new files.
        self.objects = {
            str(path.relative_to(self.root)): path
            for path in sorted(self.root.rglob("*"))
            if path.is_file() and not path.is_symlink()
        }
        if any(
            not path.resolve().is_relative_to(self.root)
            for path in self.objects.values()
        ):
            raise ValueError("Remote contains a path outside its root")

    def __enter__(self):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def log_message(self, *_args):
                pass

            def reply(
                self, status, body=b"", *, path=None, offset=0, size=None, headers=None
            ):
                size = len(body) if size is None else size
                self.send_response(status)
                self.send_header("Content-Length", str(size))
                self.send_header("Connection", "close")
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                delivered = 0
                try:
                    if self.command != "HEAD":
                        if path is None:
                            self.wfile.write(body)
                            delivered = len(body)
                        else:
                            with path.open("rb") as stream:
                                stream.seek(offset)
                                while delivered < size:
                                    block = stream.read(
                                        min(1024 * 1024, size - delivered)
                                    )
                                    if not block:
                                        raise OSError(
                                            "Seed object truncated during request"
                                        )
                                    self.wfile.write(block)
                                    delivered += len(block)
                finally:
                    record = {
                        "method": self.command,
                        "path": self.path,
                        "status": status,
                        "body_bytes": delivered,
                        "response_delay_ms": fixture.delay_ms,
                        "headers_after_ms": (self.headers_at - self.started) * 1000,
                    }
                    with fixture.lock, fixture.log.open("a") as output:
                        output.write(json.dumps(record) + "\n")
                    self.close_connection = True

            def error(self, status, code):
                self.reply(status, f"<Error><Code>{code}</Code></Error>".encode())

            def dispatch(self):
                self.started = time.monotonic()
                time.sleep(fixture.delay_ms / 1000)
                self.headers_at = time.monotonic()
                if self.command not in ("GET", "HEAD"):
                    return self.error(405, "MethodNotAllowed")
                url = urlsplit(self.path)
                name = unquote(url.path)
                query = parse_qs(url.query, keep_blank_values=True)
                if name.rstrip("/") == "/qualification" and query.get("list-type") == [
                    "2"
                ]:
                    prefix = query.get("prefix", [""])[0]
                    token = query.get("continuation-token", [""])[0]
                    delimiter = query.get("delimiter", [""])[0]
                    try:
                        limit = min(
                            1000, max(1, int(query.get("max-keys", ["1000"])[0]))
                        )
                    except ValueError:
                        return self.error(400, "InvalidArgument")
                    entries = {}
                    for key, path in fixture.objects.items():
                        if key.startswith(prefix):
                            rest = key[len(prefix) :]
                            if delimiter and delimiter in rest:
                                entries[
                                    prefix + rest.split(delimiter, 1)[0] + delimiter
                                ] = None
                            else:
                                entries[key] = path
                    keys = [key for key in sorted(entries) if key > token]
                    page, more = keys[:limit], len(keys) > limit
                    content = []
                    for key in page:
                        path = entries[key]
                        if path is None:
                            content.append(
                                f"<CommonPrefixes><Prefix>{escape(key)}</Prefix></CommonPrefixes>"
                            )
                        else:
                            content.append(
                                f"<Contents><Key>{escape(key)}</Key><Size>{path.stat().st_size}</Size>"
                                "<LastModified>2026-01-01T00:00:00.000Z</LastModified>"
                                '<ETag>"fixture"</ETag><StorageClass>STANDARD</StorageClass></Contents>'
                            )
                    continuation = (
                        f"<NextContinuationToken>{escape(page[-1])}</NextContinuationToken>"
                        if more
                        else ""
                    )
                    body = (
                        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                        f"<Name>qualification</Name><Prefix>{escape(prefix)}</Prefix>"
                        f"<KeyCount>{len(page)}</KeyCount><MaxKeys>{limit}</MaxKeys>"
                        f"<IsTruncated>{str(more).lower()}</IsTruncated>{continuation}"
                        + "".join(content)
                        + "</ListBucketResult>"
                    ).encode()
                    return self.reply(
                        200, body, headers={"Content-Type": "application/xml"}
                    )
                if not name.startswith("/qualification/"):
                    return self.error(404, "NoSuchBucket")
                path = fixture.objects.get(name[len("/qualification/") :])
                if path is None:
                    return self.error(404, "NoSuchKey")
                total = path.stat().st_size
                headers = {
                    "Content-Type": "application/octet-stream",
                    "ETag": '"fixture"',
                }
                start, end, status = 0, total - 1, 200
                if value := self.headers.get("Range"):
                    try:
                        unit, interval = value.split("=", 1)
                        left, right = interval.split("-", 1)
                        if unit != "bytes" or "," in interval:
                            raise ValueError()
                        start = int(left) if left else max(0, total - int(right))
                        end = (
                            min(total - 1, int(right)) if left and right else total - 1
                        )
                        if not 0 <= start <= end < total:
                            raise ValueError()
                    except ValueError:
                        return self.error(416, "InvalidRange")
                    status = 206
                    headers["Content-Range"] = f"bytes {start}-{end}/{total}"
                self.reply(
                    status,
                    path=path,
                    offset=start,
                    size=end - start + 1,
                    headers=headers,
                )

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = dispatch

        self.log.parent.mkdir(parents=True, exist_ok=True)
        self.server = ConcurrentHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
