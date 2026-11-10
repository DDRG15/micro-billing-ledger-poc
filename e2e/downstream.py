"""Downstream receiver for the end-to-end harness.

Plays the system the worker forwards events to, and records every request it gets in
/recv/received.log (one JSON line: time, outcome, X-Transaction-Id, X-Org-Id).

How it answers depends on the request path, so each tenant's downstream_url can pick a
behaviour, and on /recv/mode for the default path:

    path /redirect      302 to /sink (a correct worker must treat it as a failure)
    path /drip          a status line, then one header byte every 2 s (never finishes)
    mode ok             200 (default)
    mode fail           503
    mode slow:<s>       sleep <s> seconds, then 200

Change the mode while the stack runs:  echo fail > e2e/recv/mode
"""
import http.server
import json
import threading
import time

_LOG_LOCK = threading.Lock()


def _mode() -> str:
    try:
        with open("/recv/mode") as f:
            return f.read().strip() or "ok"
    except OSError:
        return "ok"


class Receiver(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:  # quiet: the JSON log is the record
        pass

    def _record(self, outcome: str) -> None:
        line = {"t": time.time(), "outcome": outcome,
                "tx": self.headers.get("X-Transaction-Id"), "org": self.headers.get("X-Org-Id")}
        with _LOG_LOCK, open("/recv/received.log", "a") as f:
            f.write(json.dumps(line) + "\n")

    def _reply(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path.startswith("/redirect"):
            self.send_response(302)
            self.send_header("Location", "/sink")
            self.send_header("Content-Length", "0")
            self.end_headers()
            self._record("redirected-302")
            return
        if self.path.startswith("/drip"):
            self._record("drip-started")
            self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Drip: ")
            for _ in range(10_000):
                self.wfile.write(b"a")
                self.wfile.flush()
                time.sleep(2)
            return
        mode = _mode()
        if mode == "fail":
            self._reply(503)
            self._record("fail")
            return
        if mode.startswith("slow:"):
            time.sleep(float(mode[5:]))
        self._reply(200)
        self._record("ok")

    def do_GET(self) -> None:
        # Only a worker that followed a redirect would send this.
        self._reply(200)
        self._record(f"GET {self.path} (no body)")


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("0.0.0.0", 9000), Receiver).serve_forever()  # nosec B104 — container-internal test service
