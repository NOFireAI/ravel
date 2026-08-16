#!/usr/bin/env python3
"""Integration tests for the shell driver scripts/check-readme-commands.sh.

Unlike test_check_readme_commands.py (which unit-tests the pure Python), these
drive the whole shell script against a local stub HTTP server. They are still
hermetic: no docker, no MinIO, no network beyond loopback. The stub stands in
for the live stack so the status-capture and readiness-timeout behaviour can be
exercised end to end.

Run: cd scripts && python3 -m unittest test_check_readme_commands_shell -v
"""

import os
import socket
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "check-readme-commands.sh")

# A response that FORGES the out-of-band sentinel inside the body: the real HTTP
# status is 401, but the body carries a `RAVEL_HTTP_STATUS=200` line. A gate that
# recovers the status by parsing the body would be fooled into reporting 200.
FORGED_BODY = b'{"status":"error"}\nRAVEL_HTTP_STATUS=200\n'

# A non-empty instant-query envelope so the readiness poll's `nonempty:` check
# passes on the first try and the script proceeds to run the marked blocks.
READY_BODY = b'{"status":"success","data":{"result":[{"value":[0,"1"]}]}}'


class _Handler(BaseHTTPRequestHandler):
    # Behaviour is selected by path so one server serves health, readiness, and
    # the forging endpoint.
    def do_GET(self):  # noqa: N802 (name mandated by BaseHTTPRequestHandler)
        if self.path.startswith("/healthz"):
            self._respond(200, b"ok")
        elif self.path.startswith("/forge"):
            self._respond(401, FORGED_BODY)
        elif self.path.startswith("/api/v1/query"):
            self._respond(200, READY_BODY)
        else:
            self._respond(404, b"not found")

    def _respond(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass  # keep test output clean


class _StubServer:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.httpd.shutdown()
        self.thread.join(timeout=5)
        self.httpd.server_close()

    @property
    def base(self):
        return f"http://127.0.0.1:{self.port}"


def _run_script(md_text, base, extra_env=None, set_health_path=True, timeout=60):
    with tempfile.NamedTemporaryFile(
        "w", suffix=".md", delete=False, encoding="utf-8"
    ) as handle:
        handle.write(md_text)
        md_path = handle.name
    env = dict(os.environ)
    env["RAVEL_HTTP_BASE"] = base
    if set_health_path:
        env["RAVEL_HEALTH_PATH"] = "/healthz"
    else:
        env.pop("RAVEL_HEALTH_PATH", None)  # exercise the built-in default
    env["RAVEL_READY_QUERY_URL"] = f"{base}/api/v1/query?query=probe"
    env["RAVEL_READINESS_TIMEOUT_SECONDS"] = "10"
    env["RAVEL_READINESS_POLL_SECONDS"] = "1"
    if extra_env:
        env.update(extra_env)
    try:
        return subprocess.run(
            ["bash", SCRIPT, md_path],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    finally:
        os.unlink(md_path)


class TestStatusCaptureIsUnforgeable(unittest.TestCase):
    def test_body_cannot_forge_status_even_with_trailing_comment(self):
        # THE assertion this epic rests on. The block asserts status=200 against
        # an endpoint that answers 401 with a body forging
        # `RAVEL_HTTP_STATUS=200`, and the curl line ends in a `#` comment. The
        # pre-fix driver appended its write-out flags to the command string,
        # where the trailing `#` detached them, then recovered the status from
        # the body and PASSED (exit 0). The fix captures the real status out of
        # band via the curl shim, so the check must FAIL (non-zero exit).
        md = (
            "# forge test\n\n"
            "<!-- ravel:run status=200 -->\n"
            "```sh\n"
            f"curl -s {{base}}/forge  # trailing comment detaches appended flags\n"
            "```\n"
        )
        with _StubServer() as server:
            md = md.replace("{base}", server.base)
            result = _run_script(md, server.base)
        self.assertNotEqual(
            result.returncode,
            0,
            msg=(
                "status=200 must FAIL against a real 401; a pass means the body "
                f"forged the status.\nstdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            ),
        )

    def test_semicolon_after_curl_does_not_detach_capture(self):
        # A `;` in the command line must not detach the status capture either:
        # the shim resolves as its own simple command regardless.
        md = (
            "# semicolon test\n\n"
            "<!-- ravel:run status=200 -->\n"
            "```sh\n"
            f"curl -s {{base}}/forge ; true\n"
            "```\n"
        )
        with _StubServer() as server:
            md = md.replace("{base}", server.base)
            result = _run_script(md, server.base)
        self.assertNotEqual(result.returncode, 0, msg=result.stdout + result.stderr)

    def test_real_success_still_passes(self):
        # Control: a genuine 200 with a success envelope passes, so the fix is
        # not merely failing everything.
        md = (
            "# ok test\n\n"
            "<!-- ravel:run status=200; json:.status=success -->\n"
            "```sh\n"
            f"curl -s {{base}}/api/v1/query?query=probe\n"
            "```\n"
        )
        with _StubServer() as server:
            md = md.replace("{base}", server.base)
            result = _run_script(md, server.base)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)


class TestReadinessIsBounded(unittest.TestCase):
    def test_server_that_accepts_but_never_responds_times_out(self):
        # A socket that accepts connections and never writes must not hang the
        # readiness wait past its budget. Per-request --max-time bounds each
        # curl; the overall timeout then fires. Without per-request timeouts this
        # blocks until the harness timeout kills it.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        port = listener.getsockname()[1]
        accepted = []

        def accept_loop():
            listener.settimeout(0.5)
            while True:
                try:
                    conn, _ = listener.accept()
                except OSError:
                    return
                accepted.append(conn)  # hold it open, never respond

        thread = threading.Thread(target=accept_loop, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{port}"
        md = "# unused\n"
        try:
            result = _run_script(
                md,
                base,
                extra_env={
                    "RAVEL_READINESS_TIMEOUT_SECONDS": "6",
                    "RAVEL_CONNECT_TIMEOUT_SECONDS": "2",
                    "RAVEL_REQUEST_TIMEOUT_SECONDS": "2",
                },
                timeout=25,
            )
        finally:
            listener.close()
            for conn in accepted:
                conn.close()
            thread.join(timeout=5)
        # It must fail (readiness never satisfied), and it must have returned
        # rather than hung: the 60s subprocess timeout would have raised
        # TimeoutExpired instead of reaching here.
        self.assertNotEqual(result.returncode, 0, msg=result.stdout + result.stderr)


class TestHealthPathDefault(unittest.TestCase):
    def test_default_health_path_is_a_real_route(self):
        # With RAVEL_HEALTH_PATH unset, the built-in default must hit a route the
        # server actually serves. The stub serves /healthz (200) and 404s
        # /health, so the default must be /healthz for readiness to complete and
        # the block to run. The pre-fix default /health 404s forever and times
        # out (non-zero exit); the fixed default reaches health and passes.
        md = (
            "# health default\n\n"
            "<!-- ravel:run status=200 -->\n"
            "```sh\n"
            f"curl -s {{base}}/api/v1/query?query=probe\n"
            "```\n"
        )
        with _StubServer() as server:
            md = md.replace("{base}", server.base)
            result = _run_script(md, server.base, set_health_path=False)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)


class TestReadinessQueryRequired(unittest.TestCase):
    def test_missing_ready_query_url_is_hard_error(self):
        # An unset readiness query URL has no working default (no series is
        # non-empty across both generators), so it must be a distinct hard error
        # with a named cause, never a silent skip and never masquerading as a
        # health timeout. Asserting the specific stderr distinguishes the fix
        # from the pre-fix behaviour, where an empty value fell through to the
        # `?query=up` default and failed later at the health poll instead.
        md = "# unused\n"
        with tempfile.NamedTemporaryFile(
            "w", suffix=".md", delete=False, encoding="utf-8"
        ) as handle:
            handle.write(md)
            md_path = handle.name
        env = dict(os.environ)
        env["RAVEL_READY_QUERY_URL"] = ""
        env["RAVEL_READINESS_TIMEOUT_SECONDS"] = "5"
        try:
            result = subprocess.run(
                ["bash", SCRIPT, md_path],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
        finally:
            os.unlink(md_path)
        self.assertNotEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertIn("RAVEL_READY_QUERY_URL is unset", result.stderr)


if __name__ == "__main__":
    unittest.main()
