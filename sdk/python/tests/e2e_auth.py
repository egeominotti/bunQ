"""E2E: auth — dedicated server with AUTH_TOKENS (own fixture, not shared)."""

from __future__ import annotations

from harness import Server, unique_name

from bunqueue import AuthError, CommandError, Connection, Queue


def run_auth_tests() -> int:
    """Runs against a dedicated auth-enabled server; returns failure count."""
    server = Server(extra_env={"AUTH_TOKENS": "secret-token"}).start()
    failed = 0
    try:
        # 1. valid token works
        try:
            with Queue(unique_name("auth"), port=server.port, token="secret-token") as queue:
                job = queue.add("t", {"x": 1})
                assert queue.get_state(job.id) in ("waiting", "prioritized")
            print("PASS e2e_auth.valid_token")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL e2e_auth.valid_token: {exc}")

        # 2. wrong token -> AuthError on connect
        try:
            conn = Connection(host="127.0.0.1", port=server.port, token="wrong-token")
            try:
                conn.connect()
                raise AssertionError("expected AuthError with wrong token")
            except AuthError:
                pass
            finally:
                conn.close()
            print("PASS e2e_auth.wrong_token_rejected")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL e2e_auth.wrong_token_rejected: {exc}")

        # 3. no token -> commands rejected
        try:
            conn = Connection(host="127.0.0.1", port=server.port)
            try:
                conn.call({"cmd": "PUSH", "queue": "authless", "data": {"name": "t"}})
                raise AssertionError("expected rejection without token")
            except CommandError:
                pass
            finally:
                conn.close()
            print("PASS e2e_auth.no_token_rejected")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL e2e_auth.no_token_rejected: {exc}")
    finally:
        server.stop()
    return failed
