"""Focused runner for the audit-fix repro tests (shared server + standalone)."""

from __future__ import annotations

import sys

import harness
from harness import Server

import e2e_audit_fixes  # noqa: F401  (registers @test cases)


def main() -> int:
    print(f"collected {len(harness.TESTS)} shared-server audit tests\n")
    server = Server().start()
    try:
        failed = harness.run_registered(server)
    finally:
        server.stop()
    failed += e2e_audit_fixes.run_standalone_audit_tests()
    total = len(harness.TESTS) + 3
    print(f"\n{total - failed}/{total} audit checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
