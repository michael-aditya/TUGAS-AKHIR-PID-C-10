#!/usr/bin/env python3
"""Wait for one or more host:port TCP targets to accept connections, then exec the command."""
from __future__ import annotations

import os
import socket
import sys
import time


def wait_for(address: str, timeout: float | None = None, interval: float = 1.0) -> bool:
    host, port_str = address.rsplit(":", 1)
    port = int(port_str)
    start = time.monotonic()
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except OSError:
            pass
        if timeout is not None and (time.monotonic() - start) > timeout:
            return False
        time.sleep(interval)


def main() -> None:
    if "--" not in sys.argv:
        print("Usage: wait_for.py host:port [...] -- cmd args...", file=sys.stderr)
        sys.exit(2)

    idx = sys.argv.index("--")
    targets = sys.argv[1:idx]
    cmd = sys.argv[idx + 1 :]
    if not targets or not cmd:
        print("Usage: wait_for.py host:port [...] -- cmd args...", file=sys.stderr)
        sys.exit(2)

    timeout_env = os.getenv("WAIT_TIMEOUT")
    timeout = float(timeout_env) if timeout_env else None

    for target in targets:
        print(f"[wait_for] Waiting for {target}...", flush=True)
        if not wait_for(target, timeout=timeout):
            print(f"[wait_for] Timed out waiting for {target}", file=sys.stderr)
            sys.exit(1)
        print(f"[wait_for] {target} is available", flush=True)

    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
