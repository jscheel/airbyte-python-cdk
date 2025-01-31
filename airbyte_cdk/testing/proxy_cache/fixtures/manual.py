import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Generator

import psutil
import pytest
import requests
from pytest import MonkeyPatch


def kill_process_on_port(port: int) -> None:
    # First try to kill by command line
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and ('mitmdump' in cmdline[0] or 'mitmproxy' in cmdline[0]):
                proc.terminate()
                proc.wait(timeout=1)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            pass
    
    # Then try to kill anything on the port
    for conn in psutil.net_connections(kind='tcp'):
        try:
            if conn.laddr.port == port and conn.pid:
                proc = psutil.Process(conn.pid)
                proc.terminate()
                proc.wait(timeout=1)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            pass


@pytest.fixture
def mitmproxy_cache(monkeypatch: MonkeyPatch) -> Generator[None, None, None]:
    proxy_port = 8081
    proxy_dir = Path("/tmp/mitmproxy_manual")
    proxy_dir.mkdir(exist_ok=True, parents=True)
    
    # Kill any existing mitmproxy processes using this port
    kill_process_on_port(proxy_port)
    
    process = subprocess.Popen(
        [
            "mitmdump",
            "--listen-port", str(proxy_port),
            "-s", str(Path(__file__).parent.parent / "addon.py"),
            "--mode", "regular"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Wait for proxy to start
    time.sleep(2)
    
    # Check if process is still running
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        raise RuntimeError(f"mitmproxy failed to start. stdout: {stdout}, stderr: {stderr}")

    monkeypatch.setenv("HTTP_PROXY", f"http://localhost:{proxy_port}")
    monkeypatch.setenv("HTTPS_PROXY", f"http://localhost:{proxy_port}")
    monkeypatch.setenv("no_proxy", "")

    yield

    process.terminate()
    process.wait(timeout=5)
    
    if process.returncode is None:
        process.kill()
        process.wait(timeout=5)
