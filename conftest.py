"""
conftest.py – Set JAVA_HOME to Java 21 before Spark starts (if available).

PySpark 3.5.x requires Java 17 or 21.
On macOS with Homebrew the Java 21 home is at the path below.
On Linux CI (GitHub Actions), setup-java pins JAVA_HOME automatically.
"""
from __future__ import annotations

import os
import subprocess
import sys
from typing import Optional


def _find_java21_home() -> Optional[str]:
    """Try common locations for Homebrew / system Java 21."""
    candidates = [
        # Homebrew ARM (Apple silicon)
        "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
        # Homebrew Intel
        "/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
        # Temurin installed via brew cask
        "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home",
        # GitHub Actions / Linux
        "/usr/lib/jvm/java-21-openjdk-amd64",
    ]
    for path in candidates:
        if os.path.isdir(path):
            return path
    # Try java_home utility (macOS)
    try:
        result = subprocess.run(
            ["/usr/libexec/java_home", "-v", "21"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            path = result.stdout.strip()
            if path:
                return path
    except Exception:
        pass
    return None


def pytest_configure(config):
    """Force JAVA_HOME to Java 21 before any test (and before Spark JVM starts)."""
    current_java = os.environ.get("JAVA_HOME", "")
    # If already pointing at Java 21 (CI), leave it alone
    if "21" in current_java:
        return

    java21 = _find_java21_home()
    if java21:
        os.environ["JAVA_HOME"] = java21
        # Also prepend the bin dir so `java` resolves correctly
        java_bin = os.path.join(java21, "bin")
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
        print(f"\n[conftest] JAVA_HOME → {java21}", file=sys.stderr)
    else:
        print(
            "\n[conftest] WARNING: Java 21 not found. "
            "PySpark 3.5 requires Java 17 or 21. "
            "Install with: brew install openjdk@21",
            file=sys.stderr,
        )

