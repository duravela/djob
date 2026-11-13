"""Keep subprocesses on the Go toolchain selected by the caller."""
import os
from pathlib import Path
import subprocess


def selected_go(cwd):
    root = subprocess.check_output(["go", "env", "GOROOT"], cwd=cwd, text=True).strip()
    executable = Path(root) / "bin" / ("go.exe" if os.name == "nt" else "go")
    if not root or not executable.is_file():
        raise RuntimeError("The selected Go toolchain has no executable: " + str(executable))
    return str(executable)


def local_go_environment(executable, **overrides):
    env = dict(os.environ, **overrides)
    env["GOROOT"] = str(Path(executable).parent.parent)
    env["PATH"] = str(Path(executable).parent) + os.pathsep + env.get("PATH", "")
    env["GOTOOLCHAIN"] = "local"
    return env
