"""Collect notices from modules actually compiled into the release commands.

Finding license files is an inventory operation, not a license compatibility or
copied-source clearance. The checked-in source disposition record remains separate.
"""
import json
from pathlib import Path
import subprocess


def dependency_notices(root, go_binary, env):
    raw = subprocess.check_output(
        [go_binary, "list", "-deps", "-json", "./cmd/djob", "./cmd/djob-worker", "./cmd/djob-console"],
        cwd=root, env=env, text=True)
    decoder, modules = json.JSONDecoder(), {}
    while raw.strip():
        package, length = decoder.raw_decode(raw.lstrip())
        raw = raw.lstrip()[length:]
        module = package.get("Module")
        if module and not module.get("Main"):
            modules[module["Path"]] = module
    goroot = Path(subprocess.check_output([go_binary, "env", "GOROOT"], cwd=root, env=env, text=True).strip())
    sections = ["djob compiled dependency license inventory\n\n"
                "Includes notices found in compiled module roots and their license directories.\n"
                "This does not clear copied source, interpret compatibility, or replace the\n"
                "THIRD_PARTY_NOTICES.txt provenance review. Engine and SDK licenses are separate.\n",
                "Go toolchain and standard library\n\n" + (goroot / "LICENSE").read_text()]
    for name, module in sorted(modules.items()):
        directory = Path(module["Dir"])
        paths = [p for p in directory.iterdir() if p.is_file()
                 and p.name.upper().startswith(("LICENSE", "COPYING", "NOTICE", "COPYRIGHT"))]
        for child in directory.iterdir():
            if child.is_dir() and not child.is_symlink() and child.name.lower() in ("licenses", "license"):
                paths.extend(p for p in child.rglob("*") if p.is_file() and not p.is_symlink())
        if not paths:
            raise RuntimeError("No dependency license files found for " + name)
        sections.append("Module: " + name + "@" + module.get("Version", "workspace") + "\n")
        for path in sorted(set(paths)):
            # Notice files must be readable text; do not silently omit an asset.
            sections.append("File: " + path.relative_to(directory).as_posix() + "\n\n" + path.read_text())
    return ("\n\n" + "=" * 72 + "\n\n").join(sections).encode()
