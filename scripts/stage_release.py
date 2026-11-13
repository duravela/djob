"""Verify and stage release executables for the container qualification harness."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile

from release_licenses import LICENSE_NAMES

ROOT = Path(__file__).resolve().parent.parent

def sha(data):
    return hashlib.sha256(data).hexdigest()

def main():
    release, arch, destination = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    sums = {}
    for line in (release / "SHA256SUMS").read_text().splitlines():
        value, name = line.split("  ", 1)
        if Path(name).name != name or name in sums:
            raise ValueError("invalid or duplicate checksum path")
        sums[name] = value
    manifest_data = (release / "manifest.json").read_bytes()
    if sha(manifest_data) != sums.get("manifest.json"):
        raise ValueError("manifest checksum mismatch")
    manifest = json.loads(manifest_data)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if manifest["revision"] != revision or manifest["source_clean"] is not True:
        raise ValueError("fixture source and immutable release revision differ")
    target = manifest["targets"]["linux/" + arch]
    name = target["archive"]
    if Path(name).name != name:
        raise ValueError("unsafe archive path")
    data = (release / name).read_bytes()
    if sha(data) != sums.get(name) or sha(data) != target["archive_sha256"]:
        raise ValueError("archive checksum mismatch")
    notices = manifest.get("license_files_sha256", {})
    if set(notices) != LICENSE_NAMES:
        raise ValueError("release license inventory is incomplete")
    binaries = {}
    licenses = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for member in archive:
            is_license = member.name in notices
            entries = licenses if is_license else binaries
            if member.name not in {"djob", "djob-worker", "djob-console"} | set(notices) or not member.isfile() or member.name in entries:
                raise ValueError("unexpected release archive entry")
            content = archive.extractfile(member).read()
            expected = notices if is_license else target["binaries_sha256"]
            if sha(content) != expected[member.name]:
                raise ValueError("license checksum mismatch" if is_license else "executable checksum mismatch")
            entries[member.name] = content
    if set(binaries) != {"djob", "djob-worker", "djob-console"}:
        raise ValueError("release archive is incomplete")
    if set(licenses) != set(notices):
        raise ValueError("release archive licenses are incomplete")
    for name, content in binaries.items():
        path = destination / name
        path.write_bytes(content)
        path.chmod(0o755)
    for name, content in licenses.items():
        path = destination / name
        path.write_bytes(content)
        path.chmod(0o644)
    (destination / "release-manifest.json").write_bytes(manifest_data)
    print("Using verified release " + manifest["version"] + " at " + revision, flush=True)

if __name__ == "__main__":
    main()
