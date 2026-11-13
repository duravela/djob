"""Build immutable Linux controller, worker and management console archives."""

import argparse
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

from go_toolchain import local_go_environment, selected_go
from release_licenses import LICENSE_PATHS, LICENSE_NAMES
from dependency_licenses import dependency_notices

ROOT = Path(__file__).resolve().parent.parent
SYMBOL = "github.com/duravela/djob/internal/buildinfo."


def output(args, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True).strip()


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as reader:
        for block in iter(lambda: reader.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--target", action="append", choices=["linux/amd64", "linux/arm64"])
    args = parser.parse_args()
    if not re.fullmatch(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", args.version):
        parser.error("version must be a Go semantic version, including v prefix")
    if output(["git", "status", "--porcelain"]):
        parser.error("release builds require a clean committed source tree")
    destination = args.output.resolve()
    if destination == ROOT or ROOT in destination.parents:
        parser.error("output must be outside the source checkout")
    if destination.exists():
        parser.error("output must be a new directory; release artifacts are immutable")
    destination.parent.mkdir(parents=True, exist_ok=True)
    revision = output(["git", "rev-parse", "HEAD"])
    epoch = int(output(["git", "show", "-s", "--format=%ct", "HEAD"]))
    timestamp = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    go_binary = selected_go(ROOT)
    go_version = output([go_binary, "env", "GOVERSION"], local_go_environment(go_binary))
    flags = "-s -w -buildid= " + " ".join("-X " + SYMBOL + key + "=" + value for key, value in {"Version": args.version, "Revision": revision, "BuildTime": timestamp}.items())
    manifest = {"version": args.version, "revision": revision, "source_clean": True, "source_date_epoch": epoch, "build_time": timestamp, "go_version": go_version, "targets": {}}
    with tempfile.TemporaryDirectory(prefix="djob-release-", dir=destination.parent) as work:
        base = Path(work)
        release = base / "release"
        release.mkdir()
        manifest["license_files_sha256"] = {}
        for name, source in LICENSE_PATHS.items():
            path = release / name
            path.write_bytes((ROOT / source).read_bytes())
            manifest["license_files_sha256"][name] = digest(path)
        for target in dict.fromkeys(args.target or ["linux/amd64", "linux/arm64"]):
            platform, arch = target.split("/")
            binaries = base / arch
            binaries.mkdir()
            env = local_go_environment(go_binary, GOWORK=str(ROOT / "go.work"), CGO_ENABLED="0", GOOS=platform, GOARCH=arch, GOAMD64="v1", GOARM64="v8.0", GOFLAGS="")
            hashes = {}
            notices = dependency_notices(ROOT, go_binary, env)
            notice_path = release / "DEPENDENCY_LICENSES.txt"
            if notice_path.exists() and notice_path.read_bytes() != notices:
                raise RuntimeError("target dependency notices differ; use separate release directories")
            notice_path.write_bytes(notices)
            manifest["license_files_sha256"][notice_path.name] = digest(notice_path)
            for name in ["djob", "djob-worker", "djob-console"]:
                path = binaries / name
                subprocess.run([go_binary, "build", "-p=2", "-trimpath", "-buildvcs=true", "-ldflags", flags, "-o", str(path), "./cmd/" + name], cwd=ROOT, env=env, check=True)
                if output([go_binary, "version", str(path)], env) != str(path) + ": " + go_version:
                    raise RuntimeError(name + " was built with a different Go toolchain than its manifest")
                hashes[name] = digest(path)
            archive = release / ("djob_" + args.version + "_" + platform + "_" + arch + ".tar.gz")
            with archive.open("wb") as file, gzip.GzipFile(filename="", mode="wb", fileobj=file, mtime=0) as compressed, tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as tar:
                for name in sorted(set(hashes) | LICENSE_NAMES):
                    path = binaries / name if name in hashes else release / name
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = path.stat().st_size, 0o755 if name in hashes else 0o644, epoch
                    with path.open("rb") as reader:
                        tar.addfile(info, reader)
            manifest["targets"][target] = {"archive": archive.name, "archive_sha256": digest(archive), "binaries_sha256": hashes}
            print("Built " + target + " " + manifest["targets"][target]["archive_sha256"], flush=True)
        manifest["tools"] = {}
        for name in ["manage-release.py", "replace-member.py", "replace-kubernetes-member.py"]:
            tool = release / name
            tool.write_bytes((ROOT / "scripts" / name).read_bytes())
            tool.chmod(0o755)
            manifest["tools"][name] = {"sha256": digest(tool), "requires": "Python 3.10+ (POSIX)" + ("; kubectl with explicit kubeconfig/context" if name == "replace-kubernetes-member.py" else "")}
        (release / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        checksums = [digest(path) + "  " + path.name for path in sorted(release.iterdir())]
        (release / "SHA256SUMS").write_text("\n".join(checksums) + "\n")
        if output(["git", "rev-parse", "HEAD"]) != revision or output(["git", "status", "--porcelain"]):
            raise RuntimeError("source changed during release build; refusing publication")
        release.rename(destination)
    print("Release " + args.version + " at " + revision + ": " + str(destination), flush=True)


if __name__ == "__main__":
    main()
