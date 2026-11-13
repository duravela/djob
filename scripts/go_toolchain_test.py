"""Exercise release scripts with different launcher and selected Go versions."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

from release_licenses import LICENSE_PATHS, LICENSE_NAMES

SCRIPTS = Path(__file__).resolve().parent


class GoToolchainTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="djob-toolchain-test-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.source = self.base / "source"
        (self.source / "scripts").mkdir(parents=True)
        for name in ["build_release.py", "go_toolchain.py", "release_licenses.py", "dependency_licenses.py", "stage_release.py", "manage-release.py", "replace-member.py", "replace-kubernetes-member.py"]:
            if (SCRIPTS / name).exists():
                shutil.copyfile(SCRIPTS / name, self.source / "scripts" / name)
        for relative in LICENSE_PATHS.values():
            destination = self.source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(SCRIPTS.parent / relative, destination)
        (self.source / ".gitignore").write_text("__pycache__/\n")
        (self.source / "go.mod").write_text("module example.com/toolchain-test\n\ngo 1.26.0\n")
        for version in ["default", "selected"]:
            go = self.base / "toolchains" / version / "bin/go"
            go.parent.mkdir(parents=True)
            shutil.copyfile(SCRIPTS.parent / "licenses/BSD-golang.txt", go.parent.parent / "LICENSE")
            go.write_text("#!" + sys.executable + "\n" + '''import os
from pathlib import Path
import sys

args = sys.argv[1:]
root = Path(__file__).resolve().parents[2]
selected = Path(__file__).resolve().parent.parent.name == "selected" or os.environ.get("GOTOOLCHAIN") == "go1.27.4"
version = "go1.27.4" if selected else "go1.26.10"
if args == ["env", "GOROOT"]:
    print(root / ("selected" if selected else "default"))
elif args == ["env", "GOVERSION"]:
    print(version)
elif args[:3] == ["list", "-deps", "-json"]:
    print('{"ImportPath":"fmt","Standard":true}')
elif args[0] == "build":
    output = Path(args[args.index("-o") + 1])
    output.write_text("go1.26.10" if os.environ.get("DJOB_TEST_WRONG_COMPILER") else version)
elif args[0] == "version":
    print(args[-1] + ": " + Path(args[-1]).read_text())
else:
    raise SystemExit("Unexpected Go invocation: " + repr(args))
''')
            go.chmod(0o755)
        self.env = dict(os.environ, GOTOOLCHAIN="go1.27.4",
                        PATH=str(self.base / "toolchains/default/bin") + os.pathsep + os.environ.get("PATH", ""))
        for args in [["init", "--quiet"], ["add", "."],
                     ["-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                      "-c", "user.name=Release test", "-c", "user.email=release-test@example.invalid",
                      "commit", "--quiet", "-m", "Local release fixture"]]:
            subprocess.run(["git", *args], cwd=self.source, env=self.env, check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def build(self, **changes):
        self.release = self.base / "release"
        return subprocess.run([sys.executable, "scripts/build_release.py", "--version", "v0.0.0-test",
                               "--output", str(self.release)], cwd=self.source,
                              env=dict(self.env, **changes), text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def test_requested_toolchain_is_used_for_archives(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stdout)
        manifest = json.loads((self.release / "manifest.json").read_text())
        self.assertEqual(manifest["go_version"], "go1.27.4")
        for name in ["manage-release.py", "replace-member.py", "replace-kubernetes-member.py"]:
            tool = (self.release / name).read_bytes()
            self.assertEqual(tool, (SCRIPTS / name).read_bytes())
            self.assertEqual(hashlib.sha256(tool).hexdigest(), manifest["tools"][name]["sha256"])
        packaged_help = subprocess.run([sys.executable, str(self.release / "replace-member.py"), "--help"], capture_output=True, text=True)
        self.assertEqual(packaged_help.returncode, 0, packaged_help.stderr)
        self.assertIn("apply", packaged_help.stdout)
        for target in manifest["targets"].values():
            with tarfile.open(self.release / target["archive"], "r:gz") as archive:
                self.assertEqual(set(archive.getnames()), {"djob", "djob-worker", "djob-console"} | LICENSE_NAMES)
                for entry in archive:
                    content = archive.extractfile(entry).read()
                    if entry.name in LICENSE_NAMES:
                        if entry.name in LICENSE_PATHS:
                            self.assertEqual(content, (SCRIPTS.parent / LICENSE_PATHS[entry.name]).read_bytes())
                        else:
                            self.assertIn(b"Go toolchain and standard library", content)
                        self.assertEqual(hashlib.sha256(content).hexdigest(), manifest["license_files_sha256"][entry.name])
                        self.assertEqual(content, (self.release / entry.name).read_bytes())
                        self.assertEqual(entry.mode, 0o644)
                    else:
                        self.assertEqual(content.decode(), manifest["go_version"],
                                         "archive compiler differs from its manifest")

    def test_stage_verifies_license_contents_and_requires_every_notice(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stdout)
        destination = self.base / "staged"
        destination.mkdir()
        command = [sys.executable, "scripts/stage_release.py", str(self.release), "arm64", str(destination)]
        staged = subprocess.run(command, cwd=self.source, env=self.env, capture_output=True, text=True)
        self.assertEqual(staged.returncode, 0, staged.stderr)
        for name, source in LICENSE_PATHS.items():
            self.assertEqual((destination / name).read_bytes(), (SCRIPTS.parent / source).read_bytes())
        # A release can have valid outer checksums yet be missing a required
        # license or carrying a different license from its recorded inventory.
        manifest_path = self.release / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        archive_path = self.release / manifest["targets"]["linux/arm64"]["archive"]
        with tarfile.open(archive_path, "r:gz") as archive:
            original = [(member, archive.extractfile(member).read()) for member in archive]
        import io
        for omitted in [False, True]:
            with tarfile.open(archive_path, "w:gz") as archive:
                for member, content in original:
                    if member.name == "sdk-go-LICENSE":
                        if omitted:
                            continue
                        content = b"different license"
                        member.size = len(content)
                    archive.addfile(member, io.BytesIO(content))
            manifest["targets"]["linux/arm64"]["archive_sha256"] = hashlib.sha256(archive_path.read_bytes()).hexdigest()
            manifest_path.write_text(json.dumps(manifest))
            (self.release / "SHA256SUMS").write_text("\n".join(
                hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name
                for path in sorted(self.release.iterdir()) if path.name != "SHA256SUMS") + "\n")
            staged = subprocess.run(command, cwd=self.source, env=self.env, capture_output=True, text=True)
            self.assertNotEqual(staged.returncode, 0)
            self.assertIn("licenses are incomplete" if omitted else "license checksum mismatch", staged.stderr)

    def test_wrong_embedded_compiler_prevents_release_publication(self):
        result = self.build(DJOB_TEST_WRONG_COMPILER="1")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("toolchain", result.stdout)
        self.assertFalse(self.release.exists(), "a failed build must not publish an archive directory")

    def test_standalone_consumer_uses_selected_go_with_local_mode(self):
        spec = importlib.util.spec_from_file_location("go_toolchain", SCRIPTS / "go_toolchain.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from unittest.mock import patch
        with patch.dict(os.environ, self.env, clear=True):
            selected = module.selected_go(self.source)
            env = module.local_go_environment(selected, GOWORK="off")
        self.assertEqual(env["GOTOOLCHAIN"], "local")
        binary = self.base / "consumer"
        subprocess.run(["go", "build", "-o", str(binary), "."], cwd=self.base, env=env, check=True)
        self.assertEqual(binary.read_text(), "go1.27.4")


if __name__ == "__main__":
    unittest.main()
