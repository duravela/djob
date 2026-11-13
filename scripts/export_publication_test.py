import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from export_publication import export, git


class PublicationExportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="djob-publication-test-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "private source"
        self.root.mkdir()
        git(self.root, "-c", "init.templateDir=", "init", "--quiet")
        git(self.root, "config", "user.name", "Fixture")
        git(self.root, "config", "user.email", "fixture@example.com")
        (self.root / "old-source").write_text("historical source excluded from publication")
        git(self.root, "add", "--all")
        git(self.root, "commit", "--quiet", "-m", "private historical source")
        self.old = git(self.root, "rev-parse", "HEAD").decode().strip()
        self.old_blob = git(self.root, "rev-parse", "HEAD:old-source").decode().strip()
        git(self.root, "tag", "private-tag")
        (self.root / "old-source").unlink()
        (self.root / "current.go").write_text("package current\n")
        (self.root / "run.sh").write_text("#!/bin/sh\nexit 0\n")
        (self.root / "run.sh").chmod(0o755)
        git(self.root, "add", "--all")
        git(self.root, "commit", "--quiet", "-m", "reviewed current source")

    def test_fresh_root_preserves_tree_and_excludes_historical_objects(self):
        before = git(self.root, "show-ref")
        output = self.base / "publication"
        manifest = export(self.root, output)
        fresh = output / "source"
        self.assertEqual(git(self.root, "show-ref"), before)
        self.assertEqual(git(fresh, "rev-list", "--all", "--count").strip(), b"1")
        self.assertEqual(git(fresh, "rev-parse", "HEAD^{tree}"), git(self.root, "rev-parse", "HEAD^{tree}"))
        self.assertEqual(git(fresh, "status", "--porcelain"), b"")
        for historical_object in [self.old, self.old_blob]:
            with self.assertRaises(subprocess.CalledProcessError):
                git(fresh, "cat-file", "-e", historical_object)
        self.assertEqual(git(fresh, "tag"), b"")
        self.assertFalse((fresh / "old-source").exists())
        self.assertEqual(json.loads((output / "manifest.json").read_text()), manifest)

    def test_rejects_dirty_source_and_existing_or_nested_output(self):
        output = self.base / "publication"
        (self.root / "untracked").write_text("not reviewed")
        with self.assertRaisesRegex(ValueError, "clean committed"):
            export(self.root, output)
        self.assertFalse(output.exists())
        (self.root / "untracked").unlink()
        with self.assertRaisesRegex(ValueError, "outside"):
            export(self.root, self.root / "publication")
        output.mkdir()
        with self.assertRaisesRegex(ValueError, "new directory"):
            export(self.root, output)

    def test_excludes_docs_paths_and_objects_but_preserves_source(self):
        docs = self.root / "docs" / "nested"
        docs.mkdir(parents=True)
        (docs / "private.md").write_text("private documentation sentinel")
        git(self.root, "add", "--all")
        git(self.root, "commit", "--quiet", "-m", "private docs")
        private_blob = git(self.root, "rev-parse", "HEAD:docs/nested/private.md").decode().strip()
        before = git(self.root, "show-ref")
        output = self.base / "publication"
        manifest = export(self.root, output, exclude_docs=True)
        fresh = output / "source"
        self.assertFalse((fresh / "docs").exists())
        self.assertEqual(git(self.root, "show-ref"), before)
        self.assertEqual(manifest["excluded_paths"], ["docs/"])
        self.assertNotIn("docs/nested/private.md", manifest["files_sha256"])
        self.assertNotEqual(manifest["source_tree"], manifest["publication_tree"])
        self.assertEqual((fresh / "current.go").read_bytes(), (self.root / "current.go").read_bytes())
        self.assertEqual((fresh / "run.sh").stat().st_mode & 0o777, 0o755)
        with self.assertRaises(subprocess.CalledProcessError):
            git(fresh, "cat-file", "-e", private_blob)
        self.assertEqual(git(fresh, "rev-list", "--all", "--count").strip(), b"1")

    def test_rejects_source_symlink_without_following_it(self):
        (self.root / "escape").symlink_to("/etc/passwd")
        git(self.root, "add", "escape")
        git(self.root, "commit", "--quiet", "-m", "unreviewed link")
        output = self.base / "publication"
        with self.assertRaisesRegex(ValueError, "symlinks"):
            export(self.root, output)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
