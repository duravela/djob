import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from check_provenance import check


class SourceIntegrityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "licenses").mkdir()
        (self.root / "source.go").write_text("package fixture\n")
        self.record = {
            "files": [
                {"path": "source.go", "action": "retained_with_attribution",
                 "sha256": hashlib.sha256((self.root / "source.go").read_bytes()).hexdigest()},
                {"path": "removed.go", "action": "removed"},
            ],
            "license_files_sha256": {},
        }
        (self.root / "licenses/source-integrity.json").write_text(json.dumps(self.record))

    def test_public_checkout_needs_no_private_docs_and_detects_drift(self):
        check(self.root)
        (self.root / "source.go").write_text("changed source")
        with self.assertRaisesRegex(ValueError, "reviewed source changed"):
            check(self.root)

    def test_removed_source_cannot_return(self):
        (self.root / "removed.go").write_text("restored source")
        with self.assertRaisesRegex(ValueError, "reappeared"):
            check(self.root)

    def test_private_review_must_match_public_baseline(self):
        legal = self.root / "docs/legal"
        legal.mkdir(parents=True)
        (legal / "provenance-observations.json").write_text(json.dumps({"files": [{"path": "source.go"}]}))
        (legal / "provenance-resolution.json").write_text(json.dumps(self.record))
        check(self.root)
        self.record["files"][0]["sha256"] = "0" * 64
        (legal / "provenance-resolution.json").write_text(json.dumps(self.record))
        with self.assertRaisesRegex(ValueError, "differs from private review"):
            check(self.root)
