"""Verify integrity of the recorded source review; this is not a legal opinion."""

import hashlib
import json
from pathlib import Path


def check(root):
    record = json.loads((root / "licenses/source-integrity.json").read_text())
    reviewed = {f["path"]: f for f in record["files"]}
    # The detailed review remains private. When present, require the public
    # integrity baseline to cover exactly the same reviewed paths and hashes.
    private_record = root / "docs/legal/provenance-resolution.json"
    if private_record.exists():
        private = json.loads(private_record.read_text())
        observed = json.loads((root / "docs/legal/provenance-observations.json").read_text())
        missing = {f["path"] for f in observed["files"]} - reviewed.keys()
        if missing:
            raise ValueError("missing source dispositions: " + ", ".join(sorted(missing)))
        baseline = lambda r: [{k: f[k] for k in ("path", "action", "sha256") if k in f}
                              for f in r["files"]]
        if baseline(record) != baseline(private) or record["license_files_sha256"] != private["license_files_sha256"]:
            raise ValueError("public integrity baseline differs from private review")
    for name, entry in reviewed.items():
        path = root / name
        if entry["action"] == "removed":
            if path.exists():
                raise ValueError("removed upstream source reappeared: " + name)
        elif hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ValueError("reviewed source changed; update its disposition: " + name)
    for name, expected in record["license_files_sha256"].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
            raise ValueError("reviewed upstream license changed: " + name)
    print("Source disposition integrity passed:", len(reviewed), "paths.")


if __name__ == "__main__":
    check(Path(__file__).resolve().parent.parent)
