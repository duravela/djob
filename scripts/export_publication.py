"""Export reviewed current source into a fresh Git repository without old history.

The private repository and its refs are never changed. No network actions occur.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile


def git(root, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.check_output(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
         "-c", "core.autocrlf=false", "-c", "core.attributesFile=/dev/null", *args],
        cwd=root, env=env, stderr=subprocess.PIPE)


def export(root, destination, exclude_docs=False):
    root, destination = root.resolve(), destination.resolve()
    if destination == root or root in destination.parents:
        raise ValueError("publication output must be outside the private checkout")
    if destination.exists():
        raise ValueError("publication output must be a new directory")
    if git(root, "status", "--porcelain", "--untracked-files=all").strip():
        raise ValueError("publication export requires clean committed source")
    revision = git(root, "rev-parse", "HEAD").decode().strip()
    tree = git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    # Read blobs from the commit, never follow worktree symlinks or copy .git.
    archive = git(root, "archive", "--format=tar", "HEAD")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="djob-publication-", dir=destination.parent) as temporary:
        stage = Path(temporary) / "export"
        source = stage / "source"
        source.mkdir(parents=True)
        inventory = {}
        with tarfile.open(fileobj=io.BytesIO(archive)) as package:
            for entry in package:
                name = PurePosixPath(entry.name)
                if name.is_absolute() or ".." in name.parts or any(p.lower() == ".git" for p in name.parts):
                    raise ValueError("unsafe source archive entry: " + entry.name)
                if entry.isdir():
                    continue
                if exclude_docs and name.parts[0] == "docs":
                    continue
                if not entry.isfile():
                    raise ValueError("symlinks and special source entries require explicit review: " + entry.name)
                data = package.extractfile(entry).read()
                path = source.joinpath(*name.parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                path.chmod(0o755 if entry.mode & 0o111 else 0o644)
                inventory[entry.name] = hashlib.sha256(data).hexdigest()
        git(source, "-c", "init.templateDir=", "init", "--quiet", "--initial-branch=main")
        git(source, "add", "--force", "--all")
        publication_tree = git(source, "write-tree").decode().strip()
        if not exclude_docs and publication_tree != tree:
            raise ValueError("exported Git tree differs from reviewed source (filters or archive attributes)")
        # Compare every included path, mode and blob ID against the private commit.
        # This also rejects export-ignore/subst changes outside the excluded folder.
        expected = {}
        for entry in git(root, "ls-tree", "-r", "-z", "HEAD").split(b"\0"):
            if not entry:
                continue
            metadata, name = entry.split(b"\t", 1)
            if exclude_docs and name.split(b"/", 1)[0] == b"docs":
                continue
            mode, kind, oid = metadata.split()
            expected[name] = (mode, oid)
        actual = {}
        for entry in git(source, "ls-files", "--stage", "-z").split(b"\0"):
            if entry:
                metadata, name = entry.split(b"\t", 1)
                mode, oid, stage_number = metadata.split()
                actual[name] = (mode, oid)
        if actual != expected:
            raise ValueError("exported paths, modes or blobs differ from reviewed source")
        author = git(root, "show", "-s", "--format=%an", "HEAD").decode().strip()
        email = git(root, "show", "-s", "--format=%ae", "HEAD").decode().strip()
        git(source, "-c", "user.name=" + author, "-c", "user.email=" + email,
            "commit", "--quiet", "-m", "Initial Duravela djob preview snapshot")
        fresh_revision = git(source, "rev-parse", "HEAD").decode().strip()
        if git(source, "rev-list", "--all", "--count").strip() != b"1":
            raise ValueError("publication repository unexpectedly contains ancestors")
        refs = git(source, "for-each-ref", "--format=%(refname)").decode().splitlines()
        if refs != ["refs/heads/main"] or git(source, "remote").strip():
            raise ValueError("publication repository unexpectedly contains old refs or remotes")
        manifest = {
            "format_version": 2,
            "private_source_revision": revision,
            "source_tree": tree,
            "publication_tree": publication_tree,
            "excluded_paths": ["docs/"] if exclude_docs else [],
            "publication_revision": fresh_revision,
            "history_policy": "fresh root; original private history, tags and release assets excluded",
            "files_sha256": inventory,
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        stage.rename(destination)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--exclude-docs", action="store_true",
                        help="Keep the entire root docs/ folder and its Git objects private")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    # The project-specific disposition check runs before preparing publishable source.
    from check_provenance import check
    check(root)
    manifest = export(root, args.output, exclude_docs=args.exclude_docs)
    print("Fresh publication repository:", args.output / "source")
    print("Publication revision:", manifest["publication_revision"])
    print("Private history retained; no remote or publication action performed.")


if __name__ == "__main__":
    main()
