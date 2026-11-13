"""Build real consumers from candidate module archives, outside the workspace."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import zipfile

from go_toolchain import local_go_environment, selected_go


ROOT = Path(__file__).resolve().parent.parent
ENGINE = "github.com/duravela/djob"
SDK = ENGINE + "/sdk/go"


def run(args, cwd, env):
    result = subprocess.run(args, cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(" ".join(args) + "\n" + result.stdout)
    return result.stdout


def required_version(manifest, module):
    match = re.search(r"^\s*" + re.escape(module) + r"\s+(v\S+)",
                      manifest.read_text(), re.MULTILINE)
    if not match:
        raise RuntimeError(f"Missing release version for {module} in {manifest}")
    return match.group(1)


def write_module(proxy, source, module, version, files, module_roots):
    destination = proxy / module / "@v"
    destination.mkdir(parents=True)
    (destination / "list").write_text(version + "\n")
    (destination / (version + ".info")).write_text(json.dumps({
        "Version": version, "Time": "2026-11-08T00:00:00Z"}))
    shutil.copyfile(source / "go.mod", destination / (version + ".mod"))
    nested = [p for p in module_roots if p != source and source in p.parents]
    with zipfile.ZipFile(destination / (version + ".zip"), "w",
                         zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            if source not in path.parents or any(p in path.parents for p in nested):
                continue
            archive.write(path, module + "@" + version + "/" +
                          path.relative_to(source).as_posix())


def check_sdk_consumer(base, version, env):
    consumer = base / "sdk-consumer"
    consumer.mkdir()
    (consumer / "go.mod").write_text(
        "module example.com/sdk-consumer\n\ngo 1.26.0\n\nrequire " +
        SDK + " " + version + "\n")
    (consumer / "main.go").write_text('''package main

import (
    "context"
    "crypto/sha256"
    "github.com/duravela/djob/sdk/go"
    "github.com/duravela/djob/sdk/go/handlers"
)

var _ = handlers.Builtins()

var handler djob.Handler = func(v djob.View) (djob.Decision, error) {
    sum := sha256.Sum256(v.Run.Definition.Input)
    return djob.Decision{Kind: djob.JobComplete, Status: djob.JobSucceeded, Result: sum[:]}, nil
}

type driver struct{}
func (driver) Execute(context.Context, djob.Action) djob.Effect {
    return djob.Effect{Status: djob.EffectFailed, Error: "no external operation"}
}
func (driver) Quiesce(context.Context) error { return nil }
func (driver) Close(context.Context) error { return nil }
var _ djob.DriverFactory = func(djob.JobRun) (djob.Driver, error) { return driver{}, nil }

func main() {
    decision, err := handler(djob.View{Run: djob.JobRun{Definition: djob.JobDefinition{Input: []byte("example")}}})
    if err != nil || decision.Status != djob.JobSucceeded || len(decision.Result) != sha256.Size { panic("invalid SDK result") }
}
''')
    run(["go", "mod", "tidy"], consumer, env)
    sdk_directory = Path(run(["go", "list", "-m", "-f", "{{.Dir}}", SDK], consumer, env).strip())
    for name in ["LICENSE", "NOTICE"]:
        if (sdk_directory / name).read_bytes() != (ROOT / "sdk/go" / name).read_bytes():
            raise RuntimeError("SDK module download lost its separate " + name)
    print("SDK module download retains its own Apache license and notice.", flush=True)
    run(["go", "run", "."], consumer, env)
    packages = set(run(["go", "list", "-deps", "-f",
                        "{{if not .Standard}}{{.ImportPath}}{{end}}", "."],
                       consumer, env).split())
    if packages != {"example.com/sdk-consumer", SDK, SDK + "/handlers"}:
        raise RuntimeError("Job authoring acquired non-standard-library dependencies: " + str(packages))
    print("Job-authoring consumer passed; no compiled engine or third-party packages.", flush=True)

    # Remote execution adds gRPC without pulling consensus/storage into the SDK.
    (consumer / "main.go").write_text('''package main
import (
    "context"
    "github.com/duravela/djob/sdk/go/worker"
)
var _ func(context.Context, worker.Config) error = worker.Run
var _ func(context.Context, worker.Config, worker.ReconnectConfig) error = worker.Serve
func main() { _ = worker.Config{Transport: worker.TransportGRPC} }
''')
    run(["go", "mod", "tidy"], consumer, env)
    run(["go", "run", "."], consumer, env)
    packages = set(run(["go", "list", "-deps", "-f", "{{.ImportPath}}", "."], consumer, env).split())
    if "google.golang.org/grpc" not in packages or any(
            p.startswith(ENGINE + "/") and not p.startswith(SDK + "/") and p != SDK
            for p in packages):
        raise RuntimeError("Remote SDK transport dependency boundary failed")
    print("Remote SDK consumer passed with gRPC and no engine packages.", flush=True)


def check_company_consumer(base, env, name):
    consumer = base / name
    shutil.copytree(ROOT / "examples" / name, consumer)
    run(["go", "mod", "tidy"], consumer, env)
    run(["go", "test", "-race", "-count=1", "./..."], consumer, env)
    run(["go", "build", "-o", str(base / (name + "-binary")),
         "./cmd/" + name], consumer, env)
    if re.search(r"^replace\s", (consumer / "go.mod").read_text(), re.MULTILINE):
        raise RuntimeError("External company consumer must not require local replacements")
    print(name + " consumer built and passed race tests outside the workspace.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--published", action="store_true",
                        help="Verify real published modules using the public proxy and checksum database")
    parser.add_argument("--sdk-only", action="store_true",
                        help="With --published, check the SDK before publishing the engine")
    args = parser.parse_args()
    if args.sdk_only and not args.published:
        parser.error("--sdk-only requires --published")
    go_binary = selected_go(ROOT)
    version = run([go_binary, "env", "GOVERSION"], ROOT, local_go_environment(go_binary)).strip()
    print("Checking standalone consumers with " + version, flush=True)
    engine_version = required_version(ROOT / "examples/company-node/go.mod", ENGINE)
    sdk_version = required_version(ROOT / "go.mod", SDK)
    if sdk_version != required_version(ROOT / "examples/company-node/go.mod", SDK):
        raise RuntimeError("Engine and example reference different SDK releases")
    for name in ["company-node", "company-worker"]:
        if sdk_version != required_version(ROOT / "examples" / name / "go.mod", SDK) or engine_version != required_version(ROOT / "examples" / name / "go.mod", ENGINE):
            raise RuntimeError("Engine and examples reference different candidate releases")
    files = [ROOT / name for name in subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT).decode().split("\0") if name]
    files = sorted({p for p in files if p.is_file() and not p.is_symlink()})
    module_roots = [p.parent for p in files if p.name == "go.mod"]
    cached_downloads = Path(subprocess.check_output(
        ["go", "env", "GOMODCACHE"], cwd=ROOT, text=True).strip()) / "cache/download"
    upstream = os.environ.get("GOPROXY", "https://proxy.golang.org,direct")
    with tempfile.TemporaryDirectory(prefix="djob-packaging-") as temporary:
        base = Path(temporary)
        if args.published:
            env = local_go_environment(go_binary, GOWORK="off", GOFLAGS="",
                       GOMODCACHE=str(base / "modcache"), GOPRIVATE="",
                       GONOPROXY="none", GONOSUMDB="", GOSUMDB="sum.golang.org",
                       GOPROXY="https://proxy.golang.org", GOBIN=str(base / "bin"))
            check_sdk_consumer(base, sdk_version, env)
            if not args.sdk_only:
                for name in ["company-node", "company-worker"]:
                    check_company_consumer(base, env, name)
                for name in ["djob", "djob-worker", "djob-console"]:
                    run(["go", "install", ENGINE + "/cmd/" + name + "@" + engine_version], base, env)
                print("Versioned CLI installation passed from the public proxy with checksum verification.", flush=True)
            print("Published module checks passed using a fresh cache, public proxy and checksum database.", flush=True)
            return
        proxy = base / "proxy"
        write_module(proxy, ROOT, ENGINE, engine_version, files, module_roots)
        write_module(proxy, ROOT / "sdk/go", SDK, sdk_version, files, module_roots)
        # Candidate modules stay in an isolated cache. Third-party dependencies
        # use the existing download cache first; normal GOPROXY remains fallback.
        env = local_go_environment(go_binary, GOWORK="off", GOFLAGS="",
                   GOMODCACHE=str(base / "modcache"), GONOPROXY="none",
                   GONOSUMDB=ENGINE,
                   GOPROXY=proxy.as_uri() + "," + cached_downloads.as_uri() + "," + upstream,
                   GOBIN=str(base / "bin"))
        check_sdk_consumer(base, sdk_version, env)
        for name in ["company-node", "company-worker"]:
            check_company_consumer(base, env, name)
        run(["go", "install", ENGINE + "/cmd/djob@" + engine_version], base, env)
        run(["go", "install", ENGINE + "/cmd/djob-worker@" + engine_version], base, env)
        run(["go", "install", ENGINE + "/cmd/djob-console@" + engine_version], base, env)
        print("Versioned controller, worker and console installation passed from candidate archives.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError) as error:
        raise SystemExit(str(error))
