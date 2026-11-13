# Contributing to djob

djob's engine is source-available under [LICENSE](LICENSE). Its Go SDK is
separately [Apache 2.0](sdk/go/LICENSE). Engine contributions follow Section 7.3
of IBSA 1.1, including its copyright/patent grants; SDK contributions follow
Apache 2.0. Submit only material you have the right to contribute and preserve
third-party notices. New copied or adapted code needs an identifiable source,
version, license and modification record. Removing a copyright header does not
make code original.

Section 7 permits source forks, branches, patches and the repository host's
ordinary source archives for contribution work. Keep compiled engine packages,
images and CI artifacts private to authorized review participants and providers;
public binary distribution requires a separate agreement. Section 7.4 preserves
the separate terms for SDK and other excluded portions of a mixed contribution.

Keep SDK handlers independent of engine/storage imports. Business decisions
belong in pure handlers, side effects in drivers, and authorization/durable
transitions in the engine. Changes to persisted state, wire schemas or retries
must preserve persisted formats, supported protocol versions and uncertainty
handling. Include migration and recovery implications in the proposed change.

Run `./scripts/test-workspace.sh` for all workspace modules with race detection,
`python3 -m unittest discover -s scripts -p '*_test.py'` for release tools, and
`./scripts/check-packaging.sh` for external consumer builds. Browser and Docker
fixtures use `./scripts/test-console.sh`, `./scripts/test-console-shared.sh`
and `./scripts/test-e2e.sh`; CI supplies their browser and Docker dependencies.
Do not include credentials, private configuration, production data or fabricated
qualification results in contributions. Report security issues privately as
described in [SECURITY.md](SECURITY.md).
