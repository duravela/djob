"""License and notice files carried by each combined engine binary archive."""

LICENSE_PATHS = {
    "LICENSE": "LICENSE",
    "COMMERCIAL.md": "COMMERCIAL.md",
    "THIRD_PARTY_NOTICES.txt": "THIRD_PARTY_NOTICES.txt",
    "sdk-go-LICENSE": "sdk/go/LICENSE",
    "sdk-go-NOTICE": "sdk/go/NOTICE",
    "BSD-golang.txt": "licenses/BSD-golang.txt",
    "Apache-2.0.txt": "licenses/Apache-2.0.txt",
    "CockroachDB-23.1-BSL.txt": "licenses/CockroachDB-23.1-BSL.txt",
    "CockroachDB-23.1-LICENSE.txt": "licenses/CockroachDB-23.1-LICENSE.txt",
    "MIT-rqlite.txt": "licenses/MIT-rqlite.txt",
    "MIT-fatih-pool.txt": "licenses/MIT-fatih-pool.txt",
}

LICENSE_NAMES = set(LICENSE_PATHS) | {"DEPENDENCY_LICENSES.txt"}
