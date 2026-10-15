"""Refresh pinned upstream inputs; ordinary test runs never download anything."""

import hashlib
import json
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent
DUCKDB = "e08224d653fcade680e471709e9a9035223989df"
STARROCKS = "27069855f0c1c282732dd20b03f8f8265975cbd1"
SOURCES = [
    ("sqlite", f"select{i}", "duckdb/duckdb", DUCKDB, f"test/sqlite/select{i}.test_slow", "slt")
    for i in range(1, 5)
] + [
    ("duckdb", name, "duckdb/duckdb", DUCKDB, path, "slt")
    for name, path in [
        ("count", "test/sql/aggregate/aggregates/test_count.test"),
        ("sum", "test/sql/aggregate/aggregates/test_sum.test"),
        ("null_aggregates", "test/sql/aggregate/aggregates/test_null_aggregates.test"),
        ("window", "test/sql/window/test_basic_window.test"),
        ("like_null", "test/sql/filter/test_like_null.test"),
        ("not_in", "test/sql/subquery/any_all/test_simple_not_in.test"),
    ]
] + [
    ("starrocks", "date_diff", "StarRocks/starrocks", STARROCKS,
     "test/sql/test_datetime/R/test_date_diff", "starrocks"),
    ("starrocks", "window", "StarRocks/starrocks", STARROCKS,
     "test/sql/test_window_function/R/test_basic_window_function", "starrocks"),
]


def fetch(repo, revision, path, dest):
    url = f"https://raw.githubusercontent.com/{repo}/{revision}/{path}"
    with urlopen(url, timeout=60) as response:
        data = response.read()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def main():
    manifest = []
    for suite, name, repo, revision, source_path, fmt in SOURCES:
        path = f"upstream/{suite}/{name}.test"
        checksum = fetch(repo, revision, source_path, ROOT / path)
        source = dict(id=f"{suite}/{name}", suite=suite, path=path,
                             repository=repo, revision=revision,
                             source_path=source_path, sha256=checksum, format=fmt)
        manifest.append(source)
    fetch("duckdb/duckdb", DUCKDB, "LICENSE", ROOT / "upstream/DUCKDB_LICENSE")
    fetch("StarRocks/starrocks", STARROCKS, "LICENSE.txt", ROOT / "upstream/STARROCKS_LICENSE")
    fetch("StarRocks/starrocks", STARROCKS, "NOTICE.txt", ROOT / "upstream/STARROCKS_NOTICE")
    (ROOT / "sources.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"Vendored {len(manifest)} source files at fixed revisions.")


if __name__ == "__main__":
    main()
