"""Exercise SQLancer replay, reduction, failure reporting and owned-server cleanup."""

import argparse
import json
import pathlib
import re
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=pathlib.Path, required=True)
    parser.add_argument("--server", type=pathlib.Path, required=True)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[3]
    jar = root / "tests/external_sql/sqlancer/target/thindb-sqlancer.jar"
    output = root / f"tests/external_sql/artifacts/sqlancer/validation-{time.time_ns()}"
    output.mkdir(parents=True)
    checks = []

    def run(name, replay=None, expected=0, extra=(), server_path=args.server, kind=None):
        command = [str(args.runner.resolve()), "--server", str(server_path.resolve()),
                   "--jar", str(jar), "--java", args.java, "--seeds", "1", "--output", str(output / name)]
        if "--seed-timeout" not in extra:
            command += ["--seed-timeout", "20"]
        if replay is not None:
            if isinstance(replay, dict):
                path = output / (name + ".json")
                path.write_text(json.dumps(replay), encoding="utf-8")
            else:
                path = replay
            command += ["--replay", str(path)]
        result = subprocess.run(command + list(extra), text=True, capture_output=True)
        if result.returncode != expected:
            raise AssertionError((name, result.stdout, result.stderr))
        directory = pathlib.Path(re.search(r"SQLancer diagnostics: (.*)", result.stdout)[1].strip())
        summary = json.loads((directory / "summary.json").read_text())
        seed = directory / "seed-0"
        log = (seed / "server.log").read_text()
        data = re.search(r"data-dir: (.*)", log)
        if data and pathlib.Path(data[1].strip()).exists():
            raise AssertionError(("temporary data not cleaned", data[1]))
        if "127.0.0.1:13310" in log:
            raise AssertionError("production port used")
        if kind:
            failure = json.loads((seed / "failure.json").read_text())
            if f"Mismatch: TLP {kind} mismatch" not in failure["error"]:
                raise AssertionError((name, failure["error"]))
        checks.append({"name": name, "exit_code": result.returncode, "verified": True,
                       "report": directory.relative_to(root).as_posix()})
        (output / "validation.json").write_text(json.dumps(checks, indent=2) + "\n")
        print(name, "verified", flush=True)
        return seed, summary

    setup = ["CREATE TABLE t0 (c0 BIGINT, c1 VARCHAR(64))",
             "INSERT INTO t0 VALUES (9007199254740993, '  x  ')",
             "INSERT INTO t0 VALUES (9007199254740993, '  x  ')",
             "INSERT INTO t0 VALUES (NULL, '')", "INSERT INTO t0 VALUES (-1, 'NULL')"]
    base = "SELECT c0, c1 FROM t0"
    queries = [base, base + " WHERE c0 > 0", base + " WHERE NOT (c0 > 0)", base + " WHERE (c0 > 0) IS NULL"]
    replay = {"format": 1, "seed": 123, "check": 0, "setup": setup, "queries": queries}
    run("values-session-and-duplicates", replay)
    empty = dict(replay, setup=setup[:1])
    run("empty-results", empty)
    wrong = dict(replay, queries=[queries[0], "SELECT 9007199254740992, c1 FROM t0 WHERE c0 > 0", *queries[2:]])
    run("bigint-rounding-failure", wrong, 1, kind="rows")
    missing = dict(replay, queries=[queries[0], base + " WHERE 1 = 0", *queries[2:]])
    seed, _ = run("missing-rows-reduced", missing, 1, kind="rows")
    reduced = json.loads((seed / "reduced.json").read_text())
    if len(reduced["setup"]) != 2 or reduced["queries"] != missing["queries"]:
        raise AssertionError(("reducer must retain one row and the original queries", reduced))
    run("replay-reduced-failure", seed / "reduced.json", 1, kind="rows")
    shape = dict(empty, queries=[queries[0], "SELECT c0 FROM t0 WHERE 1 = 0", *queries[2:]])
    run("zero-row-shape-failure", shape, 1, kind="shape")
    error = dict(replay, queries=["SELECT missing_column FROM t0", *queries[1:]])
    seed, summary = run("unexpected-sql-error", error, 1)
    sql_error = summary["seeds"][0]["provider"]["error"]
    if (seed / "reduced.json").exists() or not any(kind in sql_error for kind in ("SQLException", "SQLSyntaxErrorException")):
        raise AssertionError("SQL errors must fail without reduction")
    failed_setup = dict(replay, setup=[setup[0], "INSERT INTO nonexistent_table VALUES (1)", setup[1]])
    seed, summary = run("failed-setup-stops", failed_setup, 1)
    if summary["seeds"][0]["provider"]["attempted"] != 0 or "9007199254740993" in (seed / "statements.sql").read_text():
        raise AssertionError("failed setup did not stop dependent SQL")
    _, summary = run("deadline-cleanup", expected=1, extra=["--queries", "100000", "--seed-timeout", "1"])
    if "seed deadline exceeded" not in summary["seeds"][0]["error"]:
        raise AssertionError(summary)
    _, summary = run("child-startup-failure", expected=1, server_path=args.runner)
    if "server exited during startup" not in summary["seeds"][0]["error"]:
        raise AssertionError(summary)
    timeout = dict(replay, setup=[], queries=["SELECT SLEEP(5)", "SELECT 1", "SELECT 1", "SELECT 1"])
    _, summary = run("jdbc-query-timeout", timeout, 1, extra=["--query-timeout", "1", "--reduce-attempts", "0"])
    failure = summary["seeds"][0]["provider"]["error"].lower()
    if "timeout" not in failure and "timed out" not in failure:
        raise AssertionError(failure)
    print(f"All {len(checks)} live harness checks passed. Diagnostics: {output}")


if __name__ == "__main__":
    main()
