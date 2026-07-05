#!/usr/bin/env python3
"""Transform a StarRocks temp-table-chain rollforward dump into one big CTE
query runnable on thinDB. Drops DROP/SET/CREATE-temp/INSERT-target scaffolding,
converts each `CREATE TEMPORARY TABLE X AS <body>;` into `X AS (<body>)`,
converts external_plan_ids_temp into a VALUES-style CTE, inlines @vars, and
swaps project 1000051 -> 1000073 with real AirDNA plan ids."""
import re, sys

SRC = sys.argv[1]
OUT = sys.argv[2]
# AirDNA (1000073) real high-volume planIds for the external-plan filter.
PLANS = ["price_1Pv5ANGd5jE924o7k2YHiLZ4", "market-monthly-usd-3995", "market-monthly-usd-1995"]

txt = open(SRC, encoding="utf-8").read()

# --- var substitutions (project swapped to 1000073) ---
VARS = {
    "@targetCurrency": "'USD'", "@childCustomer": "0", "@isCrossDivision": "TRUE",
    "@revenueModel": "'wayroll-net-mrr'", "@currentDate": "'2026-07-04'",
    "@curMonth": "'2026-07-01'", "@isLeapYear": "FALSE", "@projectId": "1000073",
    "@hashStart": "NULL", "@hashEnd": "NULL", "@modelType": "'mrr'",
    "@includeEstimates": "0", "@comparisonMonths": "1",
}
# Replace longest names first so no name is a prefix of another mid-replace.
for name in sorted(VARS, key=len, reverse=True):
    txt = txt.replace(name, VARS[name])

# --- external_plan_ids_temp as a CTE ---
ep_cte = "external_plan_ids_temp AS (\n" + "\n  UNION ALL\n".join(
    f"  SELECT '{p}' AS externalPlanId" for p in PLANS) + "\n)"

# --- collect the CREATE TEMPORARY TABLE ... AS <body>; blocks in order ---
# Each body's only ';' is its terminator, so non-greedy .*?; captures exactly it.
ctas = re.compile(
    r"CREATE TEMPORARY TABLE (\w+)\s*(?:\([^)]*\)\s*)?"
    r"(?:DISTRIBUTED[^\n]*\n)?(?:PROPERTIES\s*\([^)]*\)\s*)?AS\s*(.*?;)",
    re.DOTALL)
members = []
for m in ctas.finditer(txt):
    name, body = m.group(1), m.group(2).rstrip()
    if name == "external_plan_ids_temp":
        continue  # replaced by the VALUES CTE
    body = body[:-1] if body.endswith(";") else body  # drop terminator
    members.append(f"{name} AS (\n{body}\n)")

# --- final INSERT INTO <target> (cols) WITH ... SELECT ... ---
fin = re.search(r"INSERT INTO \w+\s*\([^)]*\)\s*WITH\s+(.*)\Z", txt, re.DOTALL)
final_with_body = fin.group(1).strip()  # "translated_plan_ids AS (...), ... SELECT * FROM ..."

parts = [ep_cte] + members
out = "WITH " + ",\n".join(parts) + ",\n" + final_with_body + "\n"
open(OUT, "w", encoding="utf-8").write(out)
print(f"wrote {OUT}: {len(members)} CTAS members + external CTE + final WITH")
print("members:", ", ".join(m.split(" AS")[0] for m in members))
