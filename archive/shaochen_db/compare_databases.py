"""
compare_databases.py
====================
Compares ISP_HOME.db (original) and ISP_HOME_FAST.db (fast rebuild) to confirm
they contain identical data. Checks row counts, distinct values, and spot-checks
specific aggregates. Prints a pass/fail summary for each check.

Run from the directory containing both database files:
  python compare_databases.py
"""

import sqlite3
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
DB_A = SCRIPT_DIR / "ISP_HOME.db"
DB_B = SCRIPT_DIR / "ISP_HOME_FAST.db"

# ---------------------------------------------------------------------------

def connect(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def q(conn, sql, params=()) -> list:
    return conn.execute(sql, params).fetchall()


def scalar(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def check(label: str, val_a, val_b) -> bool:
    ok = val_a == val_b
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}]  {label}")
    if not ok:
        print(f"           {DB_A.name}: {val_a}")
        print(f"           {DB_B.name}: {val_b}")
    return ok


# ---------------------------------------------------------------------------

def main():
    print(f"\nConnecting to databases...")
    conn_a = connect(DB_A)
    conn_b = connect(DB_B)
    print(f"  A: {DB_A.name}")
    print(f"  B: {DB_B.name}")

    passes = 0
    failures = 0

    def run(label, val_a, val_b):
        nonlocal passes, failures
        if check(label, val_a, val_b):
            passes += 1
        else:
            failures += 1

    # ── 1. Row counts ────────────────────────────────────────────────────────
    print("\n--- Row counts ---")
    for table in ["context", "data", "non_annual_data", "mapping"]:
        a = scalar(conn_a, f"SELECT COUNT(*) FROM {table}")
        b = scalar(conn_b, f"SELECT COUNT(*) FROM {table}")
        run(f"{table} row count", a, b)

    # ── 2. Distinct values in context columns ────────────────────────────────
    print("\n--- Distinct values in context ---")
    for col in ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology"]:
        a = scalar(conn_a, f"SELECT COUNT(DISTINCT {col}) FROM context")
        b = scalar(conn_b, f"SELECT COUNT(DISTINCT {col}) FROM context")
        run(f"context DISTINCT {col}", a, b)

    # ── 3. Distinct variables and years in data ──────────────────────────────
    print("\n--- Distinct values in data ---")
    for col in ["Variable", "Year"]:
        a = scalar(conn_a, f"SELECT COUNT(DISTINCT {col}) FROM data")
        b = scalar(conn_b, f"SELECT COUNT(DISTINCT {col}) FROM data")
        run(f"data DISTINCT {col}", a, b)

    # ── 4. Aggregate value checks ────────────────────────────────────────────
    print("\n--- Aggregate value checks ---")

    agg_sql = """
        SELECT ROUND(SUM(d.Value), 2)
        FROM data d JOIN context c ON d.Id = c.Id
        WHERE c.Data_source = ? AND d.Variable = ?
    """
    for ds, var in [
        ("2022 Draft ISP",  "capacity"),
        ("2022 Final ISP",  "capacity"),
        ("2024 Draft ISP",  "capacity"),
        ("2024 Final ISP",  "capacity"),
        ("2026 Draft ISP",  "capacity"),
        ("2022 Draft ISP",  "generation"),
        ("2024 Final ISP",  "generation"),
        ("2026 Draft ISP",  "generation"),
        ("2024 Final ISP",  "storage capacity"),
        ("2026 Draft ISP",  "storage energy"),
    ]:
        a = scalar(conn_a, agg_sql, (ds, var))
        b = scalar(conn_b, agg_sql, (ds, var))
        run(f"SUM({var}) for {ds}", a, b)

    # ── 5. Row counts per Data_source ────────────────────────────────────────
    print("\n--- Context rows per Data_source ---")
    sql = "SELECT Data_source, COUNT(*) as n FROM context GROUP BY Data_source ORDER BY Data_source"
    rows_a = {r["Data_source"]: r["n"] for r in q(conn_a, sql)}
    rows_b = {r["Data_source"]: r["n"] for r in q(conn_b, sql)}
    all_ds = sorted(set(rows_a) | set(rows_b))
    for ds in all_ds:
        run(f"context rows: {ds}", rows_a.get(ds), rows_b.get(ds))

    # ── 6. Spot-check specific context Ids exist in both ────────────────────
    print("\n--- Spot-check context Ids ---")
    # Check that the first and last Id in A exist in B with same values
    for id_sql in [
        "SELECT MIN(Id) FROM context",
        "SELECT MAX(Id) FROM context",
    ]:
        id_val = scalar(conn_a, id_sql)
        row_a = q(conn_a, "SELECT * FROM context WHERE Id = ?", (id_val,))
        row_b = q(conn_b, "SELECT * FROM context WHERE Id = ?", (id_val,))
        a_vals = dict(row_a[0]) if row_a else None
        b_vals = dict(row_b[0]) if row_b else None
        run(f"context Id={id_val} exists and matches", a_vals, b_vals)

    # ── 7. Technology name check - no unstandardised names in either db ──────
    print("\n--- Technology standardisation check ---")
    # Any technology with mixed case variants would indicate standardisation failed
    sql = """
        SELECT Technology, COUNT(*) as n
        FROM context
        GROUP BY LOWER(Technology)
        HAVING COUNT(DISTINCT Technology) > 1
    """
    variants_a = len(q(conn_a, sql))
    variants_b = len(q(conn_b, sql))
    run("Technology name variants (should both be 0)", variants_a, variants_b)
    if variants_a > 0:
        print(f"    WARNING: {DB_A.name} has unstandardised technology names")
    if variants_b > 0:
        print(f"    WARNING: {DB_B.name} has unstandardised technology names")

    # ── 8. Mapping table check ───────────────────────────────────────────────
    print("\n--- Mapping table ---")
    for attr in ["Scenario_1", "Scenario_2", "State", "Region", "Technology"]:
        sql = "SELECT COUNT(*) FROM mapping WHERE Attribute_type = ?"
        a = scalar(conn_a, sql, (attr,))
        b = scalar(conn_b, sql, (attr,))
        run(f"mapping rows: {attr}", a, b)

    # ── Summary ──────────────────────────────────────────────────────────────
    total = passes + failures
    print(f"\n{'='*50}")
    print(f"  {passes}/{total} checks passed")
    if failures == 0:
        print("  Databases are identical on all checked dimensions.")
    else:
        print(f"  {failures} check(s) FAILED - review output above.")
    print(f"{'='*50}\n")

    conn_a.close()
    conn_b.close()


if __name__ == "__main__":
    main()
