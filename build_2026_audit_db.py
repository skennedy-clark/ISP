from __future__ import annotations

import sqlite3
from pathlib import Path
import pandas as pd
import re


# ------------------------------------------------------------
# Paths (project root expected as cwd)
# ------------------------------------------------------------
HIDDEN_2026_DIR = Path(r"archive/shaochen_db/hidden_data/2026 Draft")
INPUT_CSV_DIR = Path(r"archive/shaochen_db/input_csv")
OUT_DB = Path("audit_2026.db")

SHEETS = {
    "capacity": "Capacity",
    "generation": "Generation",
}

# hidden_data workbooks have headers on row 1 (because you rewrote them)
XLSX_SKIPROWS = 0


def safe_name(s: str) -> str:
    return re.sub(r"[<>:\"\\|?*]+", "_", s).strip()


def workbook_to_scenario_1(filename: str) -> str:
    # "2026 ISP - <Scenario>.xlsx"
    if filename.lower().endswith(".xlsx") and filename.startswith("2026 ISP - "):
        return filename[len("2026 ISP - "):-5]
    return filename[:-5] if filename.lower().endswith(".xlsx") else filename


def read_req_matrix(csv_path: Path, base_table: str, conn: sqlite3.Connection) -> None:
    """
    Reads a req matrix with:
      - index = Technology
      - columns = States
      - values = Y / blank
    Writes:
      - base_table (wide)
      - base_table_long (Technology, State, Flag)
    """
    df = pd.read_csv(csv_path, index_col=0)
    df.index.name = "Technology"
    df.reset_index(inplace=True)
    df.to_sql(base_table, conn, if_exists="replace", index=False)

    long = df.melt(id_vars=["Technology"], var_name="State", value_name="Flag")
    long["Flag"] = long["Flag"].astype(object)
    long.loc[long["Flag"].isna(), "Flag"] = None
    long.loc[long["Flag"].astype(str).str.strip().eq(""), "Flag"] = None
    long.to_sql(f"{base_table}_long", conn, if_exists="replace", index=False)


def read_sheet_axes(xlsx: Path, sheet: str) -> pd.DataFrame:
    """
    Read minimal axis columns only: (Scenario_2, State, Technology)
    using your existing normalisations:
      - Region -> State
      - CDP -> Scenario_2
      - Subregion -> Region (ignored for audit)
    """
    df = pd.read_excel(xlsx, sheet_name=sheet, skiprows=XLSX_SKIPROWS)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Normalise column names to match your loader behaviour
    if "Region" in df.columns and "State" not in df.columns:
        df = df.rename(columns={"Region": "State"})
    if "CDP" in df.columns and "Scenario_2" not in df.columns:
        df = df.rename(columns={"CDP": "Scenario_2"})

    # Some sheets might have Subregion, but we don't need it for this audit
    required = ["Scenario_2", "State", "Technology"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"{xlsx.name} sheet '{sheet}' missing {missing}. Columns seen: {list(df.columns)}"
        )

    out = df[required].copy()
    # normalise whitespace + nulls
    for c in required:
        out[c] = out[c].astype(str).str.strip()
        out.loc[out[c].eq("") | out[c].eq("nan"), c] = None

    return out


def create_views(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # workbook tech lists
    cur.executescript("""
    DROP VIEW IF EXISTS v_wb_capacity_tech;
    CREATE VIEW v_wb_capacity_tech AS
      SELECT DISTINCT technology
      FROM wb_capacity_axes
      WHERE technology IS NOT NULL;

    DROP VIEW IF EXISTS v_wb_generation_tech;
    CREATE VIEW v_wb_generation_tech AS
      SELECT DISTINCT technology
      FROM wb_generation_axes
      WHERE technology IS NOT NULL;

    -- req tech lists (Flag='Y' anywhere)
    DROP VIEW IF EXISTS v_req_capacity_tech;
    CREATE VIEW v_req_capacity_tech AS
      SELECT DISTINCT Technology AS technology
      FROM req_state_capacity_long
      WHERE Flag = 'Y';

    DROP VIEW IF EXISTS v_req_generation_tech;
    CREATE VIEW v_req_generation_tech AS
      SELECT DISTINCT Technology AS technology
      FROM req_state_generation_long
      WHERE Flag = 'Y';

    -- tech in workbook but NOT in req
    DROP VIEW IF EXISTS v_capacity_tech_in_wb_not_req;
    CREATE VIEW v_capacity_tech_in_wb_not_req AS
      SELECT technology
      FROM v_wb_capacity_tech
      WHERE technology NOT IN (SELECT technology FROM v_req_capacity_tech)
      ORDER BY technology;

    DROP VIEW IF EXISTS v_generation_tech_in_wb_not_req;
    CREATE VIEW v_generation_tech_in_wb_not_req AS
      SELECT technology
      FROM v_wb_generation_tech
      WHERE technology NOT IN (SELECT technology FROM v_req_generation_tech)
      ORDER BY technology;

    -- tech in req but NOT in workbook
    DROP VIEW IF EXISTS v_capacity_tech_in_req_not_wb;
    CREATE VIEW v_capacity_tech_in_req_not_wb AS
      SELECT technology
      FROM v_req_capacity_tech
      WHERE technology NOT IN (SELECT technology FROM v_wb_capacity_tech)
      ORDER BY technology;

    DROP VIEW IF EXISTS v_generation_tech_in_req_not_wb;
    CREATE VIEW v_generation_tech_in_req_not_wb AS
      SELECT technology
      FROM v_req_generation_tech
      WHERE technology NOT IN (SELECT technology FROM v_wb_generation_tech)
      ORDER BY technology;

    -- req state-tech pairs missing from workbook
    DROP VIEW IF EXISTS v_capacity_pairs_reqY_missing_in_wb;
    CREATE VIEW v_capacity_pairs_reqY_missing_in_wb AS
      SELECT r.Technology AS technology, r.State AS state
      FROM req_state_capacity_long r
      WHERE r.Flag='Y'
        AND NOT EXISTS (
          SELECT 1 FROM wb_capacity_axes w
          WHERE w.technology = r.Technology AND w.state = r.State
        )
      ORDER BY technology, state;

    DROP VIEW IF EXISTS v_generation_pairs_reqY_missing_in_wb;
    CREATE VIEW v_generation_pairs_reqY_missing_in_wb AS
      SELECT r.Technology AS technology, r.State AS state
      FROM req_state_generation_long r
      WHERE r.Flag='Y'
        AND NOT EXISTS (
          SELECT 1 FROM wb_generation_axes w
          WHERE w.technology = r.Technology AND w.state = r.State
        )
      ORDER BY technology, state;

    -- CDP coverage (Scenario_2 values) by scenario/file
    DROP VIEW IF EXISTS v_wb_capacity_cdp_by_scenario;
    CREATE VIEW v_wb_capacity_cdp_by_scenario AS
      SELECT scenario_1, scenario_2, COUNT(*) AS n_rows
      FROM wb_capacity_axes
      GROUP BY scenario_1, scenario_2
      ORDER BY scenario_1, scenario_2;

    DROP VIEW IF EXISTS v_wb_generation_cdp_by_scenario;
    CREATE VIEW v_wb_generation_cdp_by_scenario AS
      SELECT scenario_1, scenario_2, COUNT(*) AS n_rows
      FROM wb_generation_axes
      GROUP BY scenario_1, scenario_2
      ORDER BY scenario_1, scenario_2;
    """)

    conn.commit()


def main() -> None:
    root = Path.cwd()
    hidden_dir = root / HIDDEN_2026_DIR
    input_dir = root / INPUT_CSV_DIR
    out_db = root / OUT_DB

    if not hidden_dir.is_dir():
        raise FileNotFoundError(f"Not found: {hidden_dir}")
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Not found: {input_dir}")

    if out_db.exists():
        out_db.unlink()

    conn = sqlite3.connect(out_db)
    cur = conn.cursor()

    # axis tables (tiny)
    cur.execute("""
      CREATE TABLE wb_capacity_axes (
        file TEXT,
        scenario_1 TEXT,
        scenario_2 TEXT,
        state TEXT,
        technology TEXT
      )
    """)
    cur.execute("""
      CREATE TABLE wb_generation_axes (
        file TEXT,
        scenario_1 TEXT,
        scenario_2 TEXT,
        state TEXT,
        technology TEXT
      )
    """)
    conn.commit()

    # req matrices (wide + long)
    read_req_matrix(input_dir / "data_req_state_capacity.csv", "req_state_capacity", conn)
    read_req_matrix(input_dir / "data_req_state_generation.csv", "req_state_generation", conn)

    # keep other CSVs in the db too (so you can query them later)
    for extra in ["data_req_rez.csv", "data_req_state_storage.csv", "name_map_technology.csv"]:
        p = input_dir / extra
        if p.exists():
            pd.read_csv(p).to_sql(extra.replace(".csv", ""), conn, if_exists="replace", index=False)

    # load axes from each workbook, capacity + generation
    files = sorted([p for p in hidden_dir.glob("*.xlsx") if not p.name.startswith("~$")])
    if not files:
        raise RuntimeError(f"No .xlsx files in {hidden_dir}")

    cap_rows = []
    gen_rows = []

    for xlsx in files:
        scen1 = workbook_to_scenario_1(xlsx.name)
        print(f"Reading axes: {xlsx.name}")

        cap = read_sheet_axes(xlsx, SHEETS["capacity"])
        cap["file"] = xlsx.name
        cap["scenario_1"] = scen1
        cap_rows.append(cap.rename(columns={"Scenario_2": "scenario_2", "State": "state", "Technology": "technology"}))

        gen = read_sheet_axes(xlsx, SHEETS["generation"])
        gen["file"] = xlsx.name
        gen["scenario_1"] = scen1
        gen_rows.append(gen.rename(columns={"Scenario_2": "scenario_2", "State": "state", "Technology": "technology"}))

    cap_df = pd.concat(cap_rows, ignore_index=True)[["file", "scenario_1", "scenario_2", "state", "technology"]]
    gen_df = pd.concat(gen_rows, ignore_index=True)[["file", "scenario_1", "scenario_2", "state", "technology"]]

    cap_df.to_sql("wb_capacity_axes", conn, if_exists="append", index=False)
    gen_df.to_sql("wb_generation_axes", conn, if_exists="append", index=False)

    # indexes (make NOT EXISTS / NOT IN fast)
    cur.execute("CREATE INDEX idx_cap_tech_state ON wb_capacity_axes(technology, state)")
    cur.execute("CREATE INDEX idx_gen_tech_state ON wb_generation_axes(technology, state)")
    cur.execute("CREATE INDEX idx_cap_scen ON wb_capacity_axes(scenario_1, scenario_2)")
    cur.execute("CREATE INDEX idx_gen_scen ON wb_generation_axes(scenario_1, scenario_2)")
    conn.commit()

    create_views(conn)
    conn.close()

    print(f"\nDone. Created: {out_db}")


if __name__ == "__main__":
    main()
