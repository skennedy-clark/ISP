from __future__ import annotations

import logging
import re
import sqlite3
import time
import traceback
from pathlib import Path
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
HIDDEN_ROOT = BASE_DIR / "hidden_data"
DB_PATH = BASE_DIR / "ISP.db"


# -----------------------------
# Logging: NO log file unless error
# -----------------------------
def setup_console_logger() -> logging.Logger:
    logger = logging.getLogger("create_db")
    logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(sh)
    return logger


def add_file_logger_on_error(logger: logging.Logger, log_path: Path) -> None:
    fh = logging.FileHandler(str(log_path), mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(fh)


# -----------------------------
# Year handling
# -----------------------------
_FY_RE = re.compile(r"^\s*(\d{4})\s*[-/]\s*(\d{2})\s*$")
_YEAR_RE = re.compile(r"^\s*(\d{4})\s*$")


def normalise_year_label(y) -> Optional[str]:
    if pd.isna(y):
        return None
    s = str(y).strip()

    if s.lower() in {"existing and committed", "existing & committed"}:
        return "Existing and Committed"
    if s.lower() == "un33":
        return None

    m = _FY_RE.match(s)
    if m:
        return str(int(m.group(1)) + 1)

    m = _YEAR_RE.match(s)
    if m:
        return str(int(m.group(1)))

    return None


def is_annual_year(s: str) -> bool:
    return bool(_YEAR_RE.match(str(s).strip()))


# -----------------------------
# DB schema + helpers
# -----------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS mapping (
    Technology TEXT PRIMARY KEY,
    Plot_Group TEXT
);

CREATE TABLE IF NOT EXISTS context (
    Id INTEGER PRIMARY KEY,
    Data_source TEXT,
    Scenario_1 TEXT,
    Scenario_2 TEXT,
    State TEXT,
    Region TEXT,
    Technology TEXT,
    UNIQUE (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
);

CREATE TABLE IF NOT EXISTS data (
    Id INTEGER,
    Variable TEXT,
    Year TEXT,
    Value REAL,
    UNIQUE (Id, Variable, Year)
);

CREATE TABLE IF NOT EXISTS non_annual_data (
    Id INTEGER,
    Variable TEXT,
    Year TEXT,
    Value REAL,
    UNIQUE (Id, Variable, Year)
);
"""


def set_ingest_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    conn.commit()


def drop_tables_except_mapping(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cur.fetchall()]
    for t in tables:
        if t.lower() == "mapping":
            continue
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"Dropped table: {t}")
    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(SCHEMA_SQL)
    print("Ensured table exists: mapping")
    print("Created table: context")
    print("Created table: data")
    print("Created table: non_annual_data")
    conn.commit()


def df_to_records(df: pd.DataFrame) -> List[Tuple]:
    """
    Convert DataFrame to list-of-tuples for sqlite executemany,
    ensuring pd.NA (NAType) becomes real Python None.
    """
    obj = df.astype(object)
    obj = obj.where(pd.notna(obj), None)
    return [tuple(x) for x in obj.to_numpy()]


def log_incoming_duplicates(logger: logging.Logger, df: pd.DataFrame, label: str) -> int:
    key_cols = ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology", "Variable", "Year"]
    dup_mask = df.duplicated(subset=key_cols, keep=False)
    n = int(dup_mask.sum())
    if n:
        logger.warning("[%s] Incoming duplicate ROWS=%d (by %s)", label, n, ",".join(key_cols))
    return n


def _normalise_ctx_key_types(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
        out[c] = out[c].astype("string")
        out[c] = out[c].str.strip()
        out.loc[out[c] == "", c] = pd.NA
    return out


def insert_long_form(conn: sqlite3.Connection, df_long: pd.DataFrame, logger: logging.Logger, tag: str) -> None:
    log_incoming_duplicates(logger, df_long, tag)

    cur = conn.cursor()

    ctx_cols = ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology"]

    df_long = _normalise_ctx_key_types(df_long, ctx_cols)

    ctx = df_long[ctx_cols].drop_duplicates().reset_index(drop=True)

    cur.executemany(
        """
        INSERT OR IGNORE INTO context (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        df_to_records(ctx),
    )

    ctx_db = pd.read_sql_query(
        "SELECT Id, Data_source, Scenario_1, Scenario_2, State, Region, Technology FROM context",
        conn,
    )
    ctx_db = _normalise_ctx_key_types(ctx_db, ctx_cols)

    merged = df_long.merge(ctx_db, on=ctx_cols, how="left")

    if merged["Id"].isna().any():
        miss = merged[merged["Id"].isna()][ctx_cols].drop_duplicates()
        raise RuntimeError(f"Context Id lookup failed for some rows:\n{miss.head(20)}")

    d = merged[["Id", "Variable", "Year", "Value"]].copy()
    d["Value"] = pd.to_numeric(d["Value"], errors="coerce")

    annual = d[d["Year"].apply(is_annual_year)].copy()
    nonannual = d[~d["Year"].apply(is_annual_year)].copy()

    def count_rows(table: str) -> int:
        return int(cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    if len(annual):
        before = count_rows("data")
        cur.executemany(
            "INSERT OR IGNORE INTO data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            df_to_records(annual),
        )
        after = count_rows("data")
        inserted = after - before
        ignored = len(annual) - inserted
        if ignored:
            logger.warning("[%s] data: ignored duplicates=%d (attempted=%d inserted=%d)", tag, ignored, len(annual), inserted)

    if len(nonannual):
        before = count_rows("non_annual_data")
        cur.executemany(
            "INSERT OR IGNORE INTO non_annual_data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            df_to_records(nonannual),
        )
        after = count_rows("non_annual_data")
        inserted = after - before
        ignored = len(nonannual) - inserted
        if ignored:
            logger.warning("[%s] non_annual_data: ignored duplicates=%d (attempted=%d inserted=%d)", tag, ignored, len(nonannual), inserted)


# -----------------------------
# Workbook discovery + naming
# -----------------------------
def iter_hidden_workbooks(root: Path) -> List[Path]:
    return sorted([p for p in root.rglob("*.xlsx") if p.is_file()])


def infer_data_source(xlsx: Path) -> str:
    parent = xlsx.parent.name
    parts = parent.split()
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].lower() in {"draft", "final"}:
        return f"{parts[0]} {parts[1].title()} ISP"
    return parent


_SCENARIO_PREFIX_RE = re.compile(
    r"""^
    \d{4}\s+
    (?:
        (?:Draft|Final)\s+ISP\s+results\s+workbook\s+-\s+
      | ISP\s+-\s+
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def infer_scenario_1(xlsx: Path) -> str:
    s = xlsx.stem.strip()
    return _SCENARIO_PREFIX_RE.sub("", s).strip()


# -----------------------------
# Header detection + column aliasing
# -----------------------------
def find_header_row(df0: pd.DataFrame, max_scan: int = 60) -> int:
    tech_keys = {"technology", "tech", "fuel", "fuel type", "resource", "generator type"}
    region_keys = {"region", "state", "nem region", "subregion"}
    scen_keys = {"cdp", "scenario_2", "scenario 2", "scenario", "case"}

    n = min(max_scan, len(df0))
    best_i = None
    best_score = -1

    for i in range(n):
        row = df0.iloc[i].tolist()
        tokens = {str(x).strip().lower() for x in row if pd.notna(x)}
        score = 0
        if tokens & tech_keys:
            score += 3
        if tokens & region_keys:
            score += 2
        if tokens & scen_keys:
            score += 1
        if score > best_score:
            best_score = score
            best_i = i

    if best_i is None or best_score < 5:
        raise ValueError(
            f"Could not confidently detect header row (best_score={best_score}). "
            "Sheet structure differs from expected."
        )
    return int(best_i)


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    lower = {str(c).strip().lower(): c for c in df.columns}

    def rename_if_present(old: str, new: str) -> None:
        k = old.strip().lower()
        if k in lower:
            df.rename(columns={lower[k]: new}, inplace=True)

    rename_if_present("CDP", "Scenario_2")
    rename_if_present("Scenario 2", "Scenario_2")
    rename_if_present("Case", "Scenario_2")
    rename_if_present("Scenario_2", "Scenario_2")

    rename_if_present("Technology", "Technology")
    rename_if_present("Tech", "Technology")
    rename_if_present("Fuel", "Technology")
    rename_if_present("Fuel Type", "Technology")
    rename_if_present("Resource", "Technology")
    rename_if_present("Generator Type", "Technology")

    lower = {str(c).strip().lower(): c for c in df.columns}
    if "state" not in lower and "region" in lower:
        df.rename(columns={lower["region"]: "State"}, inplace=True)
        lower = {str(c).strip().lower(): c for c in df.columns}
    else:
        rename_if_present("State", "State")
        rename_if_present("NEM Region", "State")

    rename_if_present("Subregion", "Region")
    rename_if_present("Region", "Region")

    return df


def read_summary_sheet(
    xl: pd.ExcelFile,
    sheet_name: str,
    variable: str,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    df0 = pd.read_excel(xl, sheet_name=sheet_name, header=None, dtype=object).dropna(how="all")
    header_i = find_header_row(df0, max_scan=60)

    raw = pd.read_excel(xl, sheet_name=sheet_name, header=header_i, dtype=object).dropna(how="all")
    raw.columns = [str(c).strip() for c in raw.columns]
    raw = standardise_columns(raw)

    raw.insert(0, "Data_source", data_source)
    raw.insert(1, "Scenario_1", scenario_1)

    if "Scenario_2" not in raw.columns:
        raw["Scenario_2"] = None
    if "Region" not in raw.columns:
        raw["Region"] = None

    required = ["Technology", "State"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise KeyError(
            f"{sheet_name}: missing required columns {missing}. "
            f"Detected columns={list(raw.columns)} (header_row={header_i})"
        )

    id_vars = ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology"]
    value_vars = [c for c in raw.columns if c not in id_vars]

    melted = pd.melt(raw, id_vars=id_vars, value_vars=value_vars, var_name="Year", value_name="Value")
    melted.insert(melted.shape[1] - 1, "Variable", variable)

    melted["Year"] = melted["Year"].apply(normalise_year_label)
    melted = melted.dropna(subset=["Year"])
    melted["Year"] = melted["Year"].astype(str)

    return melted[
        ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology", "Year", "Variable", "Value"]
    ].copy()


# -----------------------------
# Requirements matrix -> filter + fill missing (vectorised)
# preserve Scenario_2 by doing the fill per Scenario_2 group
# -----------------------------
def load_keep_combos(req_csv: Path) -> pd.DataFrame:
    req = pd.read_csv(req_csv, index_col=0)
    mask = (req == "Y")
    keep_list = mask.stack()
    keep_list = keep_list[keep_list].index.tolist()
    keep_df = pd.DataFrame(keep_list, columns=["Technology", "State"])
    keep_df["tech_lower"] = keep_df["Technology"].astype(str).str.lower()
    keep_df["state_lower"] = keep_df["State"].astype(str).str.lower()
    return keep_df


def expand_keep_over_years(keep_df: pd.DataFrame, years: List[str]) -> pd.DataFrame:
    ydf = pd.DataFrame({"Year": [str(y) for y in years]})
    keep = keep_df[["Technology", "State", "tech_lower", "state_lower"]].copy()
    keep["__k"] = 1
    ydf["__k"] = 1
    return keep.merge(ydf, on="__k", how="inner").drop(columns="__k")


def apply_requirements_fill_missing(df_long: pd.DataFrame, keep_df: pd.DataFrame) -> pd.DataFrame:
    df = df_long.copy()
    df["tech_lower"] = df["Technology"].astype(str).str.lower()
    df["state_lower"] = df["State"].astype(str).str.lower()

    group_cols = ["Data_source", "Scenario_1", "Scenario_2", "Region", "Variable"]
    out_frames: List[pd.DataFrame] = []

    for (data_source, scenario_1, scenario_2, region, variable), g in df.groupby(group_cols, dropna=False):
        years = sorted(g["Year"].astype(str).unique().tolist())
        grid = expand_keep_over_years(keep_df, years)

        merged = grid.merge(
            g,
            on=["tech_lower", "state_lower", "Year"],
            how="left",
            suffixes=("", "_raw"),
        )

        merged["Data_source"] = data_source
        merged["Scenario_1"] = scenario_1
        merged["Scenario_2"] = scenario_2
        merged["Region"] = region
        merged["Variable"] = variable

        out = merged[
            ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology", "Year", "Variable", "Value"]
        ].copy()
        out_frames.append(out)

    return pd.concat(out_frames, ignore_index=True) if out_frames else df_long.copy()


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    print(BASE_DIR)
    logger = setup_console_logger()

    if not HIDDEN_ROOT.exists():
        raise FileNotFoundError(f"hidden_data folder not found: {HIDDEN_ROOT}")

    t0 = time.perf_counter()

    conn = sqlite3.connect(str(DB_PATH))
    try:
        set_ingest_pragmas(conn)

        drop_tables_except_mapping(conn)
        create_tables(conn)

        keep_capacity = load_keep_combos(BASE_DIR / "input_csv" / "data_req_state_capacity.csv")
        keep_generation = load_keep_combos(BASE_DIR / "input_csv" / "data_req_state_generation.csv")

        workbooks = iter_hidden_workbooks(HIDDEN_ROOT)

        for i, xlsx in enumerate(workbooks, start=1):
            start = time.perf_counter()
            data_source = infer_data_source(xlsx)
            scenario_1 = infer_scenario_1(xlsx)
            tag = f"{data_source} - {scenario_1}"

            print("===================================")
            print(f"Processing File {i}: {tag}")
            print("-----------------------------------")

            xl = pd.ExcelFile(xlsx)

            frames: List[pd.DataFrame] = []

            if "Capacity" in xl.sheet_names:
                print("    Processing Capacity Summary Data")
                cap = read_summary_sheet(xl, "Capacity", "capacity", data_source, scenario_1)
                cap = apply_requirements_fill_missing(cap, keep_capacity)
                frames.append(cap)

            if "Generation" in xl.sheet_names:
                print("    Processing Generation Summary Data")
                gen = read_summary_sheet(xl, "Generation", "generation", data_source, scenario_1)
                gen = apply_requirements_fill_missing(gen, keep_generation)
                frames.append(gen)

            if not frames:
                print("    Skipped (no Capacity/Generation sheets found)")
                continue

            final_df = pd.concat(frames, ignore_index=True)

            print("    -------------------------------")
            print("    Inserting data into the database")
            print("    -------------------------------")

            with conn:
                insert_long_form(conn, final_df, logger, tag)

            dt = time.perf_counter() - start
            m, s = divmod(dt, 60)
            print(f"Execution time: {int(m)} minutes and {s:.2f} seconds")
            total = time.perf_counter() - t0
            m, s = divmod(total, 60)
            print(f"Total Execution time: {int(m)} minutes and {s:.2f} seconds")

    except Exception as e:
        log_path = BASE_DIR / "create_db_error.log"
        add_file_logger_on_error(logger, log_path)
        logger.error("Unhandled exception: %s", e)
        logger.error(traceback.format_exc())
        raise
    finally:
        conn.close()

    total = time.perf_counter() - t0
    m, s = divmod(total, 60)
    print("===================================")
    print(f"Total Execution time: {int(m)} minutes and {s:.2f} seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())