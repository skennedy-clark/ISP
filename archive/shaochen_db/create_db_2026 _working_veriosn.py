#!/usr/bin/env python3
"""
create_db_2026.py

Loads ISP hidden workbook data into a SQLite database.

Key tables:
- context: unique context rows (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
- data: annual time-series (Year is stored as a 4-digit year string)
- non_annual_data: non-annual labels (e.g. "Existing and Committed") with same schema as data
- mapping: convenience mapping derived from context

Changes (2026):
- Keep "Existing and Committed" as valid non-annual data (previously dropped).
- Store non-annual rows in non_annual_data, not data.
- FY labels like "2024-25" normalise to ending year "2025".
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / "hidden_data"


# ---------------------------------------------------------------------
# Year normalisation
# ---------------------------------------------------------------------
_FY_RE = re.compile(r"^\s*(\d{4})\s*[-/]\s*(\d{2})\s*$")
_YEAR_RE = re.compile(r"^\s*(\d{4})\s*$")


def normalise_year_label(y) -> str | None:
    """Normalise a workbook 'year' column label.

    Notes
    -----
    Some workbooks have columns like 'Existing and Committed' in the same table.
    This is valid ISP metadata but not an annual year.
    We keep the label and load it into a separate table (non_annual_data).

    FY labels:
      '2024-25' -> '2025' (end year)
      '2025-26' -> '2026' (end year)
    """
    if pd.isna(y):
        return None

    s = str(y).strip()

    # CHANGE (2026): keep non-annual label(s) so we can load them separately.
    if s.lower() in {"existing and committed", "existing & committed"}:
        return "Existing and Committed"

    # Known junk accidental column (user confirmed this can happen)
    if s.lower() == "un33":
        return None

    m = _FY_RE.match(s)
    if m:
        y0 = int(m.group(1))
        y1 = int(m.group(2))
        # treat as end-year
        # e.g. 2024-25 -> 2025; 2025-26 -> 2026
        return str(y0 + 1)

    m = _YEAR_RE.match(s)
    if m:
        return str(int(m.group(1)))

    return None


def find_year_columns(df: pd.DataFrame) -> list[str]:
    """Return original column names that look like year-ish columns.

    We include:
      - 4 digit year columns
      - FY columns like 2024-25
      - 'Existing and Committed' (non-annual, handled later)
    """
    years: list[str] = []
    for col in df.columns:
        if normalise_year_label(col) is not None:
            years.append(col)
    return years


def normalise_years_in_melt(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df['Year'] is the normalised label (annual -> 4 digit, non-annual kept)."""
    df = df.copy()
    df["Year"] = df["Year"].apply(normalise_year_label)
    return df


# ---------------------------------------------------------------------
# Excel readers
# ---------------------------------------------------------------------
def read_xlsx_sheet(path_xlsx: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(path_xlsx, sheet_name=sheet_name)
    df = df.dropna(how="all")
    # trim whitespace in headers
    df.columns = [str(c).strip() for c in df.columns]
    return df


def clean_context_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise basic context columns. Keep changes minimal and defensive."""
    df = df.copy()
    for c in ["CDP", "State", "Region", "Subregion", "Technology"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
            df.loc[df[c].isin(["", "nan", "NaN"]), c] = None
    return df


def _melt_capacity_or_generation(df: pd.DataFrame, value_var: str) -> pd.DataFrame:
    """Melt a wide sheet to long format: context + Year + Value."""
    df = clean_context_columns(df)

    # Identify year columns (including "Existing and Committed")
    year_cols = find_year_columns(df)

    # Base id vars in these workbooks vary. Be permissive.
    id_vars = []
    for c in ["CDP", "State", "Region", "Subregion", "Technology"]:
        if c in df.columns and c not in year_cols:
            id_vars.append(c)

    melted = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=year_cols,
        var_name="Year",
        value_name="Value",
    )
    melted["Variable"] = value_var
    melted = normalise_years_in_melt(melted)

    # Drop rows where Year failed to normalise (keeps noise out)
    melted = melted.dropna(subset=["Year"])

    return melted


def extract_data_from_workbook(
    xlsx_path: Path,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    """Read 'Capacity' and 'Generation' sheets, return a single long df."""
    long_parts: list[pd.DataFrame] = []

    # Capacity
    cap_df = read_xlsx_sheet(xlsx_path, "Capacity")
    long_parts.append(_melt_capacity_or_generation(cap_df, "capacity"))

    # Generation
    gen_df = read_xlsx_sheet(xlsx_path, "Generation")
    long_parts.append(_melt_capacity_or_generation(gen_df, "generation"))

    out = pd.concat(long_parts, ignore_index=True)

    # Attach workbook metadata (be careful: keep as plain strings)
    out["Data_source"] = str(data_source)
    out["Scenario_1"] = str(scenario_1)

    # Scenario_2 sometimes exists as CDP-like (e.g. CDP1), but in your design it's part of context.
    # We'll keep "CDP" as Scenario_2 if present (legacy expectation).
    if "CDP" in out.columns:
        out["Scenario_2"] = out["CDP"]
    else:
        out["Scenario_2"] = None

    # State/Region/Technology are already in columns if present.
    # Ensure expected columns exist.
    for c in ["State", "Region", "Technology", "Subregion"]:
        if c not in out.columns:
            out[c] = None

    # In these workbooks, Region is sometimes "State region" and Subregion is the finer slice.
    # We keep Region as-is (can be null, per your note) and ignore Subregion for context uniqueness.
    return out


# ---------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------
def drop_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # CHANGE (2026): include non_annual_data.
    for t in ["mapping", "context", "data", "non_annual_data"]:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"Dropped table: {t}")
    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE mapping (
            Technology TEXT PRIMARY KEY,
            Plot_Group TEXT
        );
        """
    )
    print("Created table: mapping")

    cur.execute(
        """
        CREATE TABLE context (
            Id INTEGER PRIMARY KEY,
            Data_source TEXT,
            Scenario_1 TEXT,
            Scenario_2 TEXT,
            State TEXT,
            Region TEXT,
            Technology TEXT,
            UNIQUE (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
        );
        """
    )
    print("Created table: context")

    cur.execute(
        """
        CREATE TABLE data (
            Id INTEGER,
            Variable TEXT,
            Year TEXT,
            Value REAL,
            UNIQUE (Id, Variable, Year)
        );
        """
    )
    print("Created table: data")

    # CHANGE (2026): store non-annual labels (e.g. 'Existing and Committed')
    # separately so they don't collide with annual year keys.
    cur.execute(
        """
        CREATE TABLE non_annual_data (
            Id INTEGER,
            Variable TEXT,
            Year TEXT,
            Value REAL,
            UNIQUE (Id, Variable, Year)
        );
        """
    )
    print("Created table: non_annual_data")

    conn.commit()


def _df_to_records(df: pd.DataFrame) -> list[tuple]:
    """Convert a df to sqlite-safe tuples (replace pandas NA with None)."""
    df = df.copy()
    df = df.where(pd.notna(df), None)
    return [tuple(x) for x in df.to_numpy()]


def insert_context_and_data(
    conn: sqlite3.Connection,
    df_long: pd.DataFrame,
) -> None:
    """Upsert context rows then insert data rows with context Id."""

    cur = conn.cursor()

    # Context unique key columns
    context_cols = ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology"]

    # Build context df from incoming data
    context_df = df_long[context_cols].drop_duplicates().copy()

    # Replace pandas NA with None for sqlite
    context_df = context_df.where(pd.notna(context_df), None)

    # Upsert context
    cur.executemany(
        """
        INSERT OR IGNORE INTO context (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        _df_to_records(context_df),
    )

    # Pull context ids and merge back
    ctx_db = pd.read_sql_query(
        "SELECT Id, Data_source, Scenario_1, Scenario_2, State, Region, Technology FROM context",
        conn,
    )

    merged = df_long.merge(ctx_db, on=context_cols, how="left")

    # Ensure numeric Value (NULL allowed)
    merged["Value"] = pd.to_numeric(merged["Value"], errors="coerce")

    data_df = merged[["Id", "Variable", "Year", "Value"]].copy()

    # CHANGE (2026): split annual vs non-annual.
    # Annual rows have Year as a 4-digit string after normalisation (FY->end-year).
    # Non-annual labels (e.g. 'Existing and Committed') go to non_annual_data.
    is_annual = data_df["Year"].astype("string").str.fullmatch(r"\d{4}")
    annual_df = data_df[is_annual]
    nonannual_df = data_df[~is_annual]

    # Insert annual data (REPLACE means last write wins within the rebuild)
    if len(annual_df):
        cur.executemany(
            """
            INSERT OR REPLACE INTO data (Id, Variable, Year, Value)
            VALUES (?, ?, ?, ?)
            """,
            _df_to_records(annual_df),
        )

    # Insert non-annual data (same semantics)
    if len(nonannual_df):
        cur.executemany(
            """
            INSERT OR REPLACE INTO non_annual_data (Id, Variable, Year, Value)
            VALUES (?, ?, ?, ?)
            """,
            _df_to_records(nonannual_df),
        )

    conn.commit()


def populate_mapping_from_context(conn: sqlite3.Connection) -> None:
    """Minimal mapping table population: one row per Technology."""
    cur = conn.cursor()
    techs = cur.execute("SELECT DISTINCT Technology FROM context WHERE Technology IS NOT NULL").fetchall()
    rows = [(t[0], None) for t in techs]
    cur.executemany(
        "INSERT OR IGNORE INTO mapping (Technology, Plot_Group) VALUES (?, ?)",
        rows,
    )
    conn.commit()


# ---------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------
def write_incoming_reports(
    report_dir: Path,
    file_tag: str,
    df_long: pd.DataFrame,
) -> tuple[int, int]:
    """Write per-file reports about potential duplicates/conflicts in incoming data."""
    report_dir.mkdir(parents=True, exist_ok=True)

    keys = ["Data_source", "Scenario_1", "Scenario_2", "State", "Region", "Technology", "Variable", "Year"]
    g = (
        df_long.groupby(keys, dropna=False)
        .agg(
            n_rows=("Value", "size"),
            n_distinct_values=("Value", lambda s: s.dropna().nunique()),
            min_value=("Value", "min"),
            max_value=("Value", "max"),
        )
        .reset_index()
    )

    dupes = g[g["n_rows"] > 1].copy()
    conflicts = g[g["n_distinct_values"] > 1].copy()

    dup_path = report_dir / f"{file_tag}__incoming_dupes.csv"
    conf_path = report_dir / f"{file_tag}__incoming_conflicts.csv"

    dupes.to_csv(dup_path, index=False)
    conflicts.to_csv(conf_path, index=False)

    return len(dupes), len(conflicts)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def list_workbooks(raw_dir: Path) -> list[tuple[str, str, Path]]:
    """Return [(data_source, scenario_1, path_xlsx), ...]."""
    out = []
    for p in raw_dir.rglob("*.xlsx"):
        # Expect directory layout .../<Data_source>/<Scenario>.xlsx
        data_source = p.parent.parent.name if p.parent.parent != raw_dir else p.parent.name
        scenario_1 = p.stem
        out.append((data_source, scenario_1, p))
    out.sort(key=lambda t: (t[0], t[1], str(t[2])))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default=str(BASE_DIR / "ISP.db"))
    ap.add_argument("--report-dir", type=str, default=str(BASE_DIR / "results" / "db_build_reports"))
    args = ap.parse_args()

    db_path = Path(args.db)
    report_dir = Path(args.report_dir)

    print(f"BASE_DIR={BASE_DIR}")

    conn = sqlite3.connect(db_path)
    try:
        drop_tables(conn)
        create_tables(conn)

        books = list_workbooks(RAW_DATA_DIR)

        total_dupes = 0
        total_conflicts = 0

        for i, (data_source, scenario_1, xlsx_path) in enumerate(books, start=1):
            print("===================================")
            print(f"Processing File {i}: {data_source} - {scenario_1}")
            print("-----------------------------------")

            df_long = extract_data_from_workbook(xlsx_path, data_source, scenario_1)

            # Tag for report files
            file_tag = f"{data_source} - {scenario_1}"
            file_tag = re.sub(r"[^A-Za-z0-9_\\-]+", "_", file_tag).strip("_")

            d, c = write_incoming_reports(report_dir, file_tag, df_long)
            total_dupes += d
            total_conflicts += c

            insert_context_and_data(conn, df_long)

        populate_mapping_from_context(conn)

        cur = conn.cursor()
        n_ctx = cur.execute("SELECT COUNT(*) FROM context").fetchone()[0]
        n_data = cur.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        n_nonannual = cur.execute("SELECT COUNT(*) FROM non_annual_data").fetchone()[0]
        n_map = cur.execute("SELECT COUNT(*) FROM mapping").fetchone()[0]

        print("\n=== DONE ===")
        print(f"context rows : {n_ctx}")
        print(f"data rows    : {n_data}")
        print(f"non-annual   : {n_nonannual}")
        print(f"mapping rows : {n_map}")
        print(f"incoming dup groups      : {total_dupes}")
        print(f"incoming conflict groups : {total_conflicts}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
