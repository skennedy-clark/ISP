"""
create_db_2026.py

End-to-end SQLite ingestion for ISP-style Excel workbooks.

Defaults:
- If --inputs not provided, uses: hidden_data/**/*.xlsx (relative to this script)

Constraints:
- Years are STRICT 4-digit integers (19xx/20xx). Everything else becomes non_annual_data.
- "Existing and Committed" ends up in non_annual_data (because it's non-year).
- Duplicate diagnostics are reported (not silently fixed).
- Heuristic parsing (workbooks vary), but:
    * Header row detected by scanning for year tokens in first N rows
    * ID columns inferred as columns left of first year-token column
- Fails explicitly if it cannot identify a plausible header row (even after transpose fallback).

Usage:
  python create_db_2026.py --db isp.sqlite
  python create_db_2026.py --db isp.sqlite --dry-run
  python create_db_2026.py --db isp.sqlite --inputs "hidden_data/2024 Final/*.xlsx"
  python create_db_2026.py --db isp.sqlite --inputs "hidden_data/**/*.xlsx"
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
from pathlib import Path
from typing import Optional, Sequence, Tuple, Dict, Any, List

import pandas as pd


# -----------------------------
# Logging
# -----------------------------
def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")


# -----------------------------
# Schema
# -----------------------------
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS context (
    context_id INTEGER PRIMARY KEY,
    workbook_name TEXT NOT NULL,
    workbook_path TEXT NOT NULL,
    sheet_name TEXT NOT NULL,

    scenario TEXT,
    region TEXT,
    metric TEXT,
    units TEXT,

    meta_json TEXT,
    created_utc TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS data (
    context_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    value REAL,
    source_row INTEGER,
    source_col TEXT,
    id_json TEXT,
    FOREIGN KEY (context_id) REFERENCES context(context_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS non_annual_data (
    context_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    value REAL,
    source_row INTEGER,
    source_col TEXT,
    id_json TEXT,
    FOREIGN KEY (context_id) REFERENCES context(context_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_data_context_year ON data(context_id, year);
CREATE INDEX IF NOT EXISTS idx_nonannual_context_label ON non_annual_data(context_id, label);
"""


# -----------------------------
# Token rules
# -----------------------------
YEAR_RE = re.compile(r"^(19|20)\d{2}$")


def is_year_token(x: object) -> bool:
    if x is None:
        return False
    s = str(x).strip()
    return bool(YEAR_RE.match(s))


def to_year_int(x: object) -> int:
    s = str(x).strip()
    if not is_year_token(s):
        raise ValueError(f"Not a 4-digit year token: {x!r}")
    return int(s)


def norm_str(x: object) -> str:
    if x is None:
        return ""
    return str(x).strip()


def json_compact(d: Dict[str, Any]) -> str:
    import json
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), default=str)


# -----------------------------
# DB helpers
# -----------------------------
def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def insert_context(conn: sqlite3.Connection, kw: Dict[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO context (workbook_name, workbook_path, sheet_name, scenario, region, metric, units, meta_json)
        VALUES (:workbook_name, :workbook_path, :sheet_name, :scenario, :region, :metric, :units, :meta_json)
        """,
        kw,
    )
    return int(cur.lastrowid)


def insert_many_annual(conn: sqlite3.Connection, rows: List[Tuple], dry_run: bool) -> int:
    if dry_run:
        return len(rows)
    conn.executemany(
        "INSERT INTO data (context_id, year, value, source_row, source_col, id_json) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def insert_many_nonannual(conn: sqlite3.Connection, rows: List[Tuple], dry_run: bool) -> int:
    if dry_run:
        return len(rows)
    conn.executemany(
        "INSERT INTO non_annual_data (context_id, label, value, source_row, source_col, id_json) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


# -----------------------------
# Diagnostics
# -----------------------------
def report_duplicates(conn: sqlite3.Connection, limit: int = 20) -> None:
    logging.info("Duplicate diagnostics:")

    q1 = """
    SELECT context_id, year, COUNT(*) AS n
    FROM data
    GROUP BY context_id, year
    HAVING COUNT(*) > 1
    ORDER BY n DESC
    LIMIT ?
    """
    rows = conn.execute(q1, (limit,)).fetchall()
    if rows:
        logging.warning("Duplicates in data by (context_id, year). Top %d:", len(rows))
        for cid, year, n in rows:
            logging.warning("  context_id=%s year=%s n=%s", cid, year, n)
    else:
        logging.info("No duplicates in data by (context_id, year).")

    q2 = """
    SELECT context_id, label, COUNT(*) AS n
    FROM non_annual_data
    GROUP BY context_id, label
    HAVING COUNT(*) > 1
    ORDER BY n DESC
    LIMIT ?
    """
    rows = conn.execute(q2, (limit,)).fetchall()
    if rows:
        logging.warning("Duplicates in non_annual_data by (context_id, label). Top %d:", len(rows))
        for cid, label, n in rows:
            logging.warning("  context_id=%s label=%r n=%s", cid, label, n)
    else:
        logging.info("No duplicates in non_annual_data by (context_id, label).")


def report_existing_and_committed(conn: sqlite3.Connection) -> None:
    q = """
    SELECT COUNT(*)
    FROM non_annual_data
    WHERE lower(trim(label)) = 'existing and committed'
    """
    n = conn.execute(q).fetchone()[0]
    logging.info("Rows stored under label='Existing and Committed' in non_annual_data: %d", n)


# -----------------------------
# Heuristic Excel parsing (no stubs)
# -----------------------------
def read_sheet_raw(xlsx: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(xlsx, sheet_name=sheet_name, header=None, dtype=object)


def score_row_as_header(row: pd.Series) -> int:
    return sum(is_year_token(v) for v in row.values)


def find_header_row(df_raw: pd.DataFrame, min_years: int = 2, max_scan_rows: int = 60) -> Optional[int]:
    scan = df_raw.iloc[: min(max_scan_rows, len(df_raw))]
    best_i = None
    best_score = 0
    for i in range(len(scan)):
        score = score_row_as_header(scan.iloc[i])
        if score > best_score:
            best_score = score
            best_i = i
    if best_i is None or best_score < min_years:
        return None
    return int(best_i)


def extract_meta(df_raw: pd.DataFrame, header_i: int) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    top = df_raw.iloc[:header_i].copy()
    if top.empty:
        return meta

    # parse "key: value" anywhere above header
    for r in range(len(top)):
        for c in range(top.shape[1]):
            v = top.iat[r, c]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if ":" in s:
                k, val = s.split(":", 1)
                k = k.strip().lower()
                val = val.strip()
                if k and val and k not in meta:
                    meta[k] = val

    # parse 2-col key/value rows (two non-null cells)
    for r in range(len(top)):
        row = top.iloc[r]
        nn = []
        for i in range(len(row)):
            v = row.iat[i]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            nn.append((i, v))
        if len(nn) == 2:
            k = str(nn[0][1]).strip().rstrip(":").lower()
            v = str(nn[1][1]).strip()
            if k and v and k not in meta:
                meta[k] = v

    return meta


def promote_header(df_raw: pd.DataFrame, header_i: int) -> pd.DataFrame:
    header = df_raw.iloc[header_i].astype(str).map(lambda x: x.strip() if x else "")
    df = df_raw.iloc[header_i + 1 :].copy()
    df.columns = header
    df = df.reset_index(drop=True)
    df = df.dropna(how="all")

    # Ensure unique, non-empty column names
    cols = list(df.columns)
    seen: Dict[str, int] = {}
    new_cols = []
    for c in cols:
        c2 = c if c else "__blank__"
        seen[c2] = seen.get(c2, 0) + 1
        new_cols.append(c2 if seen[c2] == 1 else f"{c2}__{seen[c2]}")
    df.columns = new_cols
    return df


def infer_id_and_value_cols(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    cols = list(df.columns)
    if not cols:
        raise ValueError("Sheet has no columns after promoting header.")

    year_positions = [i for i, c in enumerate(cols) if is_year_token(c)]
    if not year_positions:
        raise ValueError(
            "No strict 4-digit year column headers found. "
            "This script requires at least one strict year header to anchor parsing."
        )

    first_year_idx = min(year_positions)
    id_cols = cols[:first_year_idx]
    value_cols = cols[first_year_idx:]
    return id_cols, value_cols


def parse_sheet_to_wide(xlsx: Path, sheet_name: str) -> Dict[str, Any]:
    df_raw = read_sheet_raw(xlsx, sheet_name)
    header_i = find_header_row(df_raw)

    was_transposed = False
    if header_i is None:
        df_raw_t = df_raw.transpose().reset_index(drop=True)
        header_i_t = find_header_row(df_raw_t)
        if header_i_t is None:
            raise ValueError(
                f"Could not locate a header row with >=2 strict year tokens in first 60 rows "
                f"for sheet '{sheet_name}' in '{xlsx.name}' (tried transpose fallback too)."
            )
        df_raw = df_raw_t
        header_i = header_i_t
        was_transposed = True

    meta = extract_meta(df_raw, header_i)
    df = promote_header(df_raw, header_i)
    id_cols, value_cols = infer_id_and_value_cols(df)

    return {
        "df": df,
        "header_row_index": header_i,
        "id_cols": id_cols,
        "value_cols": value_cols,
        "meta": meta,
        "was_transposed": was_transposed,
    }


def melt_and_split(parsed: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = parsed["df"].copy()
    id_cols = parsed["id_cols"]
    value_cols = parsed["value_cols"]

    df = df.reset_index(drop=False).rename(columns={"index": "source_row"})

    melt_id_vars = ["source_row"] + id_cols
    melted = df.melt(
        id_vars=melt_id_vars,
        value_vars=value_cols,
        var_name="source_col",
        value_name="value",
    )

    if id_cols:
        id_json = melted[id_cols].apply(
            lambda r: json_compact({c: (None if pd.isna(r[c]) else r[c]) for c in id_cols}),
            axis=1,
        )
    else:
        id_json = pd.Series(["{}"] * len(melted), index=melted.index)
    melted["id_json"] = id_json

    melted["source_col_str"] = melted["source_col"].astype(str).str.strip()

    annual = melted[melted["source_col_str"].apply(is_year_token)].copy()
    nonannual = melted[~melted["source_col_str"].apply(is_year_token)].copy()

    annual_long = annual.rename(columns={"source_col_str": "year"})[
        ["year", "value", "source_row", "source_col", "id_json"]
    ]
    nonannual_long = nonannual.rename(columns={"source_col_str": "label"})[
        ["label", "value", "source_row", "source_col", "id_json"]
    ]

    return annual_long, nonannual_long


def guess_context_fields(sheet_name: str, meta: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    def pick(*keys: str) -> Optional[str]:
        for k in keys:
            kk = k.lower()
            if kk in meta and str(meta[kk]).strip():
                return str(meta[kk]).strip()
        return None

    scenario = pick("scenario", "isp scenario", "case")
    region = pick("region", "nem region", "state")
    units = pick("units", "unit")
    metric = pick("metric", "measure", "technology", "fuel")

    if metric is None and sheet_name:
        metric = sheet_name

    return scenario, region, metric, units


# -----------------------------
# Input discovery
# -----------------------------
def discover_default_workbooks(project_root: Path) -> List[Path]:
    base = project_root / "hidden_data"
    if not base.exists():
        raise FileNotFoundError(f"Default hidden_data folder not found: {base}")
    files = sorted([p for p in base.rglob("*.xlsx") if p.is_file()])
    if not files:
        raise FileNotFoundError(f"No .xlsx files found under: {base}")
    return files


def iter_input_files(patterns: Optional[Sequence[str]], project_root: Path) -> List[Path]:
    if not patterns:
        return discover_default_workbooks(project_root)

    out: List[Path] = []
    for pat in patterns:
        full_pat = (project_root / pat) if not Path(pat).is_absolute() else Path(pat)

        if "**" in str(full_pat):
            # handle patterns like hidden_data/**/*.xlsx
            # split at ** and rglob suffix
            s = str(full_pat)
            before, after = s.split("**", 1)
            base = Path(before.rstrip("/\\"))
            suffix = after.lstrip("/\\") or "*"
            if base.exists():
                out.extend([p for p in base.rglob(suffix) if p.is_file()])
        else:
            out.extend([p for p in full_pat.parent.glob(full_pat.name) if p.is_file()])

    out = sorted(set(out))
    if not out:
        raise FileNotFoundError(f"No input files matched patterns: {patterns}")
    return out


# -----------------------------
# Main
# -----------------------------
def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path, help="Path to SQLite database file")
    ap.add_argument(
        "--inputs",
        nargs="*",
        default=None,
        help='Excel workbook glob(s). If omitted: hidden_data/**/*.xlsx (relative to script)',
    )
    ap.add_argument("--sheets", nargs="*", default=None, help="Optional sheet names (default: all sheets)")
    ap.add_argument("--dry-run", action="store_true", help="Parse + report, but do not write inserts")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--duplicate-report-limit", type=int, default=20)
    args = ap.parse_args(argv)

    setup_logging(args.verbose)

    project_root = Path(__file__).resolve().parent
    input_files = iter_input_files(args.inputs, project_root)

    conn = connect_sqlite(args.db)
    try:
        init_schema(conn)

        total_context = 0
        total_annual = 0
        total_nonannual = 0

        for xlsx in input_files:
            logging.info("Ingest workbook: %s", xlsx)

            try:
                xl = pd.ExcelFile(xlsx)
            except Exception as e:
                raise RuntimeError(f"Failed to open workbook: {xlsx}") from e

            sheet_names = args.sheets if args.sheets else xl.sheet_names

            for sheet in sheet_names:
                logging.info("  sheet: %s", sheet)

                parsed = parse_sheet_to_wide(xlsx, sheet)
                annual_long, nonannual_long = melt_and_split(parsed)

                # enforce strict year ints
                annual_long = annual_long.copy()
                annual_long["year"] = annual_long["year"].apply(to_year_int)

                # normalise nonannual labels
                nonannual_long = nonannual_long.copy()
                nonannual_long["label"] = nonannual_long["label"].map(norm_str)

                scenario, region, metric, units = guess_context_fields(sheet, parsed["meta"])

                context_kwargs = {
                    "workbook_name": xlsx.name,
                    "workbook_path": str(xlsx),
                    "sheet_name": sheet,
                    "scenario": scenario,
                    "region": region,
                    "metric": metric,
                    "units": units,
                    "meta_json": json_compact(
                        {
                            "parsed_header_row_index": parsed["header_row_index"],
                            "was_transposed": parsed["was_transposed"],
                            "meta": parsed["meta"],
                            "id_cols": parsed["id_cols"],
                            "value_cols_count": len(parsed["value_cols"]),
                        }
                    ),
                }

                if args.dry_run:
                    logging.info(
                        "[dry-run] id_cols=%s annual_rows=%d nonannual_rows=%d (transposed=%s)",
                        parsed["id_cols"], len(annual_long), len(nonannual_long), parsed["was_transposed"]
                    )
                    continue

                with conn:
                    context_id = insert_context(conn, context_kwargs)
                    total_context += 1

                    annual_rows: List[Tuple] = []
                    for _, r in annual_long.iterrows():
                        val = None if pd.isna(r["value"]) else float(r["value"])
                        annual_rows.append(
                            (context_id, int(r["year"]), val, int(r["source_row"]), str(r["source_col"]), str(r["id_json"]))
                        )
                    total_annual += insert_many_annual(conn, annual_rows, dry_run=False)

                    nonannual_rows: List[Tuple] = []
                    for _, r in nonannual_long.iterrows():
                        val = None if pd.isna(r["value"]) else float(r["value"])
                        nonannual_rows.append(
                            (context_id, str(r["label"]), val, int(r["source_row"]), str(r["source_col"]), str(r["id_json"]))
                        )
                    total_nonannual += insert_many_nonannual(conn, nonannual_rows, dry_run=False)

        logging.info("Done. context=%d annual_rows=%d nonannual_rows=%d", total_context, total_annual, total_nonannual)

        report_existing_and_committed(conn)
        report_duplicates(conn, limit=args.duplicate_report_limit)

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
