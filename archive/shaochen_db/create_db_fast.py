"""
create_db_fast.py
=================
ISP (Integrated System Plan) modelling results ingestion pipeline.

Pipeline overview
-----------------
Ingests AEMO ISP modelling results from pre-processed xlsx files into a local
SQLite database, following a standard ETL pattern:

  Extract   — recursively discover xlsx files from hidden_data/
  Transform — parse filename metadata, read named sheets, melt year columns
              to long format, normalise year labels, split annual vs non-annual
  Load      — insert into ISP_HOME.db: context, data, non_annual_data, mapping

Performance vs create_db_home.py
---------------------------------
  1. Each xlsx is opened once (ExcelFile handle) instead of once per sheet.
  2. Context read-back is filtered to the current file's Data_source and
     Scenario_1 only, not a full table scan.
  3. Each file's inserts run inside a single explicit transaction.
  4. normalise_year is vectorised using pandas str operations instead of
     row-by-row apply().
  5. DataFrame -> SQLite conversion uses df.values.tolist() instead of
     to_numpy() + list comprehension.
  6. Large inserts are chunked (INSERT_CHUNK_ROWS) to avoid SQLite page stalls.

Source file structure
---------------------
A separate pre-processing script unhides tabs in the original AEMO workbooks
and writes clean files into subdirectories of hidden_data/.

Filename metadata:
  Directory name → Data_source   "2022 Draft" → "2022 Draft ISP"
  Filename       → Scenario_1    everything after the first ' - ' up to .xlsx
                   "2022 Draft ISP results workbook - Step Change - low gas prices.xlsx"
                   → "Step Change - low gas prices"

Sheets ingested per file and their schema mapping:
  Sheet                  Variable          State col   Region col
  ─────────────────────  ────────────────  ──────────  ──────────────────────
  Capacity               capacity          Region      None (synthetic on load)
  Generation             generation        Region      None (synthetic on load)
  Storage Capacity       storage capacity  Region      None (synthetic on load)
  Storage Energy         storage energy    Region      None (synthetic on load)
  REZ Generation Cap.    capacity          Region      REZ (finer geographic)
  REZ Generation         generation        Region      REZ (finer geographic)

Column conventions (consistent across 2022–2026 vintages):
  CDP          → Scenario_2
  Region       → State  (contains state abbreviations: NSW, QLD etc)
  REZ          → Region (REZ-level geographic code: Q1, N10 etc)
  Technology   → Technology
  Storage cat. → Technology (Storage Capacity sheet only)
  REZ Name     → dropped

Year column normalisation:
  Float/int years  2024.0 / 2024  → "2024"
  FY labels        "2024-25"      → "2025" (end year)
  "Existing and Committed"        → non_annual_data table
  Anything unrecognised           → dropped with a warning

Duplicate handling:
  context uses INSERT OR IGNORE (first write wins).
  data / non_annual_data use INSERT OR IGNORE.
  Any duplicate key in incoming data is written to a per-file CSV report
  in results/db_build_reports/ so the source can be investigated.

Database
--------
ISP_HOME.db is created next to this script.
Change DB_NAME below when moving between environments.

TODO (remaining open items):
  1. [DONE] Storage sheets (Storage Capacity, Storage Energy) wired in via
     STATE_SHEETS. Filtered by data_req_state_storage.csv.
  2. [DONE] REZ sheets (REZ Generation Capacity, REZ Generation) wired in via
     REZ_SHEETS and transform_rez_sheet(). Filtered by data_req_rez.csv.
  3. [CLOSED] Synthetic Region codes (N0, Q0 etc) will NOT be implemented.
     Investigation of the old DB confirmed these were routing artefacts:
       - X0 codes do NOT exist in the AEMO REZ documentation.
         Real NSW zones start at N1; real QLD zones start at Q1 etc.
       - X0 was a catch-all bucket — Offshore Wind parked there because it
         had no specific inland REZ assignment.
       - N0 capacity != sum(N1..N8) — not a state total, not meaningful.
     Offshore Wind loads correctly via its real zone codes (O1, O2 etc)
     directly from the REZ sheets. Region=NULL for state-level rows is correct.
  4. [DONE] data_req_*.csv filters wired in as technology allowlists applied
     at transform time. Technologies absent from filters are dropped with a
     WARNING so they are visible.
     All three previously missing technologies have been added to their
     respective filter CSVs:
       - Borumba              → data_req_state_storage.csv
       - Alkaline Electrolyser → data_req_rez.csv
       - Large-scale Storage  → data_req_rez.csv
     NOTE: filter CSVs remain flat (no per-Data_source rows). If scope
     differs by Data_source in future, split into per-Data_source versions
     under input_csv/. Use find_missing_technologies.py to detect gaps.
  5. [DONE] name_map_technology.csv wired in — 57 native names standardised
     to 28 clean Technology values at transform time.
  6. [OPEN] Revisit create_db_config.yml exception thresholds. upper: 100
     does not apply to capacity (MW) or generation (GWh) where legitimate
     values run into thousands. Parked until full variable set is known.
  7. [OPEN] Incremental loads — currently drops and recreates all tables on
     each run. Extend to preserve existing data for partial re-runs.
"""

# ---------------------------------------------------------------------------
# Environment — change DB_NAME when moving between home / work machines.
# ---------------------------------------------------------------------------
DB_NAME = "ISP_HOME_FAST.db"  # same database - fast script is a drop-in replacement

# ---------------------------------------------------------------------------
# DEBUG — controls which files are processed.
#
# DEBUG = True   : filter to only directories whose name is in DEBUG_DIRS.
#                  Set DEBUG_DIRS = [] to process all directories.
# DEBUG = False  : process all files — full run, no filtering.
#
# Remove this block entirely before committing to production.
# ---------------------------------------------------------------------------
DEBUG = False
DEBUG_DIRS = []   # ignored when DEBUG = False
# ---------------------------------------------------------------------------

# Chunk size for large executemany inserts - avoids SQLite page stalls
INSERT_CHUNK_ROWS = 50_000

import logging
import re
import sqlite3
import sys
from datetime import datetime
from itertools import groupby
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR  = Path(__file__).parent.resolve()
DB_PATH     = SCRIPT_DIR / DB_NAME
DATA_DIR    = SCRIPT_DIR / "hidden_data"
REPORT_DIR      = SCRIPT_DIR / "results" / "db_build_reports"
TECH_MAP_PATH   = SCRIPT_DIR / "input_csv" / "name_map_technology.csv"
FILTER_DIR      = SCRIPT_DIR / "input_csv"
LOG_PATH        = SCRIPT_DIR / f"error_{datetime.now().strftime('%Y%m%d')}.log"

# ---------------------------------------------------------------------------
# Logging — file only created on first ERROR, deleted at start of each run.
# ---------------------------------------------------------------------------

class _LazyFileHandler(logging.Handler):
    """Defers creation of the log file until the first ERROR is emitted.
    Deletes any existing log file from a previous run on first write."""
    def __init__(self, path: Path):
        super().__init__()
        self.path = path
        self._fh: Optional[logging.FileHandler] = None

    def _get_fh(self) -> logging.FileHandler:
        if self._fh is None:
            if self.path.exists():
                self.path.unlink()
            self._fh = logging.FileHandler(self.path)
            self._fh.setFormatter(self.formatter)
        return self._fh

    def emit(self, record: logging.LogRecord) -> None:
        self._get_fh().emit(record)


_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_fmt)
_lazy_file = _LazyFileHandler(LOG_PATH)
_lazy_file.setLevel(logging.ERROR)
_lazy_file.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_console, _lazy_file])
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Year normalisation
# ---------------------------------------------------------------------------
_FY_RE   = re.compile(r"^\s*(\d{4})\s*[-/]\s*(\d{2})\s*$")
_YEAR_RE = re.compile(r"^\s*(\d{4})\s*$")

NON_ANNUAL_LABEL = "Existing and Committed"


def normalise_year(val) -> Optional[str]:
    """
    Convert a raw year column header to a normalised string.

    Returns:
      "YYYY"                    — for annual years (int, float, or FY string)
      "Existing and Committed"  — for the non-annual label
      None                      — for anything unrecognised (row will be dropped)

    FY labels take the end year: "2024-25" → "2025".
    Float years from Excel: 2024.0 → "2024".
    """
    if pd.isna(val):
        return None

    s = str(val).strip()

    if s.lower() in {"existing and committed", "existing & committed"}:
        return NON_ANNUAL_LABEL

    # Float year from Excel e.g. 2024.0
    try:
        f = float(s)
        if f == int(f):
            s = str(int(f))
    except (ValueError, OverflowError):
        pass

    m = _FY_RE.match(s)
    if m:
        return str(int(m.group(1)) + 1)   # end year

    m = _YEAR_RE.match(s)
    if m:
        return str(int(m.group(1)))

    log.warning(f"  Unrecognised year column '{val}' — dropped")
    return None


def _normalise_year_series(s: pd.Series) -> pd.Series:
    """
    Vectorised equivalent of normalise_year() for a Series of year values.
    Operates on all rows at once using pandas string operations - much faster
    than apply(normalise_year) for large DataFrames.

    Processing order:
      1. Non-annual label (Existing and Committed)
      2. Float years from Excel (2024.0 -> "2024")
      3. FY labels (2024-25 -> "2025")
      4. Plain integer years ("2024" -> "2024")
      5. Anything else -> None (will be dropped by dropna)
    """
    import numpy as np

    s = s.astype(str).str.strip()
    result = pd.Series(index=s.index, dtype=object)

    # Non-annual label
    non_annual_mask = s.str.lower().isin({"existing and committed", "existing & committed"})
    result[non_annual_mask] = NON_ANNUAL_LABEL

    # Float years: "2024.0" -> "2024"
    remaining = ~non_annual_mask
    float_mask = remaining & s.str.fullmatch(r"\d{4}\.0+")
    result[float_mask] = s[float_mask].str.extract(r"(\d{4})", expand=False)

    # FY labels: "2024-25" -> "2025"
    remaining = remaining & ~float_mask
    fy_mask = remaining & s.str.fullmatch(r"\d{4}[-/]\d{2}")
    result[fy_mask] = (
        s[fy_mask].str.extract(r"(\d{4})", expand=False).astype(int) + 1
    ).astype(str)

    # Plain integer years: "2024" -> "2024"
    remaining = remaining & ~fy_mask
    int_mask = remaining & s.str.fullmatch(r"\d{4}")
    result[int_mask] = s[int_mask]

    # Anything not matched stays None - dropped downstream by dropna(subset=["Year"])
    unmatched = remaining & ~int_mask & ~non_annual_mask
    if unmatched.any():
        for v in s[unmatched].unique():
            log.warning(f"  Unrecognised year value '{v}' — rows dropped")

    return result


def _year_cols(df: pd.DataFrame, exclude: List[str] = None) -> List[str]:
    """Return column names that normalise to a valid year or non-annual label.
    Pass exclude= to skip known id columns and avoid spurious warnings."""
    skip = set(exclude or [])
    return [c for c in df.columns if c not in skip and normalise_year(c) is not None]


# ---------------------------------------------------------------------------
# Sheet definitions
# ---------------------------------------------------------------------------
# STATE-LEVEL SHEETS (sheet_name, variable_label)
# CDP->Scenario_2, Region->State, Subregion->Region (if present)
# Storage Capacity 'storage category' col handled by TECH_MAP block in transform_sheet
STATE_SHEETS = [
    ("Capacity",         "capacity"),
    ("Generation",       "generation"),
    ("Storage Capacity", "storage capacity"),
    ("Storage Energy",   "storage energy"),
]

# REZ-LEVEL SHEETS (sheet_name, variable_label)
# CDP->Scenario_2, Region->State, REZ->Region, REZ Name->dropped
REZ_SHEETS = [
    ("REZ Generation Capacity", "capacity"),
    ("REZ Generation",          "generation"),
]

# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

def drop_tables(conn: sqlite3.Connection) -> None:
    """
    Drop transient tables before each full rebuild.
    mapping is normally preserved across runs, but is dropped and recreated
    if its schema does not match the expected columns - this handles the case
    where an old schema (e.g. Technology/Plot_Group) is still present in the db.
    TODO: extend to support incremental loads without dropping data tables.
    """
    cur = conn.cursor()
    for t in ["context", "data", "non_annual_data"]:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        log.info(f"Dropped table: {t}")

    # Drop mapping if its schema does not match expected - handles legacy schema
    expected_cols = {"Data_source", "Attribute_type", "Original_value", "Standard_value"}
    cur.execute("PRAGMA table_info(mapping)")
    existing_cols = {row[1] for row in cur.fetchall()}
    if existing_cols and existing_cols != expected_cols:
        cur.execute("DROP TABLE IF EXISTS mapping")
        log.warning(
            f"mapping table had wrong schema {existing_cols} - "
            f"dropped and will be recreated with correct schema"
        )

    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Create all tables. mapping uses IF NOT EXISTS so existing content
    survives re-runs. All other tables are recreated fresh.
    """
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mapping (
            Data_source     TEXT,
            Attribute_type  TEXT,
            Original_value  TEXT,
            Standard_value  TEXT,
            PRIMARY KEY (Data_source, Attribute_type, Original_value)
        );
    """)
    log.info("Created table: mapping (preserved across runs)")

    cur.execute("""
        CREATE TABLE context (
            Id          INTEGER PRIMARY KEY,
            Data_source TEXT,
            Scenario_1  TEXT,
            Scenario_2  TEXT,
            State       TEXT,
            Region      TEXT,
            Technology  TEXT,
            UNIQUE (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
        );
    """)
    log.info("Created table: context")

    cur.execute("""
        CREATE TABLE data (
            Id       INTEGER,
            Variable TEXT,
            Year     TEXT,
            Value    REAL,
            UNIQUE (Id, Variable, Year)
        );
    """)
    log.info("Created table: data")

    cur.execute("""
        CREATE TABLE non_annual_data (
            Id       INTEGER,
            Variable TEXT,
            Year     TEXT,
            Value    REAL,
            UNIQUE (Id, Variable, Year)
        );
    """)
    log.info("Created table: non_annual_data")

    conn.commit()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Open or create the SQLite database with performance PRAGMAs set.
      journal_mode=WAL   - concurrent reads don't block writes
      synchronous=NORMAL - fsync only at checkpoints, safe with WAL
      cache_size=-65536  - 64MB page cache (default is ~2MB)
      temp_store=MEMORY  - temp tables and indexes in RAM
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-65536;")   # 64 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY;")
    log.info(f"Connected to database: {db_path}")
    return conn


# ---------------------------------------------------------------------------
# Extract — file discovery
# ---------------------------------------------------------------------------

def find_xlsx_files(data_dir: Path) -> List[Path]:
    """
    Recursively find all xlsx files under data_dir, sorted alphabetically.
    Skips Excel temporary lock files (~$filename.xlsx).
    DEBUG mode caps to DEBUG_FILES_PER_DIR per subdirectory.
    """
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    files = sorted(
        p for p in data_dir.rglob("*.xlsx")
        if not p.name.startswith("~$")
    )

    # --- DEBUG HACK: directory filter — remove before production ---
    if DEBUG and DEBUG_DIRS:
        files = [f for f in files if f.parent.name in DEBUG_DIRS]
        log.warning(f"DEBUG MODE: filtering to {DEBUG_DIRS} — {len(files)} file(s)")
    elif DEBUG and not DEBUG_DIRS:
        log.warning("DEBUG MODE: DEBUG_DIRS=[] so processing all directories")
    # --- END DEBUG HACK ---

    return files


# ---------------------------------------------------------------------------
# Extract — filename parsing
# ---------------------------------------------------------------------------

def parse_filename(path: Path) -> Tuple[str, str]:
    """
    Derive Data_source and Scenario_1 from the file path.

    Data_source: parent directory name + ' ISP'
      "2022 Draft" → "2022 Draft ISP"

    Scenario_1: everything after the first ' - ' separator in the filename stem.
      "2022 Draft ISP results workbook - Step Change - low gas prices.xlsx"
          → "Step Change - low gas prices"
      "2024 ISP - Step Change - Core.xlsx"
          → "Step Change - Core"

    Logs a warning if no separator is found and uses the full stem.
    """
    data_source = path.parent.name + " ISP"
    stem = path.stem
    idx  = stem.find(" - ")
    scenario_1 = stem[idx + 3:] if idx != -1 else stem
    if idx == -1:
        log.warning(f"No ' - ' separator in filename: {path.name}")
    return data_source, scenario_1


def _test_parse_filename() -> bool:
    """Self-tests for parse_filename(). Runs at pipeline startup."""
    cases = [
        ("2022 Draft", "2022 Draft ISP results workbook - Hydrogen Superpower - 10% discount rate.xlsx",
         "2022 Draft ISP", "Hydrogen Superpower - 10% discount rate"),
        ("2022 Draft", "2022 Draft ISP results workbook - Step Change - Social Licence.xlsx",
         "2022 Draft ISP", "Step Change - Social Licence"),
        ("2024 Final", "2024 ISP - Step Change - Core.xlsx",
         "2024 Final ISP", "Step Change - Core"),
        ("2026 Draft", "2026 ISP - Accelerated Transition - Core.xlsx",
         "2026 Draft ISP", "Accelerated Transition - Core"),
        ("2026 Draft", "2026 ISP - Step Change - Gas Development Projection Option 1.xlsx",
         "2026 Draft ISP", "Step Change - Gas Development Projection Option 1"),
    ]
    print("\n--- Filename parser tests ---")
    all_ok = True
    for dir_name, filename, expected_ds, expected_s1 in cases:
        path = Path(dir_name) / filename
        ds, s1 = parse_filename(path)
        ok = (ds == expected_ds and s1 == expected_s1)
        if not ok:
            all_ok = False
        print(f"  {'OK  ' if ok else 'FAIL'}  ds='{ds}'  s1='{s1}'")
        if not ok:
            print(f"        expected: ds='{expected_ds}'  s1='{expected_s1}'")
    print(f"--- {'All passed' if all_ok else 'FAILURES ABOVE'} ---\n")
    return all_ok


# ---------------------------------------------------------------------------
# Technology name mapping
# ---------------------------------------------------------------------------

def load_tech_map(path: Path) -> dict:
    """Load native_name -> standard_name from CSV. Returns {} if file missing."""
    if not path.exists():
        log.warning(
            f"Technology map not found: {path} — names will not be standardised"
        )
        return {}
    df = pd.read_csv(path)
    mapping = dict(zip(df["native_name"].str.strip(), df["standard_name"].str.strip()))
    log.info(f"Technology map loaded: {len(mapping)} entries from {path.name}")
    return mapping


# Module-level dict — populated in run(), used in transform_sheet
TECH_MAP: dict = {}


def load_filters(filter_dir: Path, tech_map: dict) -> dict:
    """
    Load technology allowlists from data_req_*.csv files.
    Technology names in the CSVs are standardised via tech_map before use
    so they match the standardised names already in the pipeline.

    Returns a dict keyed by variable label:
      "capacity"        -> set of allowed Technology strings (state sheets)
      "generation"      -> set of allowed Technology strings (state sheets)
      "storage capacity"-> set of allowed Technology strings (storage sheets)
      "storage energy"  -> same as storage capacity
      "rez"             -> set of allowed Technology strings (REZ sheets)

    NOTE: The filter CSVs were built against 2022/2024 vintages. Technologies
    introduced in 2026 Draft (Alkaline Electrolyser, Borumba, Large-scale
    Storage) are not present in any filter and will be logged as warnings.
    TODO: extend filter CSVs per Data_source once 2026 Draft scope is confirmed.
    """
    def _load(filename: str, tech_col: str) -> set:
        path = filter_dir / filename
        if not path.exists():
            log.warning(f"Filter file not found: {path} — no filtering applied")
            return set()
        df = pd.read_csv(path)
        techs = df[tech_col].dropna().astype(str).str.strip().unique()
        return set(tech_map.get(t, t) for t in techs)

    cap     = _load("data_req_state_capacity.csv",   "Technology")
    gen     = _load("data_req_state_generation.csv", "Technology")
    storage = _load("data_req_state_storage.csv",    "Unnamed: 0")
    rez     = _load("data_req_rez.csv",              "Unnamed: 0")

    filters = {
        "capacity":         cap,
        "generation":       gen,
        "storage capacity": storage,
        "storage energy":   storage,   # same allowlist as storage capacity
        "rez":              rez,
    }

    for label, allowed in filters.items():
        if allowed:
            log.info(f"Filter '{label}': {len(allowed)} technologies allowed")
    return filters


# Module-level — populated in run()
FILTERS: dict = {}


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _read_sheet(source, sheet_name: str) -> Optional[pd.DataFrame]:
    """
    Read a sheet from either a Path or an already-open pd.ExcelFile handle.
    Accepts ExcelFile to avoid reopening the file for every sheet (speed).
    Strips whitespace from column names, drops fully empty rows.
    Returns None if the sheet is missing.
    """
    try:
        df = pd.read_excel(source, sheet_name=sheet_name)
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        name = source.filename if isinstance(source, pd.ExcelFile) else str(source)
        log.warning(f"  Sheet '{sheet_name}' not found in {name}: {e}")
        return None


def _clean_str_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Strip whitespace from known string id columns; replace blank/nan with None."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
            df.loc[df[c].isin(["", "nan", "NaN", "<NA>"]), c] = None
    return df


def transform_sheet(
    xl,
    sheet_name: str,
    variable: str,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    """
    Read one Capacity or Generation sheet and return a long-format DataFrame.

    Output columns:
      Data_source, Scenario_1, Scenario_2, State, Region,
      Technology, Variable, Year, Value

    Column mapping (consistent across all vintages):
      CDP       → Scenario_2
      Region    → State       (always contains state abbreviation: NSW, QLD etc)
      Subregion → Region      (2024 Final / 2026 Draft only; sub-state code e.g. NNSW)
                              Absent in 2022/2024 Draft — Region left as None
      Technology → Technology

    Year columns are detected by normalise_year():
      Integer/float year  "2024" / 2024.0  → "2024"
      FY label            "2024-25"         → "2025"  (end year)
      "Existing and Committed"              → routed to non_annual_data
      Unrecognised                          → dropped with warning
    """
    df = _read_sheet(xl, sheet_name)
    if df is None:
        return pd.DataFrame()

    df = _clean_str_cols(df, ['CDP', 'Region', 'Subregion', 'Technology',
                               'Storage category', 'storage category'])

    # Normalise technology column name ('storage category' -> 'Technology')
    # then apply name standardisation map
    for _col in list(df.columns):
        if _col.lower().strip() in ('technology', 'storage category'):
            if _col != 'Technology':
                df = df.rename(columns={_col: 'Technology'})
            if TECH_MAP:
                df['Technology'] = (
                    df['Technology']
                    .astype(str)
                    .str.strip()
                    .map(lambda x: TECH_MAP.get(x, x))
                )
            break

    # CDP → Scenario_2, Region → State (always state-level: NSW, QLD etc)
    df = df.rename(columns={'CDP': 'Scenario_2', 'Region': 'State'})

    # Subregion → Region (present in 2024 Final and 2026 Draft only)
    if 'Subregion' in df.columns:
        df = df.rename(columns={'Subregion': 'Region'})
    else:
        df['Region'] = None

    # Ensure all schema id columns exist
    for c in ['Scenario_2', 'State', 'Region', 'Technology']:
        if c not in df.columns:
            df[c] = None

    id_cols  = ['Scenario_2', 'State', 'Region', 'Technology']
    val_cols = _year_cols(df, exclude=id_cols)

    if not val_cols:
        log.warning(f"  No year columns found in '{sheet_name}' of {path.name}")
        return pd.DataFrame()

    melted = df.melt(
        id_vars=id_cols,
        value_vars=val_cols,
        var_name='Year',
        value_name='Value',
    )

    melted['Year']        = _normalise_year_series(melted['Year'])
    melted['Data_source'] = data_source
    melted['Scenario_1']  = scenario_1
    melted['Variable']    = variable

    # Drop rows where year failed to normalise
    melted = melted.dropna(subset=['Year'])

    # Apply technology allowlist filter
    allowed = FILTERS.get(variable, set())
    if allowed:
        dropped = set(melted['Technology'].dropna().unique()) - allowed
        if dropped:
            log.warning(
                f"  [{sheet_name}] dropping {len(dropped)} unlisted "
                f"technology(s): {sorted(dropped)}"
            )
        melted = melted[melted['Technology'].isin(allowed)]

    return melted[['Data_source', 'Scenario_1', 'Scenario_2', 'State',
                   'Region', 'Technology', 'Variable', 'Year', 'Value']]


def transform_rez_sheet(
    xl,
    sheet_name: str,
    variable: str,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    """
    Read one REZ Generation Capacity or REZ Generation sheet.

    Column mapping (all vintages):
      CDP      -> Scenario_2
      Region   -> State    (state abbreviation: NSW, QLD etc)
      REZ      -> Region   (zone code: N1, Q3, O2, N0 etc)
      REZ Name -> dropped
      Technology -> mapped via TECH_MAP ('Solar' -> 'Utility-scale Solar')
    """
    df = _read_sheet(xl, sheet_name)
    if df is None:
        return pd.DataFrame()

    df = _clean_str_cols(df, ["CDP", "Region", "REZ", "Technology"])

    # Apply technology name standardisation
    if "Technology" in df.columns and TECH_MAP:
        df["Technology"] = (
            df["Technology"]
            .astype(str).str.strip()
            .map(lambda x: TECH_MAP.get(x, x))
        )

    # CDP->Scenario_2, Region->State, REZ->Region, drop REZ Name
    df = df.rename(columns={"CDP": "Scenario_2", "Region": "State", "REZ": "Region"})
    df = df.drop(columns=["REZ Name"], errors="ignore")

    for c in ["Scenario_2", "State", "Region", "Technology"]:
        if c not in df.columns:
            df[c] = None

    id_cols  = ["Scenario_2", "State", "Region", "Technology"]
    val_cols = _year_cols(df, exclude=id_cols)

    if not val_cols:
        log.warning(f"  No year columns found in '{sheet_name}' of {path.name}")
        return pd.DataFrame()

    melted = df.melt(
        id_vars=id_cols,
        value_vars=val_cols,
        var_name="Year",
        value_name="Value",
    )

    melted["Year"]        = _normalise_year_series(melted["Year"])
    melted["Data_source"] = data_source
    melted["Scenario_1"]  = scenario_1
    melted["Variable"]    = variable
    melted = melted.dropna(subset=["Year"])

    # Apply REZ technology allowlist filter
    allowed = FILTERS.get("rez", set())
    if allowed:
        dropped = set(melted["Technology"].dropna().unique()) - allowed
        if dropped:
            log.warning(
                f"  [{sheet_name}] dropping {len(dropped)} unlisted "
                f"technology(s): {sorted(dropped)}"
            )
        melted = melted[melted["Technology"].isin(allowed)]

    return melted[["Data_source", "Scenario_1", "Scenario_2", "State",
                   "Region", "Technology", "Variable", "Year", "Value"]]


def transform_file(path: Path) -> pd.DataFrame:
    """
    Run transform_sheet() across STATE_SHEETS and transform_rez_sheet()
    across REZ_SHEETS for one xlsx file.

    Opens the xlsx file once as a pd.ExcelFile handle and passes it to all
    six transform calls - avoids reopening the file from disk for each sheet.
    Returns a single concatenated long-format DataFrame.
    """
    data_source, scenario_1 = parse_filename(path)
    log.info(f"  [{path.parent.name}] {path.name}")
    log.info(f"    data_source='{data_source}'  scenario_1='{scenario_1}'")

    frames = []
    # Open once, pass the handle to all sheet transforms
    with pd.ExcelFile(path) as xl:
        for sheet_name, variable in STATE_SHEETS:
            df = transform_sheet(xl, sheet_name, variable, data_source, scenario_1)
            if not df.empty:
                frames.append(df)

        for sheet_name, variable in REZ_SHEETS:
            df = transform_rez_sheet(xl, sheet_name, variable, data_source, scenario_1)
            if not df.empty:
                frames.append(df)

    if not frames:
        log.warning(f"  No data extracted from {path.name}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"    → {len(combined):,} total rows before load")
    return combined


# ---------------------------------------------------------------------------
# Duplicate reporting
# ---------------------------------------------------------------------------

def write_duplicate_report(report_dir: Path, file_tag: str, df: pd.DataFrame) -> int:
    """
    Check incoming long-format data for duplicate keys before insert.
    Writes a CSV report if duplicates are found.
    Returns count of duplicate key groups found.
    """
    keys = ['Data_source', 'Scenario_1', 'Scenario_2', 'State',
            'Region', 'Technology', 'Variable', 'Year']
    grp = (
        df.groupby(keys, dropna=False)
        .agg(n_rows=('Value', 'size'), n_distinct=('Value', lambda s: s.dropna().nunique()))
        .reset_index()
    )
    dupes = grp[grp['n_rows'] > 1]
    if not dupes.empty:
        report_dir.mkdir(parents=True, exist_ok=True)
        tag = re.sub(r'[^A-Za-z0-9_\-]+', '_', file_tag).strip('_')
        out_path = report_dir / f"{tag}__dupes.csv"
        dupes.to_csv(out_path, index=False)
        log.warning(f"  {len(dupes)} duplicate key group(s) — report: {out_path.name}")
    return len(dupes)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def _to_records(df: pd.DataFrame) -> list:
    """
    Convert DataFrame to sqlite-safe list of tuples (pandas NA -> None).
    Uses df.values.tolist() which is faster than to_numpy() + list comprehension,
    then replaces any remaining float nan with None for sqlite3 compatibility.
    """
    df = df.where(pd.notna(df), None)
    return df.values.tolist()


def _chunked_executemany(cur, sql: str, records: list) -> None:
    """
    Execute an INSERT in chunks of INSERT_CHUNK_ROWS to avoid SQLite page stalls
    on very large inserts (600k+ rows).
    """
    for i in range(0, len(records), INSERT_CHUNK_ROWS):
        cur.executemany(sql, records[i : i + INSERT_CHUNK_ROWS])


def load_data(conn: sqlite3.Connection, df_long: pd.DataFrame) -> None:
    """
    Insert transformed data into context, data, and non_annual_data tables.

    Strategy:
      1. Upsert unique context combinations (INSERT OR IGNORE - first write wins).
      2. Read context Ids back from DB filtered to this file's Data_source and
         Scenario_1 only - avoids full table scan which gets expensive by file 70.
      3. Split annual vs non-annual rows.
      4. Insert into data / non_annual_data in chunks (INSERT OR IGNORE).
      5. All inserts for a file are wrapped in a single explicit transaction.

    INSERT OR IGNORE means duplicates in source data are silently skipped
    at the DB level. The write_duplicate_report() call before this surfaces
    them explicitly in a CSV so they can be investigated.
    """
    if df_long.empty:
        log.warning("  No data to load.")
        return

    cur = conn.cursor()
    context_cols = ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology']

    # Single transaction for the entire file
    conn.execute("BEGIN")

    # Upsert context
    context_df = df_long[context_cols].drop_duplicates()
    context_df = context_df.where(pd.notna(context_df), None)
    _chunked_executemany(
        cur,
        """INSERT OR IGNORE INTO context
           (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
           VALUES (?, ?, ?, ?, ?, ?)""",
        _to_records(context_df),
    )

    # Read context Ids back - filtered to this file only, not the full table
    data_source = df_long['Data_source'].iloc[0]
    scenario_1  = df_long['Scenario_1'].iloc[0]
    ctx_db = pd.read_sql_query(
        "SELECT Id, Data_source, Scenario_1, Scenario_2, State, Region, Technology "
        "FROM context WHERE Data_source = ? AND Scenario_1 = ?",
        conn,
        params=(data_source, scenario_1),
    )
    merged = df_long.merge(ctx_db, on=context_cols, how='left')
    merged['Value'] = pd.to_numeric(merged['Value'], errors='coerce')

    data_df = merged[['Id', 'Variable', 'Year', 'Value']].copy()
    data_df = data_df.where(pd.notna(data_df), None)

    # Split annual vs non-annual
    is_annual    = data_df['Year'].astype(str).str.fullmatch(r'\d{4}')
    annual_df    = data_df[is_annual]
    nonannual_df = data_df[~is_annual]

    if not annual_df.empty:
        _chunked_executemany(
            cur,
            "INSERT OR IGNORE INTO data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            _to_records(annual_df),
        )
    if not nonannual_df.empty:
        _chunked_executemany(
            cur,
            "INSERT OR IGNORE INTO non_annual_data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            _to_records(nonannual_df),
        )

    conn.execute("COMMIT")
    log.info(f"  Loaded: {len(annual_df):,} annual, {len(nonannual_df):,} non-annual rows")


def populate_mapping(conn: sqlite3.Connection) -> None:
    """
    Populate the mapping table as a data dictionary of every distinct value
    for each attribute type (Scenario_1, Scenario_2, State, Region, Technology)
    per Data_source.

    For Technology, Original_value is the native name from the source xlsx and
    Standard_value is the standardised name after applying TECH_MAP.
    For all other attribute types, Original_value = Standard_value since no
    standardisation is applied to scenarios, states or regions.

    Uses INSERT OR IGNORE so the table is preserved across runs - new values
    are added, existing entries are not overwritten.
    """
    cur = conn.cursor()
    total = 0

    # Attribute types to catalogue - mirrors the original pipeline behaviour
    attribute_types = ['Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology']

    for attr in attribute_types:
        rows = cur.execute(
            f"SELECT DISTINCT Data_source, {attr} FROM context "
            f"WHERE {attr} IS NOT NULL"
        ).fetchall()

        entries = []
        for data_source, original_value in rows:
            # For Technology, Standard_value is the mapped name.
            # For all other attributes, Standard_value = Original_value.
            if attr == 'Technology':
                standard_value = TECH_MAP.get(original_value, original_value)
            else:
                standard_value = original_value
            entries.append((data_source, attr, original_value, standard_value))

        cur.executemany(
            "INSERT OR IGNORE INTO mapping "
            "(Data_source, Attribute_type, Original_value, Standard_value) "
            "VALUES (?, ?, ?, ?)",
            entries,
        )
        total += len(entries)

    conn.commit()
    log.info(f"Mapping table: {total} entries across {len(attribute_types)} attribute types")


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def report_row_counts(conn: sqlite3.Connection) -> None:
    """Print row counts for all tables after loading."""
    cur = conn.cursor()
    print("\n--- Row counts ---")
    for table in ["context", "data", "non_annual_data", "mapping"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:20s}  {cur.fetchone()[0]:>10,}")
    print()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run() -> None:
    log.info("=" * 60)
    log.info("ISP ingestion pipeline starting")
    log.info(f"Database : {DB_PATH}")
    log.info(f"Data dir : {DATA_DIR}")
    if DEBUG:
        log.warning(f"DEBUG MODE — processing dirs: {DEBUG_DIRS if DEBUG_DIRS else 'ALL'}")
    log.info("=" * 60)

    if not _test_parse_filename():
        log.error("Filename parser self-tests failed — aborting.")
        return

    global TECH_MAP, FILTERS
    TECH_MAP = load_tech_map(TECH_MAP_PATH)
    FILTERS  = load_filters(FILTER_DIR, TECH_MAP)

    conn = get_connection(DB_PATH)
    drop_tables(conn)
    create_tables(conn)

    files = find_xlsx_files(DATA_DIR)
    total_dupes = 0

    # DEBUG: no try/except — fail immediately on any unhandled error.
    # In production, wrap with per-file try/except to log and continue.
    for path in files:
        print(f"\n{'='*60}")
        print(f"[{path.parent.name}]  {path.name}")
        print(f"{'='*60}")

        df_long = transform_file(path)

        if not df_long.empty:
            file_tag = f"{path.parent.name} - {path.stem}"
            total_dupes += write_duplicate_report(REPORT_DIR, file_tag, df_long)
            load_data(conn, df_long)

    populate_mapping(conn)
    report_row_counts(conn)

    if total_dupes:
        log.warning(f"Total duplicate key groups across all files: {total_dupes} — see {REPORT_DIR}")
    log.info("Pipeline complete.")
    conn.close()


if __name__ == "__main__":
    run()