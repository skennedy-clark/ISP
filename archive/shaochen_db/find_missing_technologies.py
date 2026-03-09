"""
create_db.py
============
ISP (Integrated System Plan) modelling results ingestion pipeline.

Pipeline overview
-----------------
Ingests AEMO ISP modelling results from pre-processed xlsx files into a local
SQLite database following a standard ETL pattern:

  Extract   - recursively discover xlsx files from hidden_data/
  Transform - parse filename metadata, read named sheets, melt year columns
              to long format, normalise year labels, split annual vs non-annual
  Load      - insert into ISP_HOME.db: context, data, non_annual_data, mapping

Prerequisites
-------------
Run read_hidden.py first. It opens the original AEMO workbooks (which have most
sheets hidden), extracts the six data sheets, and writes clean copies into
hidden_data/. This script only reads from hidden_data/, not from the originals.

Directory structure (all paths relative to this script):
  hidden_data/        - pre-processed xlsx files, one subfolder per Data_source
      2022 Draft/
      2022 Final/
      2024 Draft/
      2024 Final/
      2026 Draft/
  input_csv/          - filter and mapping CSVs (see Input files section below)
  results/            - pipeline outputs (duplicate reports etc)

Adding a new Data_source (e.g. a future ISP release)
-----------------------------------------------------
  1. Create a new subfolder under hidden_data/ named "<year> <Draft|Final>".
     The folder name becomes the Data_source label ("<year> <Draft|Final> ISP").
  2. Place the pre-processed xlsx files (output of read_hidden.py) in that folder.
  3. Check the xlsx sheet names match those in STATE_SHEETS and REZ_SHEETS below.
     If AEMO have renamed sheets, update those lists.
  4. Run find_missing_technologies.py to identify any new technologies in the
     workbooks that are not yet in the filter CSVs. Add them to the relevant
     data_req_*.csv files before running the pipeline.
  5. Check name_map_technology.csv - if the new release uses different technology
     name spellings, add entries so they map to the existing standard names.
  6. Re-run this script. The new Data_source will be picked up automatically.

Input files (input_csv/)
------------------------
  name_map_technology.csv       - maps native xlsx technology names to standard names
                                  columns: Native_name, preferred_name, standard_name
                                  edit when AEMO renames a technology between releases

  data_req_state_capacity.csv   - allowlist of technologies from the Capacity sheet
  data_req_state_generation.csv - allowlist from the Generation sheet
  data_req_state_storage.csv    - allowlist from Storage Capacity and Storage Energy
  data_req_rez.csv              - allowlist from REZ Generation Capacity and REZ Generation

  These filter CSVs are flat lists (Technology column, one row per technology).
  The state columns (NSW, QLD etc) with 'Y' values are legacy format from the
  original pipeline - only the Technology column is read, not the state columns.
  Technologies are applied across all states.

  If a technology appears in a workbook but not in the relevant filter CSV it
  will be dropped with a WARNING. Run find_missing_technologies.py before each
  run to audit coverage and update the CSVs if needed.

Sheets ingested per file
------------------------
  Sheet                    Variable           State col    Region col
  -----------------------  -----------------  -----------  ----------------------
  Capacity                 capacity           Region       None
  Generation               generation         Region       None
  Storage Capacity         storage capacity   Region       None
  Storage Energy           storage energy     Region       None
  REZ Generation Capacity  capacity           Region       REZ (zone code)
  REZ Generation           generation         Region       REZ (zone code)

Column conventions (consistent across 2022-2026 Data_sources)
--------------------------------------------------------------
  CDP          -> Scenario_2
  Region       -> State        (state abbreviation: NSW, QLD, VIC, SA, TAS)
  Subregion    -> Region       (sub-state code: NNSW, CNSW etc; 2024 Final and
                                2026 Draft only - absent in earlier releases)
  REZ          -> Region       (REZ zone code: N1, Q3, O2 etc; REZ sheets only)
  REZ Name     -> dropped
  Technology   -> Technology   (standardised via name_map_technology.csv)
  Storage cat. -> Technology   (Storage Capacity sheet uses this column name)

Year column normalisation
-------------------------
  Float/int year   2024.0 / 2024  -> "2024"
  FY label         "2024-25"      -> "2025"  (end year of the financial year)
  Non-annual       "Existing and Committed" -> non_annual_data table
  Unrecognised     -> dropped with a WARNING

Database schema
---------------
  context        - one row per unique combination of Data_source, Scenario_1,
                   Scenario_2, State, Region, Technology. Primary key is Id.
  data           - annual values: Id, Variable, Year, Value
  non_annual_data - non-annual values (Existing and Committed): same structure
  mapping        - data dictionary: every distinct value for each attribute type
                   (Scenario_1, Scenario_2, State, Region, Technology) per
                   Data_source. Useful for building query filters in analysis scripts.

Duplicate handling
------------------
  context uses INSERT OR IGNORE (first write wins, no overwriting).
  data and non_annual_data use INSERT OR IGNORE.
  Any duplicate key in incoming data is written to a per-file CSV in
  results/db_build_reports/ for investigation.

After running
-------------
  The pipeline opens the database with WAL journal mode for write performance.
  Once the run is complete, switch back to the default journal mode to avoid
  the -wal and -shm sidecar files appearing in the directory:

    PRAGMA journal_mode=DELETE;

  Run this in DBeaver or any SQLite client after the pipeline finishes.
  No VACUUM is needed - SQLite handles the WAL cleanup automatically during
  the mode switch.

Known data issues
-----------------
  Storage duplication: Coordinated DER Storage and Distributed Storage appear
  on both the Capacity sheet (variable='capacity') and the Storage Capacity
  sheet (variable='storage capacity') with identical values. Both are loaded.
  Analysis scripts should filter by Variable appropriately.

TODO (open items)
-----------------
  1. Incremental loads - currently drops and recreates all tables on each run.
     Extend to preserve existing data and only load new files.
"""

# ---------------------------------------------------------------------------
# Configuration - change DB_NAME when moving between environments.
# ---------------------------------------------------------------------------
DB_NAME = "ISP.db"

# ---------------------------------------------------------------------------
# Debug mode - controls which Data_source folders are processed.
#
# DEBUG = True  : only process folders named in DEBUG_DIRS.
#                 Set DEBUG_DIRS = [] to process all folders in debug mode.
# DEBUG = False : process all files (production mode).
#
# Set DEBUG = False before running a full production build.
# ---------------------------------------------------------------------------
DEBUG = False
DEBUG_DIRS = []   # e.g. ["2026 Draft"] to test a single Data_source

import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Paths - all derived from script location, no hardcoding needed.
# ---------------------------------------------------------------------------
SCRIPT_DIR    = Path(__file__).parent.resolve()
DB_PATH       = SCRIPT_DIR / DB_NAME
DATA_DIR      = SCRIPT_DIR / "hidden_data"
REPORT_DIR    = SCRIPT_DIR / "results" / "db_build_reports"
TECH_MAP_PATH = SCRIPT_DIR / "input_csv" / "name_map_technology.csv"
FILTER_DIR    = SCRIPT_DIR / "input_csv"
LOG_PATH      = SCRIPT_DIR / f"error_{datetime.now().strftime('%Y%m%d')}.log"

# ---------------------------------------------------------------------------
# Logging - console output always on; file only created on first ERROR.
# The log file from any previous run is deleted when a new ERROR is written.
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
      "YYYY"                   - for annual years (int, float, or FY string)
      "Existing and Committed" - for the non-annual label
      None                     - for anything unrecognised (row will be dropped)

    FY labels take the end year: "2024-25" -> "2025".
    Float years from Excel: 2024.0 -> "2024".
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
        return str(int(m.group(1)) + 1)   # end year of FY

    m = _YEAR_RE.match(s)
    if m:
        return str(int(m.group(1)))

    log.warning(f"  Unrecognised year column '{val}' - dropped")
    return None


def _year_cols(df: pd.DataFrame, exclude: List[str] = None) -> List[str]:
    """
    Return column names that normalise to a valid year or non-annual label.
    Pass exclude= to skip known id columns and avoid spurious warnings for
    non-year columns like Technology, State etc.
    """
    skip = set(exclude or [])
    return [c for c in df.columns if c not in skip and normalise_year(c) is not None]


# ---------------------------------------------------------------------------
# Sheet definitions
# To add a new sheet type: add a (sheet_name, variable_label) tuple to the
# appropriate list, and ensure the sheet's column structure is handled by
# the relevant transform function.
# ---------------------------------------------------------------------------

# State-level sheets: CDP->Scenario_2, Region->State, Subregion->Region (if present)
# The Storage Capacity 'storage category' column is renamed to 'Technology'
# inside transform_sheet().
STATE_SHEETS = [
    ("Capacity",         "capacity"),
    ("Generation",       "generation"),
    ("Storage Capacity", "storage capacity"),
    ("Storage Energy",   "storage energy"),
]

# REZ-level sheets: CDP->Scenario_2, Region->State, REZ->Region, REZ Name->dropped
REZ_SHEETS = [
    ("REZ Generation Capacity", "capacity"),
    ("REZ Generation",          "generation"),
]

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------

def drop_tables(conn: sqlite3.Connection) -> None:
    """
    Drop transient tables before each full rebuild.

    mapping is preserved across runs (it accumulates the data dictionary),
    but is dropped and recreated if its schema does not match the expected
    columns. This handles the case where an old schema is present in the db
    from a previous version of this script.

    TODO: when implementing incremental loads, remove the drops for context,
    data, and non_annual_data and instead check for existing rows before insert.
    """
    cur = conn.cursor()
    for t in ["context", "data", "non_annual_data"]:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        log.info(f"Dropped table: {t}")

    # Drop mapping only if its columns don't match - preserves data on normal re-runs
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
    """Create all tables. mapping uses IF NOT EXISTS so existing entries survive re-runs."""
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

    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_context_with_region AS 
            SELECT
                c.Id,
                c.Data_source,
                c.Scenario_1,
                c.Scenario_2,
                c.State,
                CASE
                    WHEN c.Region IS NOT NULL THEN c.Region
                    ELSE CASE c.State
                        WHEN 'NSW' THEN 'N0'
                        WHEN 'QLD' THEN 'Q0'
                        WHEN 'VIC' THEN 'V0'
                        WHEN 'SA'  THEN 'S0'
                        WHEN 'TAS' THEN 'T0'
                        ELSE NULL
                    END
                END AS Region,
                c.Technology
            FROM context c;
    """)
    log.info("Created view: v_context_with_region")

    conn.commit()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Open or create the SQLite database.
    WAL journal mode improves write performance during the pipeline run.
    After the pipeline completes, run PRAGMA journal_mode=DELETE in DBeaver
    to switch back to the default mode and remove the -wal and -shm sidecar files.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    log.info(f"Connected to database: {db_path}")
    return conn


# ---------------------------------------------------------------------------
# Extract - file discovery
# ---------------------------------------------------------------------------

def find_xlsx_files(data_dir: Path) -> List[Path]:
    """
    Recursively find all xlsx files under data_dir, sorted alphabetically.
    Skips Excel temporary lock files (~$filename.xlsx).
    In DEBUG mode, restricts to folders named in DEBUG_DIRS.
    """
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    files = sorted(
        p for p in data_dir.rglob("*.xlsx")
        if not p.name.startswith("~$")
    )

    if DEBUG and DEBUG_DIRS:
        files = [f for f in files if f.parent.name in DEBUG_DIRS]
        log.warning(f"DEBUG MODE: filtering to {DEBUG_DIRS} - {len(files)} file(s)")
    elif DEBUG and not DEBUG_DIRS:
        log.warning("DEBUG MODE: DEBUG_DIRS=[] so processing all directories")

    return files


# ---------------------------------------------------------------------------
# Extract - filename parsing
# ---------------------------------------------------------------------------

def parse_filename(path: Path) -> Tuple[str, str]:
    """
    Derive Data_source and Scenario_1 from the file path.

    Data_source: parent directory name + ' ISP'
      "2022 Draft" -> "2022 Draft ISP"
      "2026 Final" -> "2026 Final ISP"

    Scenario_1: everything after the first ' - ' separator in the filename stem.
      "2022 Draft ISP results workbook - Step Change - low gas prices.xlsx"
          -> "Step Change - low gas prices"
      "2024 ISP - Step Change - Core.xlsx"
          -> "Step Change - Core"
      "2026 ISP - Accelerated Transition - Core.xlsx"
          -> "Accelerated Transition - Core"

    Logs a warning and uses the full stem if no ' - ' separator is found.
    """
    data_source = path.parent.name + " ISP"
    stem = path.stem
    idx  = stem.find(" - ")
    scenario_1 = stem[idx + 3:] if idx != -1 else stem
    if idx == -1:
        log.warning(f"No ' - ' separator in filename: {path.name}")
    return data_source, scenario_1


def _test_parse_filename() -> bool:
    """
    Self-tests for parse_filename(). Runs automatically at pipeline startup.
    Add a new test case here whenever a new filename pattern is encountered.
    """
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
    """
    Load Native_name -> standard_name mapping from CSV. The preferred_name column is ignored.
    Returns an empty dict if the file is missing (no standardisation applied).
    """
    if not path.exists():
        log.warning(
            f"Technology map not found: {path} - names will not be standardised"
        )
        return {}
    df = pd.read_csv(path)
    mapping = dict(zip(df["Native_name"].str.strip(), df["standard_name"].str.strip()))
    log.info(f"Technology map loaded: {len(mapping)} entries from {path.name}")
    return mapping


# Module-level - populated in run() before any transforms run.
TECH_MAP: dict = {}


def load_filters(filter_dir: Path, tech_map: dict) -> dict:
    """
    Load technology allowlists from data_req_*.csv files.

    Technology names in the CSVs are standardised via tech_map before use
    so they match the standardised names produced by the transform functions.

    Returns a dict keyed by variable label:
      "capacity"         -> set of allowed Technology strings (state sheets)
      "generation"       -> set of allowed Technology strings (state sheets)
      "storage capacity" -> set of allowed Technology strings (storage sheets)
      "storage energy"   -> same allowlist as storage capacity
      "rez"              -> set of allowed Technology strings (REZ sheets)

    Technologies in the workbooks that are not in the relevant filter set will
    be dropped and logged as WARNINGs. Run find_missing_technologies.py before
    each run to audit coverage and update the CSVs if needed.
    """
    def _load(filename: str, tech_col: str) -> set:
        path = filter_dir / filename
        if not path.exists():
            log.warning(f"Filter file not found: {path} - no filtering applied")
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


# Module-level - populated in run() before any transforms run.
FILTERS: dict = {}


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _read_sheet(path: Path, sheet_name: str) -> Optional[pd.DataFrame]:
    """
    Read a named sheet from an xlsx file.
    Strips whitespace from column names and drops entirely empty rows.
    Returns None if the sheet does not exist.
    """
    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
        df = df.dropna(how='all')
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        log.warning(f"  Sheet '{sheet_name}' not found in {path.name}: {e}")
        return None


def _clean_str_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Strip whitespace from known string id columns and replace blank/nan with None."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
            df.loc[df[c].isin(["", "nan", "NaN", "<NA>"]), c] = None
    return df


def transform_sheet(
    path: Path,
    sheet_name: str,
    variable: str,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    """
    Read one state-level sheet (Capacity, Generation, Storage Capacity, or
    Storage Energy) and return a long-format DataFrame.

    Output columns:
      Data_source, Scenario_1, Scenario_2, State, Region,
      Technology, Variable, Year, Value

    Column mapping:
      CDP       -> Scenario_2
      Region    -> State       (state abbreviation: NSW, QLD etc)
      Subregion -> Region      (sub-state code e.g. NNSW; 2024 Final and 2026 Draft
                                only - absent in earlier releases, set to None)
      Technology -> Technology (standardised via TECH_MAP)
      Storage category -> Technology (Storage Capacity sheet uses this column name)
    """
    df = _read_sheet(path, sheet_name)
    if df is None:
        return pd.DataFrame()

    df = _clean_str_cols(df, ['CDP', 'Region', 'Subregion', 'Technology',
                               'Storage category', 'storage category'])

    # Rename 'storage category' to 'Technology', then apply name standardisation
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

    df = df.rename(columns={'CDP': 'Scenario_2', 'Region': 'State'})

    # Subregion present in 2024 Final and 2026 Draft - absent in earlier releases
    if 'Subregion' in df.columns:
        df = df.rename(columns={'Subregion': 'Region'})
    else:
        df['Region'] = None

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

    melted['Year']        = melted['Year'].apply(normalise_year)
    melted['Data_source'] = data_source
    melted['Scenario_1']  = scenario_1
    melted['Variable']    = variable

    melted = melted.dropna(subset=['Year'])

    # Drop technologies not in the allowlist for this variable
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
    path: Path,
    sheet_name: str,
    variable: str,
    data_source: str,
    scenario_1: str,
) -> pd.DataFrame:
    """
    Read one REZ-level sheet (REZ Generation Capacity or REZ Generation)
    and return a long-format DataFrame.

    Column mapping:
      CDP      -> Scenario_2
      Region   -> State     (state abbreviation: NSW, QLD etc)
      REZ      -> Region    (zone code: N1, Q3, O2 etc)
      REZ Name -> dropped
      Technology -> standardised via TECH_MAP
    """
    df = _read_sheet(path, sheet_name)
    if df is None:
        return pd.DataFrame()

    df = _clean_str_cols(df, ["CDP", "Region", "REZ", "Technology"])

    if "Technology" in df.columns and TECH_MAP:
        df["Technology"] = (
            df["Technology"]
            .astype(str).str.strip()
            .map(lambda x: TECH_MAP.get(x, x))
        )

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

    melted["Year"]        = melted["Year"].apply(normalise_year)
    melted["Data_source"] = data_source
    melted["Scenario_1"]  = scenario_1
    melted["Variable"]    = variable
    melted = melted.dropna(subset=["Year"])

    # Drop technologies not in the REZ allowlist
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
    Extract and transform all six sheets from one xlsx file.
    Returns a single concatenated long-format DataFrame.
    """
    data_source, scenario_1 = parse_filename(path)
    log.info(f"  [{path.parent.name}] {path.name}")
    log.info(f"    data_source='{data_source}'  scenario_1='{scenario_1}'")

    frames = []
    for sheet_name, variable in STATE_SHEETS:
        df = transform_sheet(path, sheet_name, variable, data_source, scenario_1)
        if not df.empty:
            frames.append(df)

    for sheet_name, variable in REZ_SHEETS:
        df = transform_rez_sheet(path, sheet_name, variable, data_source, scenario_1)
        if not df.empty:
            frames.append(df)

    if not frames:
        log.warning(f"  No data extracted from {path.name}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"    -> {len(combined):,} total rows before load")
    return combined


# ---------------------------------------------------------------------------
# Duplicate reporting
# ---------------------------------------------------------------------------

def write_duplicate_report(report_dir: Path, file_tag: str, df: pd.DataFrame) -> int:
    """
    Check incoming long-format data for duplicate keys before insert.
    Writes a CSV to results/db_build_reports/ if any duplicates are found.
    Returns the count of duplicate key groups.
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
        log.warning(f"  {len(dupes)} duplicate key group(s) - report: {out_path.name}")
    return len(dupes)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def _to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to sqlite-safe list of tuples (pandas NA -> None)."""
    df = df.where(pd.notna(df), None)
    return [tuple(r) for r in df.to_numpy()]


def load_data(conn: sqlite3.Connection, df_long: pd.DataFrame) -> None:
    """
    Insert one file's transformed data into context, data, and non_annual_data.

    Strategy:
      1. Upsert context rows (INSERT OR IGNORE - first write wins).
      2. Read all context Ids back from DB and join onto df_long to resolve Ids.
      3. Split annual (4-digit year) vs non-annual rows.
      4. Insert into data and non_annual_data (INSERT OR IGNORE).

    INSERT OR IGNORE means source duplicates are silently skipped at the DB level.
    write_duplicate_report() surfaces them in a CSV before this function is called.
    """
    if df_long.empty:
        log.warning("  No data to load.")
        return

    cur = conn.cursor()
    context_cols = ['Data_source', 'Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology']

    # Insert unique context combinations
    context_df = df_long[context_cols].drop_duplicates()
    context_df = context_df.where(pd.notna(context_df), None)
    cur.executemany(
        """INSERT OR IGNORE INTO context
           (Data_source, Scenario_1, Scenario_2, State, Region, Technology)
           VALUES (?, ?, ?, ?, ?, ?)""",
        _to_records(context_df),
    )

    # Read all context Ids back and join to resolve the Id for each data row
    ctx_db = pd.read_sql_query(
        "SELECT Id, Data_source, Scenario_1, Scenario_2, State, Region, Technology FROM context",
        conn,
    )
    merged = df_long.merge(ctx_db, on=context_cols, how='left')
    merged['Value'] = pd.to_numeric(merged['Value'], errors='coerce')

    data_df = merged[['Id', 'Variable', 'Year', 'Value']].copy()
    data_df = data_df.where(pd.notna(data_df), None)

    is_annual    = data_df['Year'].astype(str).str.fullmatch(r'\d{4}')
    annual_df    = data_df[is_annual]
    nonannual_df = data_df[~is_annual]

    if not annual_df.empty:
        cur.executemany(
            "INSERT OR IGNORE INTO data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            _to_records(annual_df),
        )
    if not nonannual_df.empty:
        cur.executemany(
            "INSERT OR IGNORE INTO non_annual_data (Id, Variable, Year, Value) VALUES (?, ?, ?, ?)",
            _to_records(nonannual_df),
        )

    conn.commit()
    log.info(f"  Loaded: {len(annual_df):,} annual, {len(nonannual_df):,} non-annual rows")


def populate_mapping(conn: sqlite3.Connection) -> None:
    """
    Populate the mapping table as a data dictionary after all files are loaded.

    Records every distinct value for each attribute type (Scenario_1, Scenario_2,
    State, Region, Technology) per Data_source. Useful for analysis scripts that
    need to know what valid values exist for a given Data_source.

    For Technology: Original_value is the native xlsx name, Standard_value is
    the standardised name after TECH_MAP is applied - providing an audit trail
    of any name standardisation that occurred.

    For all other attributes: Original_value = Standard_value (no standardisation
    is applied to scenarios, states or regions).

    Uses INSERT OR IGNORE so the table accumulates across runs without overwriting.
    """
    cur = conn.cursor()
    total = 0

    attribute_types = ['Scenario_1', 'Scenario_2', 'State', 'Region', 'Technology']

    for attr in attribute_types:
        rows = cur.execute(
            f"SELECT DISTINCT Data_source, {attr} FROM context "
            f"WHERE {attr} IS NOT NULL"
        ).fetchall()

        entries = []
        for data_source, original_value in rows:
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
    """Print row counts for all four tables after the pipeline completes."""
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
        log.warning(f"DEBUG MODE - processing dirs: {DEBUG_DIRS if DEBUG_DIRS else 'ALL'}")
    log.info("=" * 60)

    if not _test_parse_filename():
        log.error("Filename parser self-tests failed - aborting.")
        return

    global TECH_MAP, FILTERS
    TECH_MAP = load_tech_map(TECH_MAP_PATH)
    FILTERS  = load_filters(FILTER_DIR, TECH_MAP)

    conn = get_connection(DB_PATH)
    drop_tables(conn)
    create_tables(conn)

    files = find_xlsx_files(DATA_DIR)
    total_dupes = 0

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
        log.warning(f"Total duplicate key groups across all files: {total_dupes} - see {REPORT_DIR}")
    log.info("Pipeline complete.")
    conn.close()


if __name__ == "__main__":
    run()