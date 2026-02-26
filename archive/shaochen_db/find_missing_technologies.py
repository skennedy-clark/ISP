"""
find_missing_technologies.py
============================
Scans all xlsx files under hidden_data/ for technology names, applies the
name standardisation map (name_map_technology.csv), then compares against
every CSV found recursively under input_csv/.

Reports technologies that appear in the source data but are not covered by
any filter CSV — these would be silently dropped by the pipeline.

Output is printed to stdout as a readable table.
Redirect to a file if you want to save it:
  python find_missing_technologies.py > missing_technologies.txt

Usage:
  python find_missing_technologies.py
"""

import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd

# ---------------------------------------------------------------------------
# Paths — all relative to this script's directory
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR   = SCRIPT_DIR / "hidden_data"
FILTER_DIR = SCRIPT_DIR / "input_csv"

# ---------------------------------------------------------------------------
# Sheet scanning config (same as main pipeline)
# ---------------------------------------------------------------------------
SHEETS = [
    "Capacity",
    "Generation",
    "Storage Capacity",
    "Storage Energy",
    "REZ Generation Capacity",
    "REZ Generation",
]

TECH_COL_CANDIDATES = ["technology", "storage category"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_tech_col(df: pd.DataFrame) -> str | None:
    """Return the first column name matching a known technology column."""
    lower_map = {c.lower().strip(): c for c in df.columns}
    for candidate in TECH_COL_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def load_tech_map() -> dict:
    """Load native_name -> standard_name from name_map_technology.csv."""
    path = FILTER_DIR / "name_map_technology.csv"
    if not path.exists():
        print(f"[WARN] name_map_technology.csv not found at {path} — "
              "names will not be standardised", file=sys.stderr)
        return {}
    df = pd.read_csv(path)
    return dict(zip(df["native_name"].str.strip(), df["standard_name"].str.strip()))


def load_all_filter_technologies() -> dict:
    """
    Recursively find all CSV files under FILTER_DIR (excluding
    name_map_technology.csv) and collect every technology name listed in them.

    Returns:
        dict mapping csv_filename -> set of raw technology strings found in it
    """
    result = {}
    for csv_path in sorted(FILTER_DIR.rglob("*.csv")):
        if csv_path.name == "name_map_technology.csv":
            continue
        try:
            df = pd.read_csv(csv_path)
            # Technology column is either named 'Technology' or is the first col
            tech_col = "Technology" if "Technology" in df.columns else df.columns[0]
            techs = (
                df[tech_col]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )
            result[csv_path.relative_to(FILTER_DIR)] = set(techs)
        except Exception as e:
            print(f"[WARN] Could not read {csv_path.name}: {e}", file=sys.stderr)
    return result


def scan_xlsx_technologies() -> dict:
    """
    Scan all xlsx files under DATA_DIR.

    Returns:
        dict mapping (data_source, sheet) -> set of raw technology strings
        Also builds a per-file breakdown stored in file_techs:
            (data_source, filename, sheet) -> set of raw technology strings
    """
    if not DATA_DIR.exists():
        print(f"ERROR: hidden_data/ not found at {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    files = sorted(
        p for p in DATA_DIR.rglob("*.xlsx")
        if not p.name.startswith("~$")
    )

    if not files:
        print(f"ERROR: No xlsx files found under {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {len(files)} xlsx files...", file=sys.stderr)

    # data_source -> sheet -> set of raw tech names
    by_vintage_sheet = defaultdict(lambda: defaultdict(set))
    # (data_source, file, sheet) -> set — for detailed report
    by_file_sheet = {}

    for path in files:
        data_source = path.parent.name + " ISP"
        try:
            xl = pd.ExcelFile(path)
        except Exception as e:
            print(f"[WARN] Could not open {path.name}: {e}", file=sys.stderr)
            continue

        for sheet in SHEETS:
            if sheet not in xl.sheet_names:
                continue
            try:
                df = pd.read_excel(path, sheet_name=sheet)
                df = df.dropna(how="all")
                df.columns = [str(c).strip() for c in df.columns]
                tech_col = find_tech_col(df)
                if tech_col is None:
                    continue
                techs = (
                    df[tech_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                )
                tech_set = set(techs)
                by_vintage_sheet[data_source][sheet] |= tech_set
                by_file_sheet[(data_source, path.name, sheet)] = tech_set
            except Exception as e:
                print(f"[WARN] {path.name} / {sheet}: {e}", file=sys.stderr)

    return by_vintage_sheet, by_file_sheet


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tech_map   = load_tech_map()
    filter_csvs = load_all_filter_technologies()

    if not filter_csvs:
        print("ERROR: No filter CSVs found under input_csv/", file=sys.stderr)
        sys.exit(1)

    # Build the combined set of all technology names across all filter CSVs
    # standardised via tech_map
    all_filtered_raw = set()
    for techs in filter_csvs.values():
        all_filtered_raw |= techs
    all_filtered_std = set(tech_map.get(t, t) for t in all_filtered_raw)

    by_vintage_sheet, by_file_sheet = scan_xlsx_technologies()

    # -------------------------------------------------------------------
    # Report 1: Filter CSV summary
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("FILTER CSVs FOUND")
    print("=" * 70)
    for csv_name, techs in sorted(filter_csvs.items()):
        std_techs = sorted(set(tech_map.get(t, t) for t in techs))
        print(f"\n  {csv_name}  ({len(std_techs)} technologies after standardisation)")
        for t in std_techs:
            print(f"    {t}")

    # -------------------------------------------------------------------
    # Report 2: Missing by Data_source × sheet
    # -------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TECHNOLOGIES IN XLSX BUT NOT IN ANY FILTER CSV")
    print("(after name standardisation)")
    print("=" * 70)

    grand_missing = set()
    any_missing = False

    for data_source in sorted(by_vintage_sheet.keys()):
        data_source_missing = set()
        for sheet in SHEETS:
            raw_techs = by_vintage_sheet[data_source].get(sheet, set())
            if not raw_techs:
                continue
            std_techs = set(tech_map.get(t, t) for t in raw_techs)
            missing = std_techs - all_filtered_std
            if missing:
                any_missing = True
                data_source_missing |= missing
                grand_missing |= missing
                print(f"\n  [{data_source}] {sheet}")
                for t in sorted(missing):
                    print(f"    MISSING: {t}")

    if not any_missing:
        print("\n  ✓ All technologies in xlsx files are covered by filter CSVs.")

    # -------------------------------------------------------------------
    # Report 3: Grand summary
    # -------------------------------------------------------------------
    if grand_missing:
        print("\n" + "=" * 70)
        print("SUMMARY — DISTINCT MISSING TECHNOLOGIES (ALL Data_sources)")
        print("=" * 70)
        for t in sorted(grand_missing):
            # Show which Data_source labels it appears in
            data_sources_found = []
            for data_source in sorted(by_vintage_sheet.keys()):
                all_std = set(
                    tech_map.get(r, r)
                    for sheet_techs in by_vintage_sheet[data_source].values()
                    for r in sheet_techs
                )
                if t in all_std:
                    data_sources_found.append(data_source)
            print(f"  {t:<45} data_sources: {', '.join(data_sources_found)}")

        print(f"\n  Total missing: {len(grand_missing)} technology(s)")
        print("\n  ACTION: Add these to the appropriate filter CSV(s), or create")
        print("  per-Data_source filter CSVs if scope differs by Data_source.")

    # -------------------------------------------------------------------
    # Report 4: Technologies in filters not found in any xlsx
    #           (stale/dead entries in the filter CSVs)
    # -------------------------------------------------------------------
    all_in_xlsx_std = set(
        tech_map.get(t, t)
        for sheet_techs in by_vintage_sheet.values()
        for techs in sheet_techs.values()
        for t in techs
    )
    stale = all_filtered_std - all_in_xlsx_std
    print("\n" + "=" * 70)
    print("STALE ENTRIES — IN FILTER CSVs BUT NOT FOUND IN ANY XLSX")
    print("(may indicate old technology names that have been renamed)")
    print("=" * 70)
    if stale:
        for t in sorted(stale):
            # which CSV(s) contain it?
            in_csvs = [
                str(csv_name)
                for csv_name, techs in filter_csvs.items()
                if tech_map.get(t, t) in set(tech_map.get(r, r) for r in techs)
                or t in techs
            ]
            print(f"  {t:<45} in: {', '.join(in_csvs)}")
    else:
        print("  ✓ No stale entries found.")

    print()


if __name__ == "__main__":
    main()
