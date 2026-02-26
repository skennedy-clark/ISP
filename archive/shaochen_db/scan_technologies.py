"""
scan_technologies.py
====================
Scans all xlsx files under hidden_data/ and prints a table of:
  | file | sheet | technology |

for every distinct technology value found across all sheets.
Output is tab-separated to stdout so it can be piped to a file or
pasted into Excel/a spreadsheet for review.

Usage:
  python scan_technologies.py
  python scan_technologies.py > technology_scan.tsv
"""

import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR   = SCRIPT_DIR / "hidden_data"

# Sheets to scan and the column(s) that may hold technology names
# Listed in order of preference — first match wins
SHEETS = [
    "Capacity",
    "Generation",
    "Storage Capacity",
    "Storage Energy",
    "REZ Generation Capacity",
    "REZ Generation",
]

# Possible technology column names (case-insensitive match)
TECH_COL_CANDIDATES = ["technology", "storage category"]


def find_tech_col(df: pd.DataFrame) -> str | None:
    """Return the first column name that matches a known technology column."""
    lower_map = {c.lower().strip(): c for c in df.columns}
    for candidate in TECH_COL_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def scan_file(path: Path) -> list[tuple[str, str, str]]:
    """
    Return list of (relative_path, sheet, technology) tuples
    for all distinct technology values in all known sheets.
    """
    rel = path.relative_to(DATA_DIR)
    rows = []

    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        print(f"[WARN] Could not open {rel}: {e}", file=sys.stderr)
        return rows

    for sheet in SHEETS:
        if sheet not in xl.sheet_names:
            continue
        try:
            df = pd.read_excel(path, sheet_name=sheet)
            df = df.dropna(how="all")
            df.columns = [str(c).strip() for c in df.columns]

            tech_col = find_tech_col(df)
            if tech_col is None:
                print(f"[WARN] No technology column in {rel} / {sheet}", file=sys.stderr)
                continue

            techs = sorted(df[tech_col].dropna().astype(str).str.strip().unique())
            for tech in techs:
                rows.append((str(rel), sheet, tech))

        except Exception as e:
            print(f"[WARN] Error reading {rel} / {sheet}: {e}", file=sys.stderr)

    return rows


def main():
    if not DATA_DIR.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    files = sorted(
        p for p in DATA_DIR.rglob("*.xlsx")
        if not p.name.startswith("~$")
    )

    if not files:
        print(f"ERROR: No xlsx files found under {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {len(files)} files...", file=sys.stderr)

    # Header
    print("file\tsheet\ttechnology")

    all_rows = []
    for path in files:
        rows = scan_file(path)
        all_rows.extend(rows)
        print(f"  {path.relative_to(DATA_DIR)}: {len(rows)} rows", file=sys.stderr)

    for file, sheet, tech in all_rows:
        print(f"{file}\t{sheet}\t{tech}")

    print(f"\nTotal rows: {len(all_rows)}", file=sys.stderr)

    # Also print distinct technologies across all files/sheets for quick review
    print("\n--- Distinct technologies (all files, all sheets) ---", file=sys.stderr)
    distinct = sorted(set(tech for _, _, tech in all_rows))
    for t in distinct:
        print(f"  {t}", file=sys.stderr)
    print(f"Total distinct: {len(distinct)}", file=sys.stderr)


if __name__ == "__main__":
    main()