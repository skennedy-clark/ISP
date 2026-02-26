"""
read_hidden.py
==============
Pre-processing step that must be run BEFORE create_db.py.

Purpose
-------
The original AEMO workbooks have most of their data sheets hidden. This script
opens each workbook, reads the six relevant sheets (unhiding them in the
process via openpyxl under the hood), and writes clean copies into hidden_data/
with only those six sheets — no other content, no hidden state.

The output files in hidden_data/ are what create_db.py ingests.

Directory structure (all paths relative to this script):
  data/           — original AEMO workbooks, organised by vintage subfolder
  hidden_data/    — output directory, same subfolder structure as data/

Usage
-----
  1. Drop original AEMO xlsx files into the appropriate data/<vintage>/ folder.
  2. Run this script once:  python read_hidden.py
  3. Run the pipeline:      python create_db.py

To process a subset of vintages, edit the SKIP_FOLDERS set below.
"""

import os
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths — derived from this script's location so no hardcoding required
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()

# Source: original AEMO workbooks (may have hidden sheets)
SOURCE_DIR = SCRIPT_DIR / "data"

# Destination: clean copies with only the six data sheets visible
OUTPUT_DIR = SCRIPT_DIR / "hidden_data"

# ---------------------------------------------------------------------------
# Control which vintage folders to (re)process.
# Add a folder name here to skip it — useful when only a new data_source has
# arrived and you don't want to re-process everything.
# Set to an empty set to process all folders: SKIP_FOLDERS = set()
# ---------------------------------------------------------------------------
SKIP_FOLDERS = {"2022 Draft", "2022 Final", "2024 Draft", "2024 Final"}

# ---------------------------------------------------------------------------
# Sheets to extract from each workbook (order preserved in output file)
# ---------------------------------------------------------------------------
SHEETS = [
    "Capacity",
    "Generation",
    "Storage Capacity",
    "Storage Energy",
    "REZ Generation Capacity",
    "REZ Generation",
]


def process_file(src_path: Path, dst_path: Path) -> None:
    """
    Read the six data sheets from src_path and write them to dst_path.
    Skips entirely empty rows (dropna how='all') so the output is clean.
    skiprows=2 strips the two decorative header rows AEMO uses above the
    actual column headers in the original workbooks.
    """
    print(f"  reading  {src_path.name}")

    sheets = {}
    for sheet in SHEETS:
        df = pd.read_excel(src_path, sheet_name=sheet, skiprows=2)
        df = df.dropna(how="all")
        sheets[sheet] = df

    # Ensure the destination subfolder exists before writing
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"  writing  {dst_path.name}")
    with pd.ExcelWriter(dst_path) as writer:
        for sheet, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet, index=False)


def main():
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(f"Source directory not found: {SOURCE_DIR}")

    # Discover data_source tage subfolders inside data/
    folders = sorted(
        f for f in SOURCE_DIR.iterdir()
        if f.is_dir() and not f.name.startswith(".")
    )

    if not folders:
        print(f"No subfolders found under {SOURCE_DIR}")
        return

    for folder in folders:
        if folder.name in SKIP_FOLDERS:
            print(f"\n[{folder.name}]  skipped (in SKIP_FOLDERS)")
            continue

        files = sorted(
            f for f in folder.iterdir()
            if f.suffix == ".xlsx" and not f.name.startswith("~$")
        )

        if not files:
            print(f"\n[{folder.name}]  no xlsx files found — skipping")
            continue

        print(f"\n[{folder.name}]  {len(files)} file(s)")

        for src_path in files:
            dst_path = OUTPUT_DIR / folder.name / src_path.name
            process_file(src_path, dst_path)

    print("\nDone.")


if __name__ == "__main__":
    main()