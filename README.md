
# ISP Database Pipeline

Builds a local SQLite database from AEMO Integrated System Plan (ISP) modelling
result workbooks. Covers 2022 Draft, 2022 Final, 2024 Draft, 2024 Final, and
2026 Draft releases, and is designed to extend as future releases are published.

---

## Contents

- [Background](#background)
- [Requirements](#requirements)
- [Directory structure](#directory-structure)
- [Running the pipeline](#running-the-pipeline)
- [Adding a new ISP release](#adding-a-new-isp-release)
- [Input files](#input-files)
- [Database schema](#database-schema)
- [Querying the database](#querying-the-database)
- [Known data issues](#known-data-issues)
- [Validation tools](#validation-tools)
- [Scripts reference](#scripts-reference)

---

## Background

AEMO publishes ISP modelling results as Excel workbooks with most data sheets
hidden. The pipeline has two stages:

**Stage 1 - read_hidden.py** extracts the hidden sheets and writes clean copies
into `hidden_data/`. This only needs to be run once when a new release arrives.
The original workbooks in `data/` are never modified.

**Stage 2 - create_db.py** reads the pre-processed files, transforms each sheet
into long format, standardises technology names, filters to the required
technologies, and loads everything into `ISP_HOME.db`.

The database stores modelling results across all scenarios, CDPs, states,
subregions, REZ zones, technologies, variables, and years from all loaded
releases. Analysis scripts and DBeaver queries then work from this single file.

---

## Requirements

Python 3.10 or later. Install dependencies:

```
pip install pandas openpyxl
```

SQLite is part of the Python standard library - no database server required.

---

## Directory structure

```
project root/
|
|- create_db.py                  main pipeline - builds ISP_HOME.db
|- read_hidden.py                pre-processing - extracts hidden sheets
|- find_missing_technologies.py  audit tool - checks filter CSV coverage
|- compare_databases.py          validation tool - compares two databases
|- compare_supply_annual.sql     SQL queries for comparing against Supply-annual.db
|- isp_plots.py                  plotting script - reads from ISP_HOME.db
|- ISP.db                        output database (created by create_db.py)
|- error_YYYYMMDD.log            error log (only created when errors occur)
|
|- data/                         original AEMO workbooks (read-only)
|   |- 2022 Draft/
|   |- 2022 Final/
|   |- 2024 Draft/
|   |- 2024 Final/
|   |- 2026 Draft/
|
|- hidden_data/                  pre-processed xlsx files (output of read_hidden.py)
|   |- 2022 Draft/
|   |- 2022 Final/
|   |- 2024 Draft/
|   |- 2024 Final/
|   |- 2026 Draft/
|
|- input_csv/                    filter and mapping CSVs
|   |- name_map_technology.csv
|   |- data_req_state_capacity.csv
|   |- data_req_state_generation.csv
|   |- data_req_state_storage.csv
|   |- data_req_rez.csv
|
|- results/
    |- db_build_reports/         per-file duplicate key reports (created on demand)
```

---

## Running the pipeline

### Step 1 - Pre-process the workbooks

Run once for each new ISP release. Reads from `data/` and writes clean copies of
the six data sheets into `hidden_data/`.

```
python read_hidden.py
```

To skip vintages already processed, edit the `SKIP_FOLDERS` set near the top of
`read_hidden.py`.

### Step 2 - Check for new technologies

Before running the main pipeline against a new release, check whether any new
technology names appear that are not yet in the filter CSVs:

```
python find_missing_technologies.py
```

Any names printed as missing need to be added to the relevant `data_req_*.csv`
before proceeding. Technologies absent from the filter CSVs are dropped at load
time with a WARNING - they will not silently end up in the database.

### Step 3 - Run the pipeline

```
python create_db.py
```

Progress is printed for each file. A row count summary prints at the end. Any
warnings are written to the console. Errors are also written to
`error_YYYYMMDD.log` in the project root - this file is only created when errors
occur and is replaced on the next run that produces errors.

A full run across all five releases takes approximately 15-30 minutes depending
on hardware.

### Step 4 - Reset the journal mode

The pipeline uses WAL journal mode for write performance. After the run
completes, run the following in DBeaver to switch back to the default mode.
This removes the `-wal` and `-shm` sidecar files that WAL mode creates:

```sql
PRAGMA journal_mode=DELETE;
```

No VACUUM is needed - SQLite handles the WAL cleanup as part of the mode switch.

---

## Adding a new ISP release

1. Create a subfolder under `data/` named `<year> <Draft|Final>` - for example
   `2028 Draft`. This folder name becomes the `Data_source` label in the
   database (`2028 Draft ISP`).

2. Place the original AEMO workbooks in that folder.

3. Run `read_hidden.py` with `SKIP_FOLDERS` set to skip already-processed
   releases so only the new folder is processed.

4. Run `find_missing_technologies.py` and add any new technology names to the
   relevant `data_req_*.csv` files.

5. Check `input_csv/name_map_technology.csv`. If the new release uses different
   spellings for existing technologies, add entries mapping them to the existing
   standard names. The pipeline will warn about any names it cannot map.

6. Check the sheet names in the new workbooks against `STATE_SHEETS` and
   `REZ_SHEETS` near the top of `create_db.py`. AEMO has changed sheet names
   between releases before. Update those lists if needed.

7. Run `create_db.py`. The new release will be picked up automatically.

---

## Input files

All input CSVs live in `input_csv/`.

### name_map_technology.csv

Maps native technology names from the xlsx files to standardised names stored in
the database. Columns: `native_name`, `standard_name`.

AEMO occasionally changes technology name spellings between releases. Add a row
here when a new spelling is encountered so all releases use consistent names in
the database. The original name is preserved in the `mapping` table as
`Original_value` alongside the `Standard_value` so the mapping is auditable.

### data_req_state_capacity.csv

Technologies to load from the **Capacity** sheet. Column: `Technology`.

### data_req_state_generation.csv

Technologies to load from the **Generation** sheet. Column: `Technology`.

### data_req_state_storage.csv

Technologies to load from the **Storage Capacity** and **Storage Energy** sheets.
Column: `Unnamed: 0` (legacy format from the original pipeline).

### data_req_rez.csv

Technologies to load from the **REZ Generation Capacity** and **REZ Generation**
sheets. Column: `Unnamed: 0`.

The state columns (NSW, QLD etc) with Y/N values in the storage and REZ CSVs are
legacy format from the original pipeline and are not read. All technologies in
these files apply across all states.

---

## Database schema

### context

One row per unique combination of modelling dimensions. All values in `data`
and `non_annual_data` join back here via `Id`.

| Column      | Type    | Notes                                                      |
|-------------|---------|------------------------------------------------------------|
| Id          | INTEGER | Primary key                                                |
| Data_source | TEXT    | ISP release, e.g. `2024 Final ISP`                         |
| Scenario_1  | TEXT    | Scenario name, e.g. `Step Change - Core`                   |
| Scenario_2  | TEXT    | CDP identifier, e.g. `CDP1`                                |
| State       | TEXT    | State abbreviation: NSW, QLD, VIC, SA, TAS                 |
| Region      | TEXT    | Subregion or REZ zone code; NULL for state-level rows      |
| Technology  | TEXT    | Standardised technology name                               |

### data

Annual modelled values.

| Column   | Type    | Notes                                                          |
|----------|---------|----------------------------------------------------------------|
| Id       | INTEGER | Foreign key to context                                         |
| Variable | TEXT    | `capacity`, `generation`, `storage capacity`, `storage energy` |
| Year     | TEXT    | Four-digit string, e.g. `"2030"`                               |
| Value    | REAL    | MW for capacity variables, GWh for generation variables        |

### non_annual_data

Same structure as `data`. Holds rows labelled `Existing and Committed` in the
source workbooks - these represent installed capacity at the time of publication
and cannot be placed in an annual time series.

### mapping

Data dictionary. One row per distinct value for each attribute type per
Data_source. Query this table to discover what scenarios, CDPs, states, regions,
and technologies exist for a given release without scanning the large tables.

| Column         | Type | Notes                                                          |
|----------------|------|----------------------------------------------------------------|
| Data_source    | TEXT | ISP release                                                    |
| Attribute_type | TEXT | `Scenario_1`, `Scenario_2`, `State`, `Region`, or `Technology` |
| Original_value | TEXT | Name as it appeared in the xlsx file                           |
| Standard_value | TEXT | Name after standardisation via name_map_technology.csv         |

---

## Querying the database

All queries join `data` to `context` on `Id`. Always include a `Data_source`
filter when querying by scenario name - names like `Step Change - Core` appear
in multiple releases and mixing them produces meaningless aggregates.

### What scenarios are available for a release

```sql
SELECT DISTINCT Original_value
FROM mapping
WHERE Data_source    = '2026 Draft ISP'
AND   Attribute_type = 'Scenario_1'
ORDER BY Original_value;
```

### What CDPs exist for a scenario

```sql
SELECT DISTINCT Original_value
FROM mapping
WHERE Data_source    = '2024 Final ISP'
AND   Attribute_type = 'Scenario_2'
ORDER BY Original_value;
```

### Total NEM capacity by year for one scenario

```sql
SELECT
    d.Year,
    ROUND(SUM(d.Value) / 1000.0, 1) AS total_GW
FROM data d
JOIN context c ON d.Id = c.Id
WHERE c.Data_source = '2024 Final ISP'
AND   c.Scenario_1  = 'Step Change - Core'
AND   c.Scenario_2  = 'CDP10'
AND   d.Variable    = 'capacity'
AND   c.Region IS NULL
GROUP BY d.Year
ORDER BY d.Year;
```

`WHERE c.Region IS NULL` restricts to state-level rows and excludes subregion
rows - important when summing across states to avoid double-counting.

### Technology capacity in a specific REZ zone

```sql
SELECT
    c.Technology,
    d.Year,
    d.Value AS MW
FROM data d
JOIN context c ON d.Id = c.Id
WHERE c.Data_source = '2026 Draft ISP'
AND   c.Scenario_1  = 'Step Change - Core'
AND   c.Scenario_2  = 'CDP1'
AND   c.State       = 'NSW'
AND   c.Region      = 'N1'
AND   d.Variable    = 'capacity'
ORDER BY c.Technology, d.Year;
```

### Existing and Committed capacity across releases

```sql
SELECT
    c.Data_source,
    c.Technology,
    c.State,
    d.Value AS MW
FROM non_annual_data d
JOIN context c ON d.Id = c.Id
WHERE c.Scenario_2  = 'CDP1'
AND   d.Variable    = 'capacity'
AND   c.Region IS NULL
ORDER BY c.Data_source, c.State, c.Technology;
```

---

## Known data issues

### Storage duplication

`Coordinated DER Storage` and `Distributed Storage` appear on both the Capacity
sheet (`variable = 'capacity'`) and the Storage Capacity sheet
(`variable = 'storage capacity'`) with identical values. Both are loaded. Filter
to one variable when querying these technologies to avoid double-counting.

### Negative generation values

Storage load technologies (`Utility-scale Storage Load`,
`Coordinated DER Storage Load`, `Distributed Storage Load`) have negative
generation values. This is physically correct - storage draws power from the grid
when charging. These are not data errors.

### Subregion coverage by vintage

The `Region` column (subregion codes: NNSW, CNSW etc) is only populated for
2024 Final and 2026 Draft. Earlier releases have `Region = NULL` for all rows.
Use `WHERE c.Region IS NULL` to restrict to state-level rows when comparing
across releases.

### Scenario names are not unique across releases

`Step Change - Core` and similar names exist in multiple releases. Always filter
on `Data_source` as well as `Scenario_1` in any query.

---

## Validation tools

### compare_databases.py

Compares two ISP databases to confirm row counts, distinct values, and aggregate
totals match. Useful after a rebuild to confirm nothing has changed unexpectedly.
Place both database files in the same folder and run:

```
python compare_databases.py
```

Edit `DB_A` and `DB_B` at the top of the script to point to the two files being
compared.

### compare_supply_annual.sql

SQL queries for comparing `ISP_HOME.db` against the original `Supply-annual.db`
reference database in DBeaver. Run against `ISP_HOME.db` - the script ATTACHes
`Supply-annual.db` automatically.

Sections 1 and 2 (row counts and context differences) are fast. Sections 3 and 4
(technology and value comparisons) involve large cross-database joins and are
slow on a laptop - run these only if the earlier sections show unexpected results.

---

## Scripts reference

| Script                          | When to run                                           |
|---------------------------------|-------------------------------------------------------|
| `read_hidden.py`                | Once per new ISP release, before create_db.py         |
| `find_missing_technologies.py`  | After read_hidden.py, before create_db.py             |
| `create_db.py`                  | To build or rebuild ISP_HOME.db                       |
| `isp_plots.py`                  | To generate analysis plots from ISP_HOME.db           |
| `compare_databases.py`          | To validate a new build against a reference database  |
| `compare_supply_annual.sql`     | Ad hoc comparison against Supply-annual.db in DBeaver |





Useful code:

python -m venv venv
.\venv\Scripts\Activate.ps1


docker run -it --rm alpine sh
apk add sqlite
sqlite3

