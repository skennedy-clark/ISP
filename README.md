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
technologies, and loads everything into `ISP.db`.

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
|- create_db.py                  main pipeline - builds ISP.db
|- read_hidden.py                pre-processing - extracts hidden sheets
|- find_missing_technologies.py  audit tool - checks filter CSV coverage
|- compare_databases.py          validation tool - compares two databases
|- compare_supply_annual.sql     SQL queries for comparing against Supply-annual.db
|- isp_plots.py                  plotting script - reads from ISP.db
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

8. Check the `Scenario_2` (CDP) values for the new release and update the `odp`
   and `core_scenarios` dataframes in `isp_plots.py` accordingly. Note that
   AEMO sometimes embeds the ODP label directly in the CDP name - for example,
   2026 Draft uses `CDP4 (ODP)` rather than plain `CDP4`.

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

### v_context_with_region (view)

A convenience view over `context` that replaces NULL values in the `Region`
column with synthetic state-level placeholder codes. **Analysis scripts should
query this view rather than `context` directly.**

```sql
CREATE VIEW v_context_with_region AS
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
```

Rows where `Region IS NULL` in the underlying `context` table (i.e. state-level
rows from releases without subregion breakdown) are assigned a synthetic
placeholder code: `N0` (NSW), `Q0` (QLD), `V0` (VIC), `S0` (SA), `T0` (TAS).
These codes do not appear in the source data and will not collide with real
subregion codes (`NNSW`, `CQ`, `SQ` etc.) or REZ codes (`N1`, `Q3` etc.).

**Why this matters.** The granularity of stored data differs across releases:

| Release      | Data granularity              |
|--------------|-------------------------------|
| 2022 Draft   | State level only (Region NULL) |
| 2022 Final   | State level only (Region NULL) |
| 2024 Draft   | State level only (Region NULL) |
| 2024 Final   | Subregion level only           |
| 2026 Draft   | Subregion level only           |

Each release uses exactly one level consistently per technology - there are no
releases where the same technology appears at both state and subregion level
simultaneously. This means there is no double-counting risk when summing all
rows within a release. The synthetic codes simply make the `Region` column
uniform across releases so joins and `groupby` operations behave consistently
without needing special NULL handling.

**Filtering to state-level rows across releases.** When querying through this
view you cannot use `Region IS NULL` - the view never returns NULLs. Use the
synthetic codes instead:

```sql
WHERE v.Region IN ('N0', 'Q0', 'V0', 'S0', 'T0')
```

This returns the state-level rows for 2022/2024 Draft releases. For 2024 Final
and 2026 Draft, which have no state-level rows at all, this filter returns
nothing. In those cases you should query without a Region filter and let the
subregion rows aggregate to the state/NEM total naturally.

---

## Querying the database

All queries join `data` to `v_context_with_region` on `Id`. Always include a
`Data_source` filter when querying by scenario name - names like
`Step Change - Core` appear in multiple releases and mixing them produces
meaningless aggregates.

### What scenarios are available for a release

```sql
SELECT DISTINCT Original_value
FROM mapping
WHERE Data_source    = '2026 Draft ISP'
AND   Attribute_type = 'Scenario_1'
ORDER BY Original_value;
```

### What CDPs exist for a release

```sql
SELECT DISTINCT Original_value
FROM mapping
WHERE Data_source    = '2024 Final ISP'
AND   Attribute_type = 'Scenario_2'
ORDER BY Original_value;
```

### Total NEM capacity by year - releases with state-level data (2022, 2024 Draft)

Filter to the synthetic state codes to select state-level rows only and avoid
picking up any subregion rows if they are ever added in future:

```sql
SELECT
    d.Year,
    ROUND(SUM(d.Value) / 1000.0, 1) AS total_GW
FROM data d
JOIN v_context_with_region v ON d.Id = v.Id
WHERE v.Data_source = '2022 Final ISP'
AND   v.Scenario_1  = 'Step Change - Updated Inputs'
AND   v.Scenario_2  = 'CDP12'
AND   d.Variable    = 'capacity'
AND   v.Region IN ('N0', 'Q0', 'V0', 'S0', 'T0')
GROUP BY d.Year
ORDER BY d.Year;
```

### Total NEM capacity by year - releases with subregion data (2024 Final, 2026 Draft)

Omit the Region filter entirely - the subregion rows sum to the correct NEM total:

```sql
SELECT
    d.Year,
    ROUND(SUM(d.Value) / 1000.0, 1) AS total_GW
FROM data d
JOIN v_context_with_region v ON d.Id = v.Id
WHERE v.Data_source = '2024 Final ISP'
AND   v.Scenario_1  = 'Step Change - Core'
AND   v.Scenario_2  = 'CDP14'
AND   d.Variable    = 'capacity'
GROUP BY d.Year
ORDER BY d.Year;
```

### Technology capacity in a specific REZ zone

```sql
SELECT
    v.Technology,
    d.Year,
    d.Value AS MW
FROM data d
JOIN v_context_with_region v ON d.Id = v.Id
WHERE v.Data_source = '2026 Draft ISP'
AND   v.Scenario_1  = 'Step Change - Core'
AND   v.Scenario_2  = 'CDP1'
AND   v.State       = 'NSW'
AND   v.Region      = 'N1'
AND   d.Variable    = 'capacity'
ORDER BY v.Technology, d.Year;
```

### Existing and Committed capacity across releases

```sql
SELECT
    v.Data_source,
    v.Technology,
    v.State,
    d.Value AS MW
FROM non_annual_data d
JOIN v_context_with_region v ON d.Id = v.Id
WHERE v.Scenario_2  = 'CDP1'
AND   d.Variable    = 'capacity'
AND   v.Region IN ('N0', 'Q0', 'V0', 'S0', 'T0')
ORDER BY v.Data_source, v.State, v.Technology;
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

The `Region` column is only populated for 2024 Final and 2026 Draft. Earlier
releases have `Region = NULL` for all rows in the underlying `context` table.
Through `v_context_with_region` these appear as the synthetic state codes `N0`,
`Q0`, `V0`, `S0`, `T0` (see view documentation above).

Critically, 2024 Final and 2026 Draft store all data at subregion level only -
there are no state-level rows for those releases. The `Region IN ('N0'...)` filter
therefore returns nothing for those releases. See the query examples above for
the correct approach for each release type.

### Scenario names are not unique across releases

`Step Change - Core` and similar names exist in multiple releases. Always filter
on `Data_source` as well as `Scenario_1` in any query.

### ODP label embedded in CDP name (2026 Draft)

For 2026 Draft, AEMO embedded the ODP designation directly in the CDP name. The
ODP is `CDP4 (ODP)`, not `CDP4`. When referencing the 2026 Draft ODP in scripts
or queries use the full string `CDP4 (ODP)`.

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

SQL queries for comparing `ISP.db` against the original `Supply-annual.db`
reference database in DBeaver. Run against `ISP.db` - the script ATTACHes
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
| `create_db.py`                  | To build or rebuild ISP.db                            |
| `isp_plots.py`                  | To generate analysis plots from ISP.db                |
| `compare_databases.py`          | To validate a new build against a reference database  |
| `compare_supply_annual.sql`     | Ad hoc comparison against Supply-annual.db in DBeaver |

---

## Useful commands

```powershell
# Create and activate virtual environment (Windows)
python -m venv venv
.\venv\Scripts\Activate.ps1
```

```bash
# Inspect the database from a minimal Docker container
docker run -it --rm alpine sh
apk add sqlite
sqlite3 ISP.db
```