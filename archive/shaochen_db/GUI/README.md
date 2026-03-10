# ISP Plot Generator — User Guide

A PySide6 desktop application for generating ISP plots and exploring the relationships between scenarios. Dropdown menus and scenario lists are populated directly from the database. The application can produce PDF plots or, for reproducible research, a Python script that generates the same output.

Runs on Windows, macOS, and Linux as a local desktop application.

---

## Requirements

```
pip install PySide6 matplotlib pandas pyyaml
```

`pyyaml` is optional. If it is not installed the application uses a built-in reader for the simple config format used.

---

## Starting the application

Run from the `GUI/` directory:

```
python isp_plot_gui.py
```

---

## Database path

The database path is shown in the **Database** bar at the top of the window.

On first launch the application looks for `ISP.db` in the parent directory (one level above `GUI/`). If the file is not found, a warning is shown and the path field turns red.

To point the application at a different database file, click **Browse…** and select the `.db` file. The chosen path is written to `GUI/config.yaml` and will be used automatically on all future launches. You can also edit `config.yaml` directly in a text editor:

```yaml
db_path: "C:/path/to/your/ISP.db"
```

The **✔ Found** / **✘ Not found** indicator updates immediately when a new path is selected.

---

## Technology filter

Both tabs have a **Technology** group at the top of the controls panel. This controls which technologies are included when summing capacity, generation, and utilisation factor across the NEM.

Select a preset group from the dropdown:

| Group | Technologies included |
|---|---|
| Coal | Black Coal, Brown Coal |
| Gas | Mid-merit Gas, Mid-merit Gas with CCS, Peaking Gas+Liquids, Flexible Gas, Flexible Gas with CCS |
| Wind | Wind, Offshore Wind |
| Solar | Utility-scale Solar, Rooftop and Other Small-scale Solar, Solar Thermal, Distributed PV |
| Storage | Large-scale Storage, Medium Storage, Shallow Storage, Deep Storage, Utility-scale Storage, Distributed Storage |
| Coordinated CER | Coordinated CER Storage, Passive CER Storage |
| Coordinated DER | Coordinated DER Storage |
| Hydro | Hydro, Snowy 2.0, Borumba |
| Hydrogen | Hydrogen Turbine, Alkaline Electrolyser |
| Other | Biomass, Other Renewable Fuels, DSP |
| Custom | User-defined — see below |

**Storage load technologies** (rows whose names end in `Load`, which represent consumption rather than generation) are excluded from all preset groups. They are available in the Custom option but unchecked by default.

**Custom** reveals a scrollable checklist of all 35 technologies in the database. Check or uncheck individual technologies, or use the **All** / **None** buttons.

The selected technologies are aggregated across all NEM regions before plotting. The aggregation respects the storage level of each ISP release (state-level or subregion-level) to avoid double-counting.

---

## Tab 1 — Line Plots

One line per scenario row, all on a single axes.

### Building the scenario list

The **Scenarios to plot** group contains a table of rows. Each row defines one line on the chart:

| Column | What it does |
|---|---|
| **Release** | Dropdown populated from `Data_source` values in the database |
| **CDP** | Dropdown populated from `Scenario_2` values for the selected release |
| **Scenario** | Dropdown populated from `Scenario_1` values for the selected release and CDP |
| **Col** | Coloured square — click to open a colour picker for this line |
| **Style** | Linestyle — Solid, Dashed, Dotted, or Dash-dot |
| **Ref** | Checkbox — marks this row as the reference scenario, which overrides the colour and style and plots the line in solid black at a heavier weight |
| **✕** | Removes this row |

Changing **Release** repopulates the CDP dropdown from the database. Changing **CDP** repopulates the Scenario dropdown. The dropdowns reflect what the database contains for that combination — nothing is pre-selected.

Click **＋ Add scenario row** to add more rows. Each new row cycles through a default colour sequence.

The aggregation for each row is: sum capacity (or generation) across all technologies in the selected technology group, across all NEM regions, for that specific `(Data_source, Scenario_1, Scenario_2)` combination. Utilisation factor is derived as generation divided by maximum possible generation (installed capacity × 8 760 hours).

### Plot types

**Core — one line per scenario row above**: draws each row as a separate line on one axes.

**Sensitivity — all CDPs for each scenario**: for each unique release in the scenario list, draws all CDPs available in the database for each scenario in that release. The CDP selected in each row is drawn solid; other CDPs are drawn dashed at reduced opacity. Produces one page per release in both the preview and the saved PDF.

### Metrics

Check any combination of **Capacity [GW]**, **Utilisation Factor [%]**, and **Generation [GWh]**. Each selected metric produces a separate page in the output PDF and in the preview.

The **Y-axis max** spinboxes control the upper limit of each metric's axis.

### Preview and saving

**▶ Preview** generates figures for all selected metrics and plot types and loads them into the preview pane. Use the **◀ Prev** and **Next ▶** buttons below the canvas to page through. The counter shows the current page and total (e.g. `2 / 3`).

**💾 Save PDF** writes all selected plot types and metrics to a single multi-page PDF in the output directory.

**🐍 Export script** writes a self-contained Python script to the output directory and opens it in your default editor. The script contains:
- A SQL block describing which rows from the database feed the plots, including the technology filter
- The scenario table as a `pandas.DataFrame`
- The plot functions and render loop

---

## Tab 2 — Filled Band Comparison

Filled transparent bands rather than individual lines. Each band spans the minimum-to-maximum envelope across a set of scenarios at each year. Overlapping bands from different groups allow visual comparison of ranges.

### How bands work

Each band is defined by:
- A **release** (`Data_source`)
- A **CDP** (`Scenario_2`)
- A **set of scenarios** (`Scenario_1`) — the band spans min-to-max across all of these at each year point
- A **colour** and **transparency** (alpha)
- A **label** for the legend

A single-scenario band collapses to a zero-width band, appearing as a line.

### Building comparison groups

Each **band** is an expandable panel with:

| Control | What it does |
|---|---|
| **Label** | Text shown in the chart legend |
| **Colour** | Hex colour for this band — click the coloured square to open a picker |
| **α** | Transparency (0 = invisible, 1 = fully opaque). Lower values allow overlapping bands to show through each other |
| **Release** | Dropdown populated from `Data_source` values in the database |
| **CDP** | Dropdown populated from `Scenario_2` values for the selected release |
| **Scenarios** | Scrollable checklist of all `Scenario_1` values for the selected release and CDP. The band spans min-to-max across all checked scenarios |
| **All / None** | Check or uncheck all scenarios for this band |
| **✕ Remove band** | Removes this band |

Click **＋ Add comparison band** to add more bands. Bands are drawn in order — earlier bands sit behind later ones.

When you change **Release**, the CDP dropdown is repopulated from the database. When you change **CDP**, the scenario checklist is repopulated. Nothing is assumed or pre-selected on a user-initiated change.

The tab opens with three pre-configured bands covering all available scenarios across three ISP releases for Gas technology. These can be modified or removed.

### Chart title

The **Chart title** field sets the title shown on each figure. The metric name (Capacity, UF, or Generation) is prepended automatically, so a title of `Comparing ISP releases` produces pages titled `Capacity: Comparing ISP releases` and so on.

### Metrics and Y-axis

The same three metrics are available as in the Line Plots tab. Each selected metric produces a separate output PDF file (named `{prefix}_Capacity.pdf`, `{prefix}_UF.pdf`, `{prefix}_Generation.pdf`).

Adjust the **Max** spinbox for each metric as needed for the technology group selected.

### Preview and saving

**▶ Preview** generates figures for all selected metrics and loads them into the paged preview canvas.

**💾 Save PDFs** writes one PDF per metric to the output directory.

**🐍 Export script** writes a self-contained Python script. The script contains:
- A SQL block listing every `(Data_source, Scenario_1, Scenario_2)` row that contributes to a band, with notes on the database view and NEM aggregation
- The `COMPARISON_SETS` list from the current band configuration
- The `build_band` and `plot_comparison_page` functions and render loop

---

## Output directory

Both tabs have an **Output** group with a directory path label and a **Browse…** button. All PDFs and exported scripts are written to this directory. It defaults to the `GUI/` folder. The directory is not saved to `config.yaml` — it resets to the default each session.

---

## Saving and loading sessions

The **💾 Save session** and **📂 Load session** buttons in the database bar save and restore the complete state of both tabs — scenario rows, band configurations, technology selection, metrics, y-axis limits, and output settings.

Sessions are saved as YAML files. Any name and location can be chosen. To share a configuration, send the session file — the recipient can load it with **📂 Load session** and the dropdowns will repopulate from their own database.

If `pyyaml` is not installed, sessions are saved in JSON format with a `.yaml` extension. Both formats load correctly.

---

## Data notes

**Aggregation**: both tabs show NEM totals only — values are summed across all regions. There is no regional breakdown in either tab; this is by design to keep comparisons consistent across ISP releases that store data at different regional granularities.

**Capacity units**: values are stored in MW in the database and converted to GW on load.

**Generation units**: values are in GWh as stored in the database.

**Utilisation factor**: derived as `generation / (capacity × 8 760)`, expressed as a percentage.

**Load technologies**: technologies whose names end in `Load` represent electricity consumption rather than generation. They are excluded from all preset technology groups and unchecked by default in the Custom option. Including them in a generation or utilisation factor calculation will reduce the aggregate total.