"""isp_filled_plots_2026.py

Produces filled-band comparison charts for ISP coal analysis.

Each chart overlays transparent min/max bands, one per comparison group.
No individual scenario lines are drawn inside or on top of the bands —
the purpose is purely visual comparison of uncertainty ranges between groups.

Outputs (three PDFs, one per metric):
    Coal_filled_Capacity.pdf
    Coal_filled_UF.pdf
    Coal_filled_Generation.pdf

Each PDF has one page per COMPARISON_SET defined below.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOW TO CHANGE WHAT IS BEING COMPARED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Everything that controls chart content lives in the COMPARISON_SETS list
(search for "CONFIGURE COMPARISON SETS" below).

Each entry in COMPARISON_SETS defines one PDF page. It is a dict with:

    "title"   : str  — page title shown on the chart
    "groups"  : list of dicts, each defining one coloured band:
        "label"     : str    — legend label for this band
        "colour"    : str    — hex colour for this band
        "alpha"     : float  — band transparency (0=invisible, 1=solid)
        "isp"       : str    — Data_source value, e.g. "2024 Final ISP"
        "cdp"       : str    — Scenario_2 (CDP) to use, e.g. "CDP14"
                               Use the ODP value to compare ODP ranges,
                               or supply a specific CDP to compare CDPs.
        "scenarios" : list[str] — Scenario_1 values included in the band.
                               The band spans min-to-max across all of these.
                               A single-item list collapses the band to a line.

EXAMPLES OF ALTERNATIVE COMPARISONS
-------------------------------------

Compare scenario families within one release:
    "groups": [
        { "isp": "2024 Final ISP", "cdp": "CDP14",
          "scenarios": ["Step Change - Core",
                        "Step Change - Extended Eraring", ...],
          "label": "Step Change family", "colour": "#000000", "alpha": 0.35 },
        { "isp": "2024 Final ISP", "cdp": "CDP14",
          "scenarios": ["Progressive Change - Core",
                        "Progressive Change - Extended Eraring"],
          "label": "Progressive Change family", "colour": "#4C72B0", "alpha": 0.35 },
    ]

Compare one scenario across CDPs (CDP sensitivity):
    Each group has one scenario and one CDP — the band collapses to a line.
    { "isp": "2022 Final ISP", "cdp": "CDP12",
      "scenarios": ["Step Change - Updated Inputs"],
      "label": "Step Change ODP (CDP12)", "colour": "#000000", "alpha": 0.6 },
    { "isp": "2022 Final ISP", "cdp": "CDP8",
      "scenarios": ["Step Change - Updated Inputs"],
      "label": "Step Change CDP8", "colour": "#888888", "alpha": 0.6 },

REFERENCE — Available ISP releases and their ODP CDPs:
    2022 Final ISP  →  ODP = CDP12
    2024 Final ISP  →  ODP = CDP14
    2026 Draft ISP  →  ODP = CDP4 (ODP)

To discover all Scenario_1 and Scenario_2 values for a release, query ISP.db:
    SELECT DISTINCT Original_value FROM mapping
    WHERE Data_source = '2026 Draft ISP' AND Attribute_type = 'Scenario_1'
    ORDER BY Original_value;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# ============================================================================
# CONFIGURE COMPARISON SETS
# ============================================================================
# Each dict in this list produces one PDF page per metric output file.
# Edit, add, or remove entries to change what is compared.
# See the module docstring above for the full configuration reference.
# ============================================================================

COMPARISON_SETS = [

    # --------------------------------------------------------------------------
    # PAGE 1: All ODP scenarios — three ISP releases side by side
    # Each band = full spread of scenarios at the ODP CDP for that release.
    # Shows how coal retirement uncertainty has shifted across ISP editions.
    # --------------------------------------------------------------------------
    {
        "title": "All ODP scenarios — comparing ISP releases",
        "groups": [
            {
                "label":     "2022 Final ISP – all ODP scenarios",
                "colour":    "#E07B39",   # orange
                "alpha":     0.35,
                "isp":       "2022 Final ISP",
                "cdp":       "CDP12",     # ODP for 2022 Final
                "scenarios": [
                    "Hydrogen Superpower - Updated Inputs",
                    "Progressive Change - Updated Inputs",
                    "Slow Change - Updated Inputs",
                    "Step Change - Updated Inputs",
                ],
            },
            {
                "label":     "2024 Final ISP – all ODP scenarios",
                "colour":    "#4C72B0",   # blue
                "alpha":     0.35,
                "isp":       "2024 Final ISP",
                "cdp":       "CDP14",     # ODP for 2024 Final
                "scenarios": [
                    "Step Change - Core",
                    "Progressive Change - Core",
                    "Green Energy Exports - Core",
                    "Step Change - Extended Eraring",
                    "Progressive Change - Extended Eraring",
                    "Green Energy Exports - Extended Eraring",
                    "Step Change - Additional Load",
                    "Step Change - Lower EV Uptake",
                    "Step Change - Reduced CER Coordination",
                    "Step Change - Low Hydrogen Flexibility",
                    "Step Change - Constrained Supply Chains",
                    "Step Change - Alternative Worst Sequence",
                ],
            },
            {
                "label":     "2026 Draft ISP – all ODP scenarios",
                "colour":    "#2CA02C",   # green
                "alpha":     0.35,
                "isp":       "2026 Draft ISP",
                "cdp":       "CDP4 (ODP)",  # ODP for 2026 Draft
                "scenarios": [
                    "Slower Growth - Core",
                    "Accelerated Transition - Core",
                    "Step Change - Core",
                ],
            },
        ],
    },

    # --------------------------------------------------------------------------
    # PAGE 2: Core scenarios only — three ISP releases side by side
    # Narrower bands using only the core (non-sensitivity) scenarios.
    # Pair with PAGE 1 to see how much sensitivities widen the range.
    # --------------------------------------------------------------------------
    {
        "title": "Core scenarios only — comparing ISP releases",
        "groups": [
            {
                "label":     "2022 Final ISP – core scenarios",
                "colour":    "#E07B39",
                "alpha":     0.40,
                "isp":       "2022 Final ISP",
                "cdp":       "CDP12",
                "scenarios": [
                    "Hydrogen Superpower - Updated Inputs",
                    "Progressive Change - Updated Inputs",
                    "Slow Change - Updated Inputs",
                    "Step Change - Updated Inputs",
                ],
            },
            {
                "label":     "2024 Final ISP – core scenarios",
                "colour":    "#4C72B0",
                "alpha":     0.40,
                "isp":       "2024 Final ISP",
                "cdp":       "CDP14",
                "scenarios": [
                    "Step Change - Core",
                    "Progressive Change - Core",
                    "Green Energy Exports - Core",
                ],
            },
            {
                "label":     "2026 Draft ISP – core scenarios",
                "colour":    "#2CA02C",
                "alpha":     0.40,
                "isp":       "2026 Draft ISP",
                "cdp":       "CDP4 (ODP)",
                "scenarios": [
                    "Slower Growth - Core",
                    "Accelerated Transition - Core",
                    "Step Change - Core",
                ],
            },
        ],
    },

    # --------------------------------------------------------------------------
    # PAGE 3: 2024 Final ISP — core vs full sensitivity range
    # Both bands use the same release and CDP.
    # The wider (purple) band = all scenarios including sensitivities.
    # The narrower (blue) band = core scenarios only.
    # Overlap shows whether sensitivities add meaningful range beyond the core.
    # --------------------------------------------------------------------------
    {
        "title": "2024 Final ISP — core scenarios vs full sensitivity range",
        "groups": [
            {
                "label":     "2024 Final — all ODP scenarios (incl. sensitivities)",
                "colour":    "#9467BD",   # purple — drawn first, sits behind
                "alpha":     0.25,
                "isp":       "2024 Final ISP",
                "cdp":       "CDP14",
                "scenarios": [
                    "Step Change - Core",
                    "Progressive Change - Core",
                    "Green Energy Exports - Core",
                    "Step Change - Extended Eraring",
                    "Progressive Change - Extended Eraring",
                    "Green Energy Exports - Extended Eraring",
                    "Step Change - Additional Load",
                    "Step Change - Lower EV Uptake",
                    "Step Change - Reduced CER Coordination",
                    "Step Change - Low Hydrogen Flexibility",
                    "Step Change - Constrained Supply Chains",
                    "Step Change - Alternative Worst Sequence",
                ],
            },
            {
                "label":     "2024 Final — core scenarios only",
                "colour":    "#4C72B0",   # blue — drawn on top, narrower band
                "alpha":     0.50,
                "isp":       "2024 Final ISP",
                "cdp":       "CDP14",
                "scenarios": [
                    "Step Change - Core",
                    "Progressive Change - Core",
                    "Green Energy Exports - Core",
                ],
            },
        ],
    },

]

# ============================================================================
# END OF CONFIGURATION — no need to edit below this line for typical use
# ============================================================================


# ---------------------------------------------------------------------------
# Y-axis limits per metric
# Adjust if values exceed these bounds for a new release or technology scope.
# ---------------------------------------------------------------------------
MAX_CAPACITY   = 26       # GW
MAX_UF         = 100      # percent
MAX_GENERATION = 140000   # GWh


# ---------------------------------------------------------------------------
# Data loading (identical to isp_plots.py)
# ---------------------------------------------------------------------------
def get_data(isp_report):
    """Query capacity, generation, and context from ISP.db.

    Parameters
    ----------
    isp_report : str
        SQL LIKE fragment — use "%" to load all releases.

    Returns
    -------
    capacity, generation, context : pd.DataFrame
    """
    db_path = os.path.join(os.getcwd(), "ISP.db")
    conn    = sqlite3.connect(db_path)

    capacity = pd.read_sql(
        """
        SELECT a.Id, a.Variable, a.Year, a.Value,
               v.Data_source, v.Scenario_1, v.Scenario_2,
               v.State, v.Region, v.Technology
        FROM data a
        INNER JOIN v_context_with_region v ON a.Id = v.Id
        WHERE a.Variable = 'capacity'
          AND v.Data_source LIKE ?
        """,
        con=conn, params=[f"%{isp_report}%"],
    )
    capacity = capacity[~capacity.Year.isin(["Existing and Committed", "Un33"])]
    capacity = capacity[capacity.Value.notna() & capacity.Scenario_2.notna()]
    capacity["Value"] = capacity["Value"].astype(float) / 1000
    capacity["Year"]  = capacity["Year"].astype(int)

    generation = pd.read_sql(
        """
        SELECT a.Id, a.Variable, a.Year, a.Value,
               v.Data_source, v.Scenario_1, v.Scenario_2,
               v.State, v.Region, v.Technology
        FROM data a
        INNER JOIN v_context_with_region v ON a.Id = v.Id
        WHERE a.Variable = 'generation'
          AND v.Data_source LIKE ?
        """,
        con=conn, params=[f"%{isp_report}%"],
    )
    generation = generation[~generation.Year.isin(["Existing and Committed", "Un33"])]
    generation = generation[generation.Value.notna() & generation.Scenario_2.notna()]
    generation["Value"] = generation["Value"].astype(float)
    generation["Year"]  = generation["Year"].astype(int)

    context = pd.read_sql(
        "SELECT * FROM v_context_with_region WHERE Data_source LIKE ?",
        con=conn, params=[f"%{isp_report}%"],
    )

    conn.close()
    return capacity, generation, context


# ---------------------------------------------------------------------------
# Band computation
# ---------------------------------------------------------------------------
def build_band(df_metric, isp, cdp, scenarios):
    """Compute per-year min/max envelope for a set of scenarios.

    Parameters
    ----------
    df_metric : pd.DataFrame
        Aggregated metric with columns Data_source, Scenario_1, Scenario_2,
        Year, Value.
    isp : str
        Data_source to filter on.
    cdp : str
        Scenario_2 to filter on.
    scenarios : list[str]
        Scenario_1 values to include. The band spans min-to-max across all
        of these at each year. A single-item list produces a zero-width band
        (effectively a line).

    Returns
    -------
    env_min, env_max : pd.Series indexed by Year, or (None, None) if no data.
    """
    pieces = []
    for scen in scenarios:
        subset = df_metric[
            (df_metric["Data_source"] == isp) &
            (df_metric["Scenario_1"]  == scen) &
            (df_metric["Scenario_2"]  == cdp)
        ][["Year", "Value"]].set_index("Year").rename(columns={"Value": scen})
        if not subset.empty:
            pieces.append(subset)

    if not pieces:
        return None, None

    wide    = pd.concat(pieces, axis=1).sort_index()
    env_min = wide.min(axis=1)
    env_max = wide.max(axis=1)
    return env_min, env_max


# ---------------------------------------------------------------------------
# Page rendering
# ---------------------------------------------------------------------------
def plot_comparison_page(df_metric, comparison_set, ylabel, max_value):
    """Draw one filled-band comparison page.

    Overlays one transparent band per group defined in comparison_set.
    No individual lines are drawn — the visual comparison is purely between
    the coloured uncertainty ranges.

    Parameters
    ----------
    df_metric : pd.DataFrame
        Aggregated metric (capacity, UF, or generation).
    comparison_set : dict
        One entry from COMPARISON_SETS.
    ylabel : str
        Y-axis label including units.
    max_value : float
        Y-axis upper limit.

    Returns
    -------
    fig : matplotlib.figure.Figure
    """
    fig, ax = plt.subplots(figsize=(13, 8))

    for group in comparison_set["groups"]:
        env_min, env_max = build_band(
            df_metric = df_metric,
            isp       = group["isp"],
            cdp       = group["cdp"],
            scenarios = group["scenarios"],
        )

        if env_min is None:
            print(f"  WARNING: no data for group '{group['label']}' "
                  f"(ISP={group['isp']}, CDP={group['cdp']})")
            continue

        ax.fill_between(
            env_min.index,
            env_min,
            env_max,
            alpha    = group["alpha"],
            color    = group["colour"],
            label    = group["label"],
            linewidth = 0,   # suppress border line on band edges
        )

    ax.set_title(comparison_set["title"], fontweight="bold", fontsize=15)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=13)
    ax.set_ylim(0, max_value)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="best", fontsize=11, framealpha=0.9)
    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Main — load data, aggregate, render all PDFs
# ---------------------------------------------------------------------------
print("Loading data from ISP.db...")
(capacity, generation, context) = get_data("%")

coal_tech = ["Black Coal", "Black coal", "Brown Coal", "Brown coal"]

capacity_gpg   = capacity[capacity.Technology.isin(coal_tech)].copy()
generation_gpg = generation[generation.Technology.isin(coal_tech)].copy()

capacity_gpg["max_annual_gen"] = capacity_gpg["Value"] * 24 * 365

# NEM-total aggregations.
# No Region filter needed — each release stores data at exactly one level:
#   2022 Final / 2024 Draft  →  state level   (Region = synthetic N0/Q0/...)
#   2024 Final / 2026 Draft  →  subregion level (Region = CNSW/SQ/...)
# Each set of subregion rows sums to the correct NEM total on its own.
capacity_gpg_sum = capacity_gpg.groupby(
    ["Data_source", "Scenario_1", "Scenario_2", "Year"], as_index=False
).agg({"Value": "sum", "max_annual_gen": "sum"})

generation_gpg_sum = generation_gpg.groupby(
    ["Data_source", "Scenario_1", "Scenario_2", "Year"], as_index=False
)["Value"].sum()

# Utilisation factor = actual generation / theoretical maximum (capacity × 8760h)
util_factor_gpg = (
    capacity_gpg_sum[["Data_source", "Scenario_1", "Scenario_2", "Year", "max_annual_gen"]]
    .merge(generation_gpg_sum, how="inner",
           on=["Data_source", "Scenario_1", "Scenario_2", "Year"])
)
util_factor_gpg["Value"] = (
    util_factor_gpg["Value"] / util_factor_gpg["max_annual_gen"] * 100
)

print(f"Data loaded. Rendering {len(COMPARISON_SETS)} comparison set(s) × 3 metrics...\n")

# One output file per metric; one page per comparison set
METRICS = [
    {
        "name":      "Capacity",
        "df":        capacity_gpg_sum,
        "ylabel":    "Capacity [GW]",
        "max_value": MAX_CAPACITY,
        "outfile":   "Coal_filled_Capacity.pdf",
    },
    {
        "name":      "UF",
        "df":        util_factor_gpg,
        "ylabel":    "UF [GWh / GWh × 100]",
        "max_value": MAX_UF,
        "outfile":   "Coal_filled_UF.pdf",
    },
    {
        "name":      "Generation",
        "df":        generation_gpg_sum,
        "ylabel":    "Generation [GWh]",
        "max_value": MAX_GENERATION,
        "outfile":   "Coal_filled_Generation.pdf",
    },
]

for metric in METRICS:
    with PdfPages(metric["outfile"]) as pdf:
        for cset in COMPARISON_SETS:
            # Prepend metric name to title so pages are self-explanatory
            page_cset = {**cset, "title": f"{metric['name']}: {cset['title']}"}

            fig = plot_comparison_page(
                df_metric      = metric["df"],
                comparison_set = page_cset,
                ylabel         = metric["ylabel"],
                max_value      = metric["max_value"],
            )
            pdf.savefig(fig)
            plt.close(fig)
            print(f"  [{metric['name']}] {cset['title']}")

    print(f"Saved: {metric['outfile']}\n")

print("Done.")