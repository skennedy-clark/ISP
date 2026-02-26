"""isp_filled_plots_2026.py

Produces filled-envelope versions of the ISP coal analysis charts.

For each plot, all scenarios within a release are collapsed into a shaded
min/max envelope (fill_between the per-year minimum and maximum values across
all ODP scenarios), with the reference scenario drawn as a solid line on top.

Three output PDFs are produced, one per metric:
    Coal_filled_Capacity.pdf
    Coal_filled_UF.pdf
    Coal_filled_Generation.pdf

Each PDF contains one page per ISP release (2022 Final, 2024 Final, 2026 Draft)
showing all releases together in the same axes for cross-release comparison.

Data source: ISP.db (same database as isp_plots.py).
"""

import sqlite3
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# ---------------------------------------------------------------------------
# Palette - one colour per ISP release
# ---------------------------------------------------------------------------
RELEASE_COLOURS = {
    "2022 Final ISP": "#E07B39",   # orange
    "2024 Final ISP": "#4C72B0",   # blue
    "2026 Draft ISP": "#2CA02C",   # green
}

RELEASE_LABELS = {
    "2022 Final ISP": "2022 Final",
    "2024 Final ISP": "2024 Final",
    "2026 Draft ISP": "2026 Draft",
}


# ---------------------------------------------------------------------------
# Data loading  (identical to isp_plots.py)
# ---------------------------------------------------------------------------
def get_data(isp_report):
    """Query capacity, generation, and context from ISP.db.

    Parameters
    ----------
    isp_report : str
        SQL LIKE fragment - use "%" to load all releases.

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
# Envelope plot  (one page = all releases together)
# ---------------------------------------------------------------------------
def plot_filled_envelope(
    df_metric,
    all_scenarios_odp,
    reference_scenarios,
    odp,
    ylabel,
    title,
    max_value,
    release_order=None,
):
    """Draw a filled min/max envelope for each ISP release on shared axes.

    For each release:
      - The shaded band spans the per-year min and max values across all ODP
        scenarios for that release.
      - The reference scenario is drawn as a solid line on top of the band.
      - A thinner line is drawn for all other ODP scenarios within the band so
        the individual trajectories are visible but not dominant.

    Parameters
    ----------
    df_metric : pd.DataFrame
        Aggregated metric (capacity_gpg_sum, util_factor_gpg, or
        generation_gpg_sum). Must have columns:
        Data_source, Scenario_1, Scenario_2, Year, Value.
    all_scenarios_odp : pd.DataFrame
        All (Data_source, Scenario_1, Scenario_2) combinations at the ODP,
        one row per unique scenario. Built by merging all_scenarios with odp.
    reference_scenarios : pd.DataFrame
        Columns: ISP, core. Identifies the reference scenario per release.
    odp : pd.DataFrame
        Columns: Data_source, Scenario_2. Maps each release to its ODP CDP.
    ylabel : str
        Y-axis label including units.
    title : str
        Chart title.
    max_value : float
        Y-axis upper limit.
    release_order : list[str] or None
        Order in which releases are drawn (bottom to top in legend).
        Defaults to the order they appear in reference_scenarios.

    Returns
    -------
    fig : matplotlib.figure.Figure
    """
    fig, ax = plt.subplots(figsize=(13, 8))

    if release_order is None:
        release_order = list(reference_scenarios["ISP"].unique())

    for isp in release_order:
        colour = RELEASE_COLOURS.get(isp, "gray")
        label  = RELEASE_LABELS.get(isp, isp)

        # Scenarios belonging to this release at its ODP CDP
        isp_scenarios = all_scenarios_odp[all_scenarios_odp["Data_source"] == isp]
        if isp_scenarios.empty:
            continue

        # Reference scenario name for this release
        ref_row = reference_scenarios[reference_scenarios["ISP"] == isp]
        ref_scenario = ref_row["core"].iloc[0] if not ref_row.empty else None

        # Collect all scenario series into a wide pivot: rows=Year, cols=Scenario_1
        pieces = []
        for _, row in isp_scenarios.iterrows():
            subset = df_metric[
                (df_metric["Data_source"] == isp) &
                (df_metric["Scenario_1"]  == row["Scenario_1"]) &
                (df_metric["Scenario_2"]  == row["Scenario_2"])
            ][["Year", "Value"]].set_index("Year").rename(columns={"Value": row["Scenario_1"]})
            pieces.append(subset)

        if not pieces:
            continue

        wide = pd.concat(pieces, axis=1).sort_index()

        # Per-year min and max across all scenarios
        env_min = wide.min(axis=1)
        env_max = wide.max(axis=1)

        # Filled envelope
        ax.fill_between(
            wide.index,
            env_min,
            env_max,
            alpha=0.20,
            color=colour,
            label=f"{label} range",
        )

        # Thin lines for each individual scenario inside the band
        for col in wide.columns:
            if col == ref_scenario:
                continue  # drawn separately below
            ax.plot(
                wide.index,
                wide[col],
                color=colour,
                linewidth=0.8,
                alpha=0.45,
            )

        # Reference scenario as a bold solid line
        if ref_scenario and ref_scenario in wide.columns:
            ax.plot(
                wide.index,
                wide[ref_scenario],
                color=colour,
                linewidth=2.5,
                linestyle="-",
                label=f"{label} – {ref_scenario} (reference)",
            )

    ax.set_title(title, fontweight="bold", fontsize=15)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=13)
    ax.set_ylim(0, max_value)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="best", fontsize=10, framealpha=0.9)
    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Per-release sensitivity page  (one page per ISP release)
# ---------------------------------------------------------------------------
def plot_filled_per_release(
    df_metric,
    all_scenarios_odp,
    reference_scenarios,
    odp,
    ylabel,
    title_prefix,
    max_value,
    pdf,
):
    """One PDF page per ISP release showing its own filled envelope.

    In addition to the all-releases combined page this gives a cleaner view
    of each individual release without the other releases competing for space.

    Parameters
    ----------
    pdf : PdfPages
        Open PdfPages object to append pages to.
    All other parameters are the same as plot_filled_envelope.
    """
    for isp in reference_scenarios["ISP"].unique():
        colour = RELEASE_COLOURS.get(isp, "gray")
        label  = RELEASE_LABELS.get(isp, isp)

        isp_scenarios = all_scenarios_odp[all_scenarios_odp["Data_source"] == isp]
        if isp_scenarios.empty:
            continue

        ref_row = reference_scenarios[reference_scenarios["ISP"] == isp]
        ref_scenario = ref_row["core"].iloc[0] if not ref_row.empty else None

        pieces = []
        for _, row in isp_scenarios.iterrows():
            subset = df_metric[
                (df_metric["Data_source"] == isp) &
                (df_metric["Scenario_1"]  == row["Scenario_1"]) &
                (df_metric["Scenario_2"]  == row["Scenario_2"])
            ][["Year", "Value"]].set_index("Year").rename(columns={"Value": row["Scenario_1"]})
            pieces.append(subset)

        if not pieces:
            continue

        wide = pd.concat(pieces, axis=1).sort_index()
        env_min = wide.min(axis=1)
        env_max = wide.max(axis=1)

        fig, ax = plt.subplots(figsize=(13, 8))

        ax.fill_between(
            wide.index,
            env_min,
            env_max,
            alpha=0.25,
            color=colour,
            label="Scenario range (min–max)",
        )

        for col in wide.columns:
            if col == ref_scenario:
                continue
            ax.plot(
                wide.index,
                wide[col],
                color=colour,
                linewidth=0.9,
                alpha=0.5,
                label=col,
            )

        if ref_scenario and ref_scenario in wide.columns:
            ax.plot(
                wide.index,
                wide[ref_scenario],
                color=colour,
                linewidth=3.0,
                linestyle="-",
                label=f"{ref_scenario} (reference)",
            )

        ax.set_title(
            f"{title_prefix}: {isp}",
            fontweight="bold", fontsize=15,
        )
        ax.set_ylabel(ylabel, fontweight="bold", fontsize=13)
        ax.set_ylim(0, max_value)
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.legend(loc="best", fontsize=9, framealpha=0.9)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
(capacity, generation, context) = get_data("%")

# Unique scenario/CDP combinations across all releases
all_scenarios = (
    context[["Data_source", "Scenario_1", "Scenario_2"]]
    .drop_duplicates()
    .dropna(subset=["Data_source", "Scenario_1", "Scenario_2"])
    .reset_index(drop=True)
)

# Core scenarios and ODP per release
core_scenarios = pd.DataFrame([
    # 2022 Final ISP
    {"ISP": "2022 Final ISP", "core": "Hydrogen Superpower - Updated Inputs", "ODP": "CDP12"},
    {"ISP": "2022 Final ISP", "core": "Progressive Change - Updated Inputs",   "ODP": "CDP12"},
    {"ISP": "2022 Final ISP", "core": "Slow Change - Updated Inputs",           "ODP": "CDP12"},
    {"ISP": "2022 Final ISP", "core": "Step Change - Updated Inputs",           "ODP": "CDP12"},
    # 2024 Final ISP
    {"ISP": "2024 Final ISP", "core": "Step Change - Core",          "ODP": "CDP14"},
    {"ISP": "2024 Final ISP", "core": "Progressive Change - Core",   "ODP": "CDP14"},
    {"ISP": "2024 Final ISP", "core": "Green Energy Exports - Core", "ODP": "CDP14"},
    # 2026 Draft ISP
    {"ISP": "2026 Draft ISP", "core": "Slower Growth - Core",          "ODP": "CDP4 (ODP)"},
    {"ISP": "2026 Draft ISP", "core": "Accelerated Transition - Core", "ODP": "CDP4 (ODP)"},
    {"ISP": "2026 Draft ISP", "core": "Step Change - Core",            "ODP": "CDP4 (ODP)"},
])

odp = pd.DataFrame({
    "Data_source": ["2022 Final ISP", "2024 Final ISP", "2026 Draft ISP"],
    "Scenario_2":  ["CDP12",          "CDP14",          "CDP4 (ODP)"],
})

reference_scenarios = pd.DataFrame({
    "ISP":  ["2022 Final ISP",             "2024 Final ISP",    "2026 Draft ISP"],
    "core": ["Step Change - Updated Inputs", "Step Change - Core", "Slower Growth - Core"],
})

# ODP scenarios only (one row per scenario at each release's ODP CDP)
all_scenarios_odp = all_scenarios.merge(odp, how="inner")

# ---------------------------------------------------------------------------
# Technology filters
# ---------------------------------------------------------------------------
coal_tech = ["Black Coal", "Black coal", "Brown Coal", "Brown coal"]

capacity_gpg  = capacity[capacity.Technology.isin(coal_tech)].copy()
generation_gpg = generation[generation.Technology.isin(coal_tech)].copy()

capacity_gpg["max_annual_gen"] = capacity_gpg["Value"] * 24 * 365

# NEM-total aggregations (no Region filter - each release uses one level only)
capacity_gpg_sum = capacity_gpg.groupby(
    ["Data_source", "Scenario_1", "Scenario_2", "Year"], as_index=False
).agg({"Value": "sum", "max_annual_gen": "sum"})

generation_gpg_sum = generation_gpg.groupby(
    ["Data_source", "Scenario_1", "Scenario_2", "Year"], as_index=False
)["Value"].sum()

# Utilisation factor: generation / max_possible_generation
util_factor_gpg = (
    capacity_gpg_sum[["Data_source", "Scenario_1", "Scenario_2", "Year", "max_annual_gen"]]
    .merge(generation_gpg_sum, how="inner",
           on=["Data_source", "Scenario_1", "Scenario_2", "Year"])
)
util_factor_gpg["Value"] = (
    util_factor_gpg["Value"] / util_factor_gpg["max_annual_gen"] * 100
)

RELEASE_ORDER = ["2022 Final ISP", "2024 Final ISP", "2026 Draft ISP"]

# ---------------------------------------------------------------------------
# Output: Capacity
# ---------------------------------------------------------------------------
with PdfPages("Coal_filled_Capacity.pdf") as pdf:
    # Page 1: all releases together
    fig = plot_filled_envelope(
        df_metric         = capacity_gpg_sum,
        all_scenarios_odp = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp               = odp,
        ylabel            = "Capacity [GW]",
        title             = "Coal Capacity – ODP scenario range across ISP releases",
        max_value         = 26,
        release_order     = RELEASE_ORDER,
    )
    pdf.savefig(fig)
    plt.close(fig)

    # Pages 2–4: one per release
    plot_filled_per_release(
        df_metric           = capacity_gpg_sum,
        all_scenarios_odp   = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp                 = odp,
        ylabel              = "Capacity [GW]",
        title_prefix        = "Coal Capacity – ODP scenario range",
        max_value           = 26,
        pdf                 = pdf,
    )

print("Saved: Coal_filled_Capacity.pdf")

# ---------------------------------------------------------------------------
# Output: Utilisation Factor
# ---------------------------------------------------------------------------
with PdfPages("Coal_filled_UF.pdf") as pdf:
    fig = plot_filled_envelope(
        df_metric           = util_factor_gpg,
        all_scenarios_odp   = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp                 = odp,
        ylabel              = "UF [GWh / GWh × 100]",
        title               = "Coal Utilisation Factor – ODP scenario range across ISP releases",
        max_value           = 100,
        release_order       = RELEASE_ORDER,
    )
    pdf.savefig(fig)
    plt.close(fig)

    plot_filled_per_release(
        df_metric           = util_factor_gpg,
        all_scenarios_odp   = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp                 = odp,
        ylabel              = "UF [GWh / GWh × 100]",
        title_prefix        = "Coal Utilisation Factor – ODP scenario range",
        max_value           = 100,
        pdf                 = pdf,
    )

print("Saved: Coal_filled_UF.pdf")

# ---------------------------------------------------------------------------
# Output: Generation
# ---------------------------------------------------------------------------
with PdfPages("Coal_filled_Generation.pdf") as pdf:
    fig = plot_filled_envelope(
        df_metric           = generation_gpg_sum,
        all_scenarios_odp   = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp                 = odp,
        ylabel              = "Generation [GWh]",
        title               = "Coal Generation – ODP scenario range across ISP releases",
        max_value           = 140000,
        release_order       = RELEASE_ORDER,
    )
    pdf.savefig(fig)
    plt.close(fig)

    plot_filled_per_release(
        df_metric           = generation_gpg_sum,
        all_scenarios_odp   = all_scenarios_odp,
        reference_scenarios = reference_scenarios,
        odp                 = odp,
        ylabel              = "Generation [GWh]",
        title_prefix        = "Coal Generation – ODP scenario range",
        max_value           = 140000,
        pdf                 = pdf,
    )

print("Saved: Coal_filled_Generation.pdf")
