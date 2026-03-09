# -*- coding: utf-8 -*-
"""
isp_plot_gui.py
===============
PySide6 GUI for generating ISP coal analysis plots.

Sits in the GUI/ subdirectory. The database and plotting script live one
level up:
    ../ISP.db          — SQLite database (source of scenarios/CDPs)
    ../isp_plots.py    — plot functions imported directly

Run from the GUI/ directory:
    python isp_plot_gui.py

Dependencies:
    pip install PySide6 matplotlib pandas
"""

import sys
import os
import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("QtAgg")   # interactive Qt backend — must be set before pyplot import
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_pdf import PdfPages

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QGroupBox, QLabel, QCheckBox, QComboBox, QPushButton,
    QSpinBox, QDoubleSpinBox, QScrollArea, QFileDialog, QSplitter,
    QFrame, QLineEdit, QMessageBox, QProgressBar, QSizePolicy,
    QButtonGroup, QRadioButton, QListWidget, QListWidgetItem,
)
from PySide6.QtCore import Qt, QThread, Signal, QObject
from PySide6.QtGui import QFont

# ---------------------------------------------------------------------------
# Paths — GUI/ is one level below the project root
# ---------------------------------------------------------------------------
GUI_DIR     = Path(__file__).parent.resolve()
PROJECT_DIR = GUI_DIR.parent
DB_PATH     = PROJECT_DIR / "ISP.db"
OUTPUT_DIR  = GUI_DIR   # default PDF output location

# ---------------------------------------------------------------------------
# Import plot functions from the parent script.
# isp_plots.py runs module-level code (data loading, plotting) on import,
# which we must prevent. We import only the function objects we need by
# temporarily redirecting the module so its __name__ != '__main__' guard
# fires, but the top-level statements still run.
#
# Cleaner solution: the plot functions are self-contained — we redefine only
# what we call, pulling them in via exec into a dedicated namespace so the
# module-level side effects (get_data(), plot calls) are never executed.
# ---------------------------------------------------------------------------

def _load_plot_functions():
    """
    Load only the function definitions from isp_plots.py without executing
    the module-level data loading and plotting calls.

    We read the source, strip everything after the last function definition,
    and exec the remainder into an isolated namespace.
    """
    src_path = PROJECT_DIR / "isp_plots.py"
    if not src_path.exists():
        raise FileNotFoundError(f"isp_plots.py not found at {src_path}")

    source = src_path.read_text(encoding="utf-8")

    # Split at the data-loading block (the first non-def/non-class top-level
    # statement after the function definitions). The marker is the get_data("%")
    # call which starts the module-level execution.
    marker = "\n(\n    capacity,"
    cutoff = source.find(marker)
    if cutoff == -1:
        # Fallback: find the load block comment
        marker2 = "# Load data"
        cutoff = source.find(marker2)

    functions_source = source[:cutoff] if cutoff != -1 else source

    ns: dict = {}
    exec(compile(functions_source, str(src_path), "exec"), ns)
    return ns


try:
    _plot_ns = _load_plot_functions()
    plot_core_scenarios       = _plot_ns["plot_core_scenarios"]
    plot_sensitivity_scenarios = _plot_ns["plot_sensitivity_scenarios"]
    plot_all_cdps             = _plot_ns["plot_all_cdps"]
    plot_stack_by_reg         = _plot_ns["plot_stack_by_reg"]
    _PLOTS_LOADED = True
except Exception as e:
    _PLOTS_LOADED = False
    _PLOT_LOAD_ERROR = str(e)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def db_connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"ISP.db not found at {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def query_releases() -> list[str]:
    """Return all distinct Data_source values, sorted."""
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT Data_source FROM context ORDER BY Data_source"
        ).fetchall()
    return [r[0] for r in rows]


def query_odp_for_release(release: str) -> str | None:
    """
    Return the ODP Scenario_2 for a release by finding the CDP that appears
    in the mapping table with '(ODP)' in its name, or fall back to the
    highest-numbered CDP.
    """
    with db_connect() as conn:
        # Prefer any CDP labelled as ODP
        rows = conn.execute(
            """SELECT DISTINCT Original_value FROM mapping
               WHERE Data_source = ? AND Attribute_type = 'Scenario_2'
               ORDER BY Original_value""",
            (release,),
        ).fetchall()
    cdps = [r[0] for r in rows]
    for cdp in cdps:
        if "(ODP)" in cdp or "(odp)" in cdp.lower():
            return cdp
    # Fall back to last alphabetically (CDP12 > CDP9 needs numeric sort)
    if cdps:
        def _cdp_sort(s):
            import re
            m = re.search(r"(\d+)", s)
            return int(m.group(1)) if m else 0
        return sorted(cdps, key=_cdp_sort)[-1]
    return None


def query_scenarios_for_release(release: str, cdp: str) -> list[str]:
    """Return Scenario_1 values for a release at a given CDP, sorted."""
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT DISTINCT Scenario_1 FROM context
               WHERE Data_source = ? AND Scenario_2 = ?
               ORDER BY Scenario_1""",
            (release, cdp),
        ).fetchall()
    return [r[0] for r in rows]


def query_all_cdps_for_release(release: str) -> list[str]:
    """Return all CDPs for a release."""
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT DISTINCT Scenario_2 FROM context
               WHERE Data_source = ?
               ORDER BY Scenario_2""",
            (release,),
        ).fetchall()
    return [r[0] for r in rows]


def load_data_from_db() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load capacity, generation, context — mirrors get_data('%') in isp_plots.py."""
    with db_connect() as conn:
        capacity = pd.read_sql(
            """SELECT a.Id, a.Variable, a.Year, a.Value,
                      v.Data_source, v.Scenario_1, v.Scenario_2,
                      v.State, v.Region, v.Technology
               FROM data a
               INNER JOIN v_context_with_region v ON a.Id = v.Id
               WHERE a.Variable = 'capacity'""",
            conn,
        )
        generation = pd.read_sql(
            """SELECT a.Id, a.Variable, a.Year, a.Value,
                      v.Data_source, v.Scenario_1, v.Scenario_2,
                      v.State, v.Region, v.Technology
               FROM data a
               INNER JOIN v_context_with_region v ON a.Id = v.Id
               WHERE a.Variable = 'generation'""",
            conn,
        )
        context = pd.read_sql(
            "SELECT * FROM v_context_with_region",
            conn,
        )

    for df in (capacity, generation):
        df.drop(df[df.Year.isin(["Existing and Committed", "Un33"])].index, inplace=True)
        df.dropna(subset=["Value", "Scenario_2"], inplace=True)
        df["Year"] = df["Year"].astype(int)

    capacity["Value"] = capacity["Value"].astype(float) / 1000
    generation["Value"] = generation["Value"].astype(float)

    return capacity, generation, context


# ---------------------------------------------------------------------------
# Worker thread — runs plotting in background so GUI stays responsive
# ---------------------------------------------------------------------------

class PlotWorker(QObject):
    finished  = Signal(object)   # emits the figure (or list of figures)
    error     = Signal(str)
    progress  = Signal(str)

    def __init__(self, task_fn, *args, **kwargs):
        super().__init__()
        self._task_fn = task_fn
        self._args    = args
        self._kwargs  = kwargs

    def run(self):
        try:
            result = self._task_fn(*self._args, **self._kwargs)
            self.finished.emit(result)
        except Exception as e:
            import traceback
            self.error.emit(traceback.format_exc())


# ---------------------------------------------------------------------------
# Reusable: scrollable checkbox group
# ---------------------------------------------------------------------------

class CheckboxGroup(QScrollArea):
    """A scrollable list of labelled checkboxes."""

    def __init__(self, items: list[str], parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.StyledPanel)
        self.setMinimumHeight(120)

        inner = QWidget()
        self._layout = QVBoxLayout(inner)
        self._layout.setSpacing(2)
        self._layout.setContentsMargins(4, 4, 4, 4)
        self._checkboxes: dict[str, QCheckBox] = {}
        self.setWidget(inner)

        self.set_items(items)

    def set_items(self, items: list[str], checked: bool = True):
        # Clear existing
        for cb in self._checkboxes.values():
            cb.setParent(None)
        self._checkboxes.clear()

        for item in items:
            cb = QCheckBox(item)
            cb.setChecked(checked)
            self._layout.addWidget(cb)
            self._checkboxes[item] = cb

        self._layout.addStretch()

    def checked_items(self) -> list[str]:
        return [k for k, cb in self._checkboxes.items() if cb.isChecked()]

    def check_all(self, state: bool):
        for cb in self._checkboxes.values():
            cb.setChecked(state)


# ---------------------------------------------------------------------------
# Release panel — one collapsible block per ISP release
# ---------------------------------------------------------------------------

class ReleasePanel(QGroupBox):
    """
    Controls for a single ISP release:
      - Enable/disable checkbox in title
      - ODP selector (auto-populated from DB)
      - Scenario checkboxes (populated from DB for chosen ODP)
      - ODP/reference scenario selector
    """

    def __init__(self, release: str, parent=None):
        super().__init__(parent)
        self.release = release
        self.setCheckable(True)
        self.setChecked(True)
        self.setTitle(release)

        odp = query_odp_for_release(release) or ""
        all_cdps = query_all_cdps_for_release(release)

        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        # ODP row
        odp_row = QHBoxLayout()
        odp_row.addWidget(QLabel("ODP CDP:"))
        self.odp_combo = QComboBox()
        self.odp_combo.addItems(all_cdps)
        if odp in all_cdps:
            self.odp_combo.setCurrentText(odp)
        self.odp_combo.currentTextChanged.connect(self._refresh_scenarios)
        odp_row.addWidget(self.odp_combo)
        layout.addLayout(odp_row)

        # Scenarios
        layout.addWidget(QLabel("Core scenarios:"))
        scenarios = query_scenarios_for_release(release, odp) if odp else []
        self.scenario_group = CheckboxGroup(scenarios)
        layout.addWidget(self.scenario_group)

        # Select all / none
        btn_row = QHBoxLayout()
        btn_all  = QPushButton("All")
        btn_none = QPushButton("None")
        btn_all.setFixedWidth(50)
        btn_none.setFixedWidth(50)
        btn_all.clicked.connect(lambda: self.scenario_group.check_all(True))
        btn_none.clicked.connect(lambda: self.scenario_group.check_all(False))
        btn_row.addWidget(btn_all)
        btn_row.addWidget(btn_none)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # Highlight (reference) scenario
        hi_row = QHBoxLayout()
        hi_row.addWidget(QLabel("Highlight:"))
        self.highlight_combo = QComboBox()
        self.highlight_combo.addItems(["(none)"] + scenarios)
        hi_row.addWidget(self.highlight_combo)
        layout.addLayout(hi_row)

    def _refresh_scenarios(self, cdp: str):
        scenarios = query_scenarios_for_release(self.release, cdp)
        self.scenario_group.set_items(scenarios)
        self.highlight_combo.clear()
        self.highlight_combo.addItems(["(none)"] + scenarios)

    def is_active(self) -> bool:
        return self.isChecked()

    def get_odp(self) -> str:
        return self.odp_combo.currentText()

    def get_selected_scenarios(self) -> list[str]:
        return self.scenario_group.checked_items()

    def get_highlight(self) -> str | None:
        h = self.highlight_combo.currentText()
        return None if h == "(none)" else h


# ---------------------------------------------------------------------------
# Matplotlib canvas widget
# ---------------------------------------------------------------------------

class PlotCanvas(QWidget):
    """Embeds a matplotlib figure with navigation toolbar."""

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self._fig, self._ax = plt.subplots(figsize=(10, 6))
        self._canvas = FigureCanvas(self._fig)
        self._canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        layout.addWidget(self._canvas)

        self._placeholder()

    def _placeholder(self):
        self._ax.clear()
        self._ax.text(
            0.5, 0.5, "Configure selections and click Preview",
            ha="center", va="center", fontsize=14, color="#888888",
            transform=self._ax.transAxes,
        )
        self._ax.axis("off")
        self._canvas.draw()

    def show_figure(self, fig: plt.Figure):
        """Replace current figure with a new one."""
        self._canvas.figure = fig
        self._canvas.draw()
        self._fig = fig

    def get_figure(self) -> plt.Figure:
        return self._fig


# ---------------------------------------------------------------------------
# Core / Sensitivity tab
# ---------------------------------------------------------------------------

class CoreSensitivityTab(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data_cache = None   # loaded once, reused

        main_split = QSplitter(Qt.Horizontal, self)
        main_layout = QVBoxLayout(self)
        main_layout.addWidget(main_split)

        # ---- Left: controls ----
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setMinimumWidth(320)
        left_widget = QWidget()
        left_scroll.setWidget(left_widget)
        left_layout = QVBoxLayout(left_widget)
        left_layout.setSpacing(10)
        main_split.addWidget(left_scroll)

        # Technology filter
        tech_box = QGroupBox("Technology")
        tech_layout = QVBoxLayout(tech_box)
        self.coal_rb  = QRadioButton("Coal (Black Coal + Brown Coal)")
        self.gpg_rb   = QRadioButton("Gas (Mid-merit + Peaking + Flexible)")
        self.coal_rb.setChecked(True)
        tech_layout.addWidget(self.coal_rb)
        tech_layout.addWidget(self.gpg_rb)
        left_layout.addWidget(tech_box)

        # Release panels — populated from DB
        releases_box = QGroupBox("ISP Releases && Scenarios")
        releases_layout = QVBoxLayout(releases_box)
        self._release_panels: dict[str, ReleasePanel] = {}
        try:
            releases = query_releases()
        except Exception:
            releases = []

        for rel in releases:
            panel = ReleasePanel(rel)
            releases_layout.addWidget(panel)
            self._release_panels[rel] = panel

        left_layout.addWidget(releases_box)

        # Metrics
        metrics_box = QGroupBox("Metrics to plot")
        metrics_layout = QVBoxLayout(metrics_box)
        self.cb_capacity   = QCheckBox("Capacity [GW]")
        self.cb_uf         = QCheckBox("Utilisation Factor [%]")
        self.cb_generation = QCheckBox("Generation [GWh]")
        self.cb_capacity.setChecked(True)
        self.cb_uf.setChecked(True)
        self.cb_generation.setChecked(True)
        for cb in (self.cb_capacity, self.cb_uf, self.cb_generation):
            metrics_layout.addWidget(cb)
        left_layout.addWidget(metrics_box)

        # Y-axis limits
        ylim_box = QGroupBox("Y-axis limits")
        ylim_form = QHBoxLayout(ylim_box)
        ylim_left = QVBoxLayout()
        ylim_right = QVBoxLayout()
        for label, attr, default in [
            ("Capacity max (GW):", "ylim_cap", 26),
            ("UF max (%):",        "ylim_uf",  100),
            ("Generation max:",    "ylim_gen",  140000),
        ]:
            ylim_left.addWidget(QLabel(label))
            spin = QSpinBox()
            spin.setRange(1, 9_999_999)
            spin.setValue(default)
            spin.setSingleStep(1000 if default > 1000 else 1)
            setattr(self, attr, spin)
            ylim_right.addWidget(spin)
        ylim_form.addLayout(ylim_left)
        ylim_form.addLayout(ylim_right)
        left_layout.addWidget(ylim_box)

        # Plot type
        type_box = QGroupBox("Plot types")
        type_layout = QVBoxLayout(type_box)
        self.cb_core_plot    = QCheckBox("Core scenarios (ODP)")
        self.cb_sens_plot    = QCheckBox("Sensitivity scenarios (ODP)")
        self.cb_allcdp_plot  = QCheckBox("All CDPs per scenario")
        self.cb_core_plot.setChecked(True)
        self.cb_sens_plot.setChecked(True)
        self.cb_allcdp_plot.setChecked(False)
        for cb in (self.cb_core_plot, self.cb_sens_plot, self.cb_allcdp_plot):
            type_layout.addWidget(cb)
        left_layout.addWidget(type_box)

        # Output
        out_box = QGroupBox("Output")
        out_layout = QVBoxLayout(out_box)
        fn_row = QHBoxLayout()
        fn_row.addWidget(QLabel("Filename:"))
        self.filename_edit = QLineEdit("Coal_Cap_UF_Core_ODP_and_ODP_all_sensitivity.pdf")
        fn_row.addWidget(self.filename_edit)
        out_layout.addLayout(fn_row)

        dir_row = QHBoxLayout()
        self.dir_label = QLabel(str(OUTPUT_DIR))
        self.dir_label.setWordWrap(True)
        browse_btn = QPushButton("Browse…")
        browse_btn.setFixedWidth(70)
        browse_btn.clicked.connect(self._browse_dir)
        dir_row.addWidget(self.dir_label, 1)
        dir_row.addWidget(browse_btn)
        out_layout.addLayout(dir_row)
        left_layout.addWidget(out_box)

        # Action buttons
        btn_row = QHBoxLayout()
        self.preview_btn = QPushButton("▶  Preview")
        self.save_btn    = QPushButton("💾  Save PDF")
        self.export_btn  = QPushButton("🐍  Export script")
        self.preview_btn.setFixedHeight(36)
        self.save_btn.setFixedHeight(36)
        self.export_btn.setFixedHeight(36)
        self.preview_btn.clicked.connect(self._preview)
        self.save_btn.clicked.connect(self._save_pdf)
        self.export_btn.clicked.connect(self._export_script)
        btn_row.addWidget(self.preview_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.export_btn)
        left_layout.addLayout(btn_row)

        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        left_layout.addWidget(self.status_label)
        left_layout.addStretch()

        # ---- Right: preview ----
        self.canvas = PlotCanvas()
        main_split.addWidget(self.canvas)
        main_split.setSizes([360, 700])

    def _browse_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Choose output directory", str(OUTPUT_DIR))
        if d:
            self.dir_label.setText(d)

    def _get_output_path(self) -> Path:
        return Path(self.dir_label.text()) / self.filename_edit.text()

    def _build_scenario_tables(self):
        """
        Build core_scenarios, odp, reference_scenarios, all_scenarios_odp
        from the current GUI selections.
        """
        core_rows = []
        odp_rows  = []
        ref_rows  = []

        for rel, panel in self._release_panels.items():
            if not panel.is_active():
                continue
            cdp       = panel.get_odp()
            scenarios = panel.get_selected_scenarios()
            highlight = panel.get_highlight()

            for scen in scenarios:
                core_rows.append({"ISP": rel, "core": scen, "ODP": cdp})

            odp_rows.append({"Data_source": rel, "Scenario_2": cdp})

            if highlight:
                ref_rows.append({"ISP": rel, "core": highlight})
            elif scenarios:
                ref_rows.append({"ISP": rel, "core": scenarios[0]})

        if not core_rows:
            raise ValueError("No scenarios selected. Enable at least one release and select scenarios.")

        core_scenarios    = pd.DataFrame(core_rows)
        odp               = pd.DataFrame(odp_rows)
        reference_scenarios = pd.DataFrame(ref_rows)

        return core_scenarios, odp, reference_scenarios

    def _get_tech_filter(self) -> list[str]:
        if self.coal_rb.isChecked():
            return ["Black Coal", "Brown Coal"]
        return ["Mid-merit Gas", "Mid-merit Gas with CCS",
                "Peaking Gas+Liquids", "Flexible Gas", "Flexible Gas with CCS"]

    def _prepare_data(self):
        """Load and aggregate data. Cached after first load."""
        if self._data_cache is None:
            self.status_label.setText("Loading data from ISP.db…")
            QApplication.processEvents()
            capacity, generation, context = load_data_from_db()
            self._data_cache = (capacity, generation, context)

        capacity, generation, context = self._data_cache
        tech = self._get_tech_filter()

        cap  = capacity[capacity.Technology.isin(tech)].copy()
        gen  = generation[generation.Technology.isin(tech)].copy()
        cap["max_annual_gen"] = cap["Value"] * 24 * 365

        grp = ["Data_source", "Scenario_1", "Scenario_2", "Year"]
        cap_sum = cap.groupby(grp, as_index=False).agg({"Value": "sum", "max_annual_gen": "sum"})
        gen_sum = gen.groupby(grp, as_index=False)["Value"].sum()

        uf = cap_sum[grp + ["max_annual_gen"]].merge(gen_sum, on=grp, how="inner")
        uf["Value"] = uf["Value"] / uf["max_annual_gen"] * 100

        all_scenarios = (
            context[["Data_source", "Scenario_1", "Scenario_2"]]
            .drop_duplicates()
            .dropna(subset=["Data_source", "Scenario_1", "Scenario_2"])
            .reset_index(drop=True)
        )

        return cap_sum, gen_sum, uf, all_scenarios

    def _build_figures(self, save_path: Path | None = None):
        """Build all selected figures. Returns list of (fig, title) tuples."""
        core_scenarios, odp, reference_scenarios = self._build_scenario_tables()
        cap_sum, gen_sum, uf, all_scenarios = self._prepare_data()

        all_scenarios_odp = all_scenarios.merge(odp, how="inner")

        # Determine highlight for plot_core_scenarios
        if not reference_scenarios.empty:
            highlight_isp  = reference_scenarios.iloc[0]["ISP"]
            highlight_core = reference_scenarios.iloc[0]["core"]
        else:
            highlight_isp  = core_scenarios.iloc[0]["ISP"]
            highlight_core = core_scenarios.iloc[0]["core"]

        metrics = []
        if self.cb_capacity.isChecked():
            metrics.append((cap_sum, "Capacity [GW]",        "Capacity",   self.ylim_cap.value()))
        if self.cb_uf.isChecked():
            metrics.append((uf,      "UF [GWh / GWh x 100]", "UF",         self.ylim_uf.value()))
        if self.cb_generation.isChecked():
            metrics.append((gen_sum, "Generation [GWh]",      "Generation", self.ylim_gen.value()))

        figures = []

        if save_path:
            with PdfPages(save_path) as pdf:
                for df_sum, ylabel, title, ymax in metrics:
                    if self.cb_core_plot.isChecked():
                        fig = plot_core_scenarios(
                            highlight_isp, highlight_core,
                            core_scenarios.copy(), df_sum, ylabel, title, ymax,
                        )
                        pdf.savefig(fig)
                        figures.append(fig)

                    if self.cb_sens_plot.isChecked():
                        # plot_sensitivity_scenarios writes directly to pdf
                        plot_sensitivity_scenarios(
                            core_scenarios.copy(), all_scenarios_odp.copy(),
                            reference_scenarios.copy(), df_sum,
                            ylabel, title, ymax, pdf,
                        )

                    if self.cb_allcdp_plot.isChecked():
                        plot_all_cdps(
                            all_scenarios_odp.copy(), all_scenarios.copy(),
                            core_scenarios.copy(), odp.copy(),
                            df_sum, ylabel, f"{title} - Coal", ymax,
                        )
        else:
            # Preview mode — core scenarios only (first metric)
            if metrics:
                df_sum, ylabel, title, ymax = metrics[0]
                fig = plot_core_scenarios(
                    highlight_isp, highlight_core,
                    core_scenarios.copy(), df_sum, ylabel, title, ymax,
                )
                figures.append(fig)

        return figures

    def _preview(self):
        self.status_label.setText("Generating preview…")
        self.preview_btn.setEnabled(False)
        QApplication.processEvents()
        try:
            figs = self._build_figures(save_path=None)
            if figs:
                self.canvas.show_figure(figs[0])
                self.status_label.setText("Preview ready.")
            else:
                self.status_label.setText("No metrics selected.")
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            QMessageBox.critical(self, "Preview error", str(e))
        finally:
            self.preview_btn.setEnabled(True)

    def _export_script(self):
        """Generate a standalone reproduction script for the current Core/Sensitivity selections."""
        try:
            core_scenarios, odp, reference_scenarios = self._build_scenario_tables()
        except ValueError as e:
            QMessageBox.warning(self, "Export error", str(e))
            return

        tech       = self._get_tech_filter()
        is_coal    = self.coal_rb.isChecked()
        tech_label = "Coal" if is_coal else "Gas"
        tech_list  = repr(tech)

        hi_isp  = reference_scenarios.iloc[0]["ISP"]  if not reference_scenarios.empty else ""
        hi_core = reference_scenarios.iloc[0]["core"] if not reference_scenarios.empty else ""

        core_rows_code = "\n".join(
            f'    {{"ISP": {row["ISP"]!r}, "core": {row["core"]!r}, "ODP": {row["ODP"]!r}}},'
            for _, row in core_scenarios.iterrows()
        )
        odp_rows_code = "\n".join(
            f'    {{"Data_source": {row["Data_source"]!r}, "Scenario_2": {row["Scenario_2"]!r}}},'
            for _, row in odp.iterrows()
        )
        ref_rows_code = "\n".join(
            f'    {{"ISP": {row["ISP"]!r}, "core": {row["core"]!r}}},'
            for _, row in reference_scenarios.iterrows()
        )

        metrics_lines = []
        if self.cb_capacity.isChecked():
            metrics_lines.append(f'    ("capacity_sum",   "Capacity [GW]",         "Capacity",   {self.ylim_cap.value()}),')
        if self.cb_uf.isChecked():
            metrics_lines.append(f'    ("util_factor",    "UF [GWh / GWh x 100]",  "UF",         {self.ylim_uf.value()}),')
        if self.cb_generation.isChecked():
            metrics_lines.append(f'    ("generation_sum", "Generation [GWh]",       "Generation", {self.ylim_gen.value()}),')
        metrics_code = "\n".join(metrics_lines)

        do_core   = self.cb_core_plot.isChecked()
        do_sens   = self.cb_sens_plot.isChecked()
        do_allcdp = self.cb_allcdp_plot.isChecked()

        import datetime
        now    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        prefix = Path(self.filename_edit.text()).stem

        sql_rows = "\n".join(
            f"          ('{row['ISP']}', '{row['core']}', '{row['ODP']}'),"
            for _, row in core_scenarios.iterrows()
        )

        script = f'''\
# -*- coding: utf-8 -*-
"""
{prefix}_isp_plots.py
Generated by isp_plot_gui.py on {now}

Reproduces Core / Sensitivity / All-CDP line plots for {tech_label}.
Run from the project root (where ISP.db lives):
    python GUI/{prefix}_isp_plots.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SQL — rows that feed these plots
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    SELECT a.Id, a.Variable, a.Year, a.Value,
           v.Data_source, v.Scenario_1, v.Scenario_2,
           v.State, v.Region, v.Technology
    FROM data a
    INNER JOIN v_context_with_region v ON a.Id = v.Id
    WHERE a.Variable IN (\'capacity\', \'generation\')
      -- Technology filter (applied in Python):
      -- Technology IN {tech}
      -- Scenario / CDP filter (ODP rows only):
      AND (v.Data_source, v.Scenario_1, v.Scenario_2) IN (
{sql_rows}
      )
    ORDER BY v.Data_source, v.Scenario_1, v.Scenario_2, a.Year;

Notes:
  v_context_with_region fills NULL Region with synthetic state codes
  (N0, Q0, V0, S0, T0) for state-level ISP releases (2022 Final,
  2024 Draft). NEM totals are obtained by groupby without Region to
  avoid double-counting across storage levels.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3, os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# ============================================================================
# CONFIGURATION — edit here to change what is plotted
# ============================================================================

core_scenarios = pd.DataFrame([
{core_rows_code}
])

odp = pd.DataFrame([
{odp_rows_code}
])

reference_scenarios = pd.DataFrame([
{ref_rows_code}
])

TECH_FILTER    = {tech_list}
HIGHLIGHT_ISP  = {hi_isp!r}
HIGHLIGHT_CORE = {hi_core!r}

# (dataframe_key, ylabel, title_label, y_max)
METRICS = [
{metrics_code}
]

DO_CORE_PLOT   = {do_core}
DO_SENS_PLOT   = {do_sens}
DO_ALLCDP_PLOT = {do_allcdp}

OUTPUT_PDF = {str(Path(self.filename_edit.text()))!r}


# ============================================================================
# DATA LOADING
# ============================================================================

def get_data():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ISP.db")
    conn    = sqlite3.connect(db_path)
    cap = pd.read_sql(
        """SELECT a.Id, a.Variable, a.Year, a.Value,
                  v.Data_source, v.Scenario_1, v.Scenario_2,
                  v.State, v.Region, v.Technology
           FROM data a
           INNER JOIN v_context_with_region v ON a.Id = v.Id
           WHERE a.Variable = \'capacity\'""", conn)
    gen = pd.read_sql(
        """SELECT a.Id, a.Variable, a.Year, a.Value,
                  v.Data_source, v.Scenario_1, v.Scenario_2,
                  v.State, v.Region, v.Technology
           FROM data a
           INNER JOIN v_context_with_region v ON a.Id = v.Id
           WHERE a.Variable = \'generation\'""", conn)
    ctx = pd.read_sql("SELECT * FROM v_context_with_region", conn)
    conn.close()
    for df in (cap, gen):
        df.drop(df[df.Year.isin(["Existing and Committed", "Un33"])].index, inplace=True)
        df.dropna(subset=["Value", "Scenario_2"], inplace=True)
        df["Year"] = df["Year"].astype(int)
    cap["Value"] = cap["Value"].astype(float) / 1000
    gen["Value"] = gen["Value"].astype(float)
    return cap, gen, ctx


# ============================================================================
# AGGREGATION
# ============================================================================

print("Loading data...")
cap_raw, gen_raw, context = get_data()

cap = cap_raw[cap_raw.Technology.isin(TECH_FILTER)].copy()
gen = gen_raw[gen_raw.Technology.isin(TECH_FILTER)].copy()
cap["max_annual_gen"] = cap["Value"] * 24 * 365

grp = ["Data_source", "Scenario_1", "Scenario_2", "Year"]
capacity_sum   = cap.groupby(grp, as_index=False).agg({{"Value": "sum", "max_annual_gen": "sum"}})
generation_sum = gen.groupby(grp, as_index=False)["Value"].sum()
util_factor    = capacity_sum[grp + ["max_annual_gen"]].merge(generation_sum, on=grp, how="inner")
util_factor["Value"] = util_factor["Value"] / util_factor["max_annual_gen"] * 100

all_scenarios     = context[["Data_source","Scenario_1","Scenario_2"]].drop_duplicates().dropna().reset_index(drop=True)
all_scenarios_odp = all_scenarios.merge(odp, how="inner")

DATA = {{"capacity_sum": capacity_sum, "util_factor": util_factor, "generation_sum": generation_sum}}
print("Data loaded.")


# ============================================================================
# PLOT FUNCTIONS
# ============================================================================

def _color(name):
    m = {{"Step Change": "black", "Slow Change": "red", "Progressive Change": "blue",
          "Hydrogen Superpower": "green", "Green Energy Exports": "green",
          "Slower Growth": "purple", "Accelerated Transition": "teal"}}
    return m.get(name.split(" -")[0], "gray")


def plot_core(df, ylabel, title, ymax):
    styles = ["--", "-", ":"]
    isp_ls = {{isp: styles[i % len(styles)] for i, isp in enumerate(core_scenarios["ISP"].unique())}}
    fig, ax = plt.subplots(figsize=(12, 8))
    for _, row in core_scenarios.iterrows():
        sub  = df[(df.Data_source==row["ISP"]) & (df.Scenario_1==row["core"]) & (df.Scenario_2==row["ODP"])]
        ref  = (row["ISP"]==HIGHLIGHT_ISP and row["core"]==HIGHLIGHT_CORE)
        ax.plot(sub.Year, sub.Value,
                color="black" if ref else _color(row["core"]),
                linestyle="-" if ref else isp_ls[row["ISP"]],
                linewidth=3.5 if ref else 1.5,
                label=f"{{row[\'ISP\']}} – {{row[\'core\']}}" + (" (Reference)" if ref else ""))
    ax.set_title(f"Core Scenarios ODP – {{title}}", fontweight="bold", fontsize=16)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=14)
    ax.set_ylim(0, ymax); ax.legend(loc="best", fontsize=11); ax.grid()
    plt.tight_layout()
    return fig


def plot_sensitivity(df, ylabel, title, ymax, pdf):
    for isp in core_scenarios["ISP"].unique():
        isp_data   = all_scenarios_odp[all_scenarios_odp.Data_source==isp].copy()
        ref_match  = reference_scenarios[reference_scenarios.ISP==isp]
        highlight  = ref_match["core"].iloc[0] if not ref_match.empty else ""
        core_names = core_scenarios[core_scenarios.ISP==isp]["core"].values
        fig, ax    = plt.subplots(figsize=(12, 8))
        for _, row in isp_data.iterrows():
            sub  = df[(df.Data_source==isp) & (df.Scenario_1==row.Scenario_1) & (df.Scenario_2==row.Scenario_2)]
            ref  = (row.Scenario_1==highlight)
            core = (row.Scenario_1 in core_names)
            ax.plot(sub.Year, sub.Value,
                    color="black" if ref else _color(row.Scenario_1),
                    linestyle="-" if (ref or core) else "--",
                    linewidth=3.5 if ref else 1.5,
                    label=f"{{row.Scenario_1}} – {{row.Scenario_2}}")
        ax.set_title(f"{{title}} ODP – core & sensitivity: {{isp}}", fontweight="bold", fontsize=16)
        ax.set_ylabel(ylabel, fontweight="bold", fontsize=14)
        ax.set_ylim(0, ymax); ax.legend(loc="best", fontsize=10); ax.grid()
        plt.tight_layout(); pdf.savefig(fig); plt.close(fig)


# ============================================================================
# RENDER
# ============================================================================

with PdfPages(OUTPUT_PDF) as pdf:
    for df_key, ylabel, title, ymax in METRICS:
        df = DATA[df_key]
        if DO_CORE_PLOT:
            fig = plot_core(df, ylabel, title, ymax)
            pdf.savefig(fig); plt.close(fig)
            print(f"  [Core] {{title}}")
        if DO_SENS_PLOT:
            plot_sensitivity(df, ylabel, title, ymax, pdf)
            print(f"  [Sensitivity] {{title}}")

print(f"Saved: {{OUTPUT_PDF}}")
'''

        out_dir  = Path(self.dir_label.text())
        out_path = out_dir / f"{prefix}_isp_plots.py"
        out_path.write_text(script, encoding="utf-8")
        self.status_label.setText(f"Script exported: {out_path.name}")
        import subprocess, sys as _sys
        if _sys.platform == "win32":
            os.startfile(out_path)
        elif _sys.platform == "darwin":
            subprocess.Popen(["open", str(out_path)])
        else:
            subprocess.Popen(["xdg-open", str(out_path)])

    def _save_pdf(self):
        out = self._get_output_path()
        self.status_label.setText(f"Saving to {out.name}…")
        self.save_btn.setEnabled(False)
        QApplication.processEvents()
        try:
            self._build_figures(save_path=out)
            self.status_label.setText(f"Saved: {out}")
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            QMessageBox.critical(self, "Save error", str(e))
        finally:
            self.save_btn.setEnabled(True)


def _tmp_path() -> str:
    """Return a temporary file path for preview-only PDF writes that are immediately discarded."""
    import tempfile
    return tempfile.mktemp(suffix=".pdf")


# ---------------------------------------------------------------------------
# Filled band functions (self-contained, no import from filled plots scripts)
# ---------------------------------------------------------------------------

def build_band(df_metric: pd.DataFrame, isp: str, cdp: str, scenarios: list):
    """Compute per-year min/max envelope across a set of scenarios."""
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
    wide = pd.concat(pieces, axis=1).sort_index()
    return wide.min(axis=1), wide.max(axis=1)


def plot_comparison_page(df_metric, comparison_set, ylabel, max_value):
    """Draw one filled-band comparison figure and return it."""
    fig, ax = plt.subplots(figsize=(13, 8))
    for group in comparison_set["groups"]:
        env_min, env_max = build_band(
            df_metric, group["isp"], group["cdp"], group["scenarios"],
        )
        if env_min is None:
            continue
        ax.fill_between(
            env_min.index, env_min, env_max,
            alpha=group["alpha"], color=group["colour"],
            label=group["label"], linewidth=0,
        )
    ax.set_title(comparison_set["title"], fontweight="bold", fontsize=15)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=13)
    ax.set_ylim(0, max_value)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="best", fontsize=11, framealpha=0.9)
    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Filled band tab
# ---------------------------------------------------------------------------

# Default band colours cycled across groups
_BAND_COLOURS = ["#E07B39", "#4C72B0", "#2CA02C", "#9467BD", "#D62728",
                 "#8C564B", "#E377C2", "#7F7F7F", "#BCBD22", "#17BECF"]


class BandGroupWidget(QGroupBox):
    """
    One row of controls representing a single filled band (comparison group).
    Emits remove_requested when the user clicks the Remove button.
    """

    remove_requested = Signal(object)   # passes self

    def __init__(self, index: int, colour: str, preset: dict | None = None, parent=None):
        super().__init__(f"Band {index + 1}", parent)
        self._index = index
        self._preset = preset or {}
        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        # Row 1: label + colour + alpha
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Label:"))
        self.label_edit = QLineEdit(self._preset.get("label", f"Band {index + 1}"))
        row1.addWidget(self.label_edit, 2)
        self.colour_btn = QPushButton()
        self.colour_btn.setFixedWidth(32)
        self._colour = self._preset.get("colour", colour)
        self._update_colour_btn()
        self.colour_btn.clicked.connect(self._pick_colour)
        row1.addWidget(self.colour_btn)
        row1.addWidget(QLabel("α:"))
        self.alpha_spin = QDoubleSpinBox()
        self.alpha_spin.setRange(0.05, 1.0)
        self.alpha_spin.setSingleStep(0.05)
        self.alpha_spin.setValue(self._preset.get("alpha", 0.35))
        self.alpha_spin.setFixedWidth(60)
        row1.addWidget(self.alpha_spin)
        layout.addLayout(row1)

        # Row 2: ISP release + CDP
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Release:"))
        self.release_combo = QComboBox()
        try:
            self.release_combo.addItems(query_releases())
        except Exception:
            pass
        if "isp" in self._preset and self._preset["isp"] in [
            self.release_combo.itemText(i) for i in range(self.release_combo.count())
        ]:
            self.release_combo.setCurrentText(self._preset["isp"])
        self.release_combo.currentTextChanged.connect(self._refresh_cdps)
        row2.addWidget(self.release_combo, 2)
        row2.addWidget(QLabel("CDP:"))
        self.cdp_combo = QComboBox()
        self.cdp_combo.setFixedWidth(110)
        row2.addWidget(self.cdp_combo)
        layout.addLayout(row2)

        # Row 3: scenarios
        layout.addWidget(QLabel("Scenarios (band spans min–max across all selected):"))
        self.scenario_group = CheckboxGroup([])
        self.scenario_group.setMinimumHeight(90)
        layout.addWidget(self.scenario_group)

        sc_btn_row = QHBoxLayout()
        btn_all  = QPushButton("All");  btn_all.setFixedWidth(40)
        btn_none = QPushButton("None"); btn_none.setFixedWidth(44)
        btn_all.clicked.connect(lambda: self.scenario_group.check_all(True))
        btn_none.clicked.connect(lambda: self.scenario_group.check_all(False))
        sc_btn_row.addWidget(btn_all)
        sc_btn_row.addWidget(btn_none)
        sc_btn_row.addStretch()
        # Remove button
        self.remove_btn = QPushButton("✕ Remove band")
        self.remove_btn.setFixedWidth(110)
        self.remove_btn.clicked.connect(lambda: self.remove_requested.emit(self))
        sc_btn_row.addWidget(self.remove_btn)
        layout.addLayout(sc_btn_row)

        # Populate CDPs for initial release (will also populate scenarios)
        self._refresh_cdps(self.release_combo.currentText())

    def _refresh_cdps(self, release: str):
        self.cdp_combo.clear()
        if not release:
            return
        cdps = query_all_cdps_for_release(release)
        self.cdp_combo.addItems(cdps)
        # Prefer preset CDP, else auto-select ODP
        preset_cdp = self._preset.get("cdp")
        if preset_cdp and preset_cdp in cdps:
            self.cdp_combo.setCurrentText(preset_cdp)
        else:
            odp = query_odp_for_release(release)
            if odp and odp in cdps:
                self.cdp_combo.setCurrentText(odp)
        self._refresh_scenarios(self.cdp_combo.currentText())
        self.cdp_combo.currentTextChanged.connect(self._refresh_scenarios)

    def _refresh_scenarios(self, cdp: str):
        release   = self.release_combo.currentText()
        scenarios = query_scenarios_for_release(release, cdp) if cdp else []
        preset_scenarios = self._preset.get("scenarios")
        if preset_scenarios:
            # Add all scenarios but only check the preset ones
            self.scenario_group.set_items(scenarios, checked=False)
            for name, cb in self.scenario_group._checkboxes.items():
                if name in preset_scenarios:
                    cb.setChecked(True)
        else:
            self.scenario_group.set_items(scenarios, checked=True)

    def _pick_colour(self):
        from PySide6.QtWidgets import QColorDialog
        from PySide6.QtGui import QColor
        c = QColorDialog.getColor(QColor(self._colour), self, "Choose band colour")
        if c.isValid():
            self._colour = c.name()
            self._update_colour_btn()

    def _update_colour_btn(self):
        self.colour_btn.setStyleSheet(
            f"background-color: {self._colour}; border: 1px solid #888;"
        )

    def to_group_dict(self) -> dict | None:
        """Return a group dict for COMPARISON_SETS, or None if incomplete."""
        scenarios = self.scenario_group.checked_items()
        if not scenarios:
            return None
        return {
            "label":     self.label_edit.text() or f"Band {self._index + 1}",
            "colour":    self._colour,
            "alpha":     self.alpha_spin.value(),
            "isp":       self.release_combo.currentText(),
            "cdp":       self.cdp_combo.currentText(),
            "scenarios": scenarios,
        }


class FilledBandTab(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data_cache = None
        self._band_widgets: list[BandGroupWidget] = []

        main_split = QSplitter(Qt.Horizontal, self)
        top_layout = QVBoxLayout(self)
        top_layout.addWidget(main_split)

        # ---- Left: controls ----
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setMinimumWidth(360)
        left_widget = QWidget()
        left_scroll.setWidget(left_widget)
        self._left_layout = QVBoxLayout(left_widget)
        self._left_layout.setSpacing(8)
        main_split.addWidget(left_scroll)

        # Technology
        tech_box = QGroupBox("Technology")
        tech_layout = QVBoxLayout(tech_box)
        self.coal_rb = QRadioButton("Coal (Black Coal + Brown Coal)")
        self.gpg_rb  = QRadioButton("Gas (Mid-merit + Peaking + Flexible)")
        self.coal_rb.setChecked(False)
        self.gpg_rb.setChecked(True)
        tech_layout.addWidget(self.coal_rb)
        tech_layout.addWidget(self.gpg_rb)
        self._left_layout.addWidget(tech_box)

        # Chart title
        title_row = QHBoxLayout()
        title_row.addWidget(QLabel("Chart title:"))
        self.title_edit = QLineEdit("All ODP scenarios — comparing ISP releases")
        self._left_layout.addLayout(title_row)
        title_row.addWidget(self.title_edit, 1)

        # Bands — inserted here dynamically
        self._bands_container = QWidget()
        self._bands_layout    = QVBoxLayout(self._bands_container)
        self._bands_layout.setSpacing(6)
        self._bands_layout.setContentsMargins(0, 0, 0, 0)
        self._left_layout.addWidget(self._bands_container)

        # Add band button
        add_btn = QPushButton("＋ Add comparison band")
        add_btn.setFixedHeight(32)
        add_btn.clicked.connect(self._add_band)
        self._left_layout.addWidget(add_btn)

        # Metrics + y-axis
        metrics_box = QGroupBox("Metrics && Y-axis limits")
        mf = QVBoxLayout(metrics_box)
        for label, attr_cb, attr_ylim, default_ylim in [
            ("Capacity [GW]",        "cb_cap", "ylim_cap",  26),
            ("Utilisation Factor [%]","cb_uf",  "ylim_uf",  100),
            ("Generation [GWh]",     "cb_gen", "ylim_gen",  140000),
        ]:
            row = QHBoxLayout()
            cb = QCheckBox(label)
            cb.setChecked(True)
            setattr(self, attr_cb, cb)
            row.addWidget(cb, 2)
            row.addWidget(QLabel("Max:"))
            spin = QSpinBox()
            spin.setRange(1, 9_999_999)
            spin.setValue(default_ylim)
            spin.setSingleStep(1000 if default_ylim > 1000 else 1)
            spin.setFixedWidth(90)
            setattr(self, attr_ylim, spin)
            row.addWidget(spin)
            mf.addLayout(row)
        self._left_layout.addWidget(metrics_box)

        # Output
        out_box = QGroupBox("Output")
        out_layout = QVBoxLayout(out_box)
        fn_row = QHBoxLayout()
        fn_row.addWidget(QLabel("Filename prefix:"))
        self.filename_edit = QLineEdit("filled_comparison")
        fn_row.addWidget(self.filename_edit)
        out_layout.addLayout(fn_row)
        dir_row = QHBoxLayout()
        self.dir_label = QLabel(str(OUTPUT_DIR))
        self.dir_label.setWordWrap(True)
        browse_btn = QPushButton("Browse…")
        browse_btn.setFixedWidth(70)
        browse_btn.clicked.connect(self._browse_dir)
        dir_row.addWidget(self.dir_label, 1)
        dir_row.addWidget(browse_btn)
        out_layout.addLayout(dir_row)
        self._left_layout.addWidget(out_box)

        # Buttons
        btn_row = QHBoxLayout()
        self.preview_btn = QPushButton("▶  Preview")
        self.save_btn    = QPushButton("💾  Save PDFs")
        self.export_btn  = QPushButton("🐍  Export script")
        self.preview_btn.setFixedHeight(36)
        self.save_btn.setFixedHeight(36)
        self.export_btn.setFixedHeight(36)
        self.preview_btn.clicked.connect(self._preview)
        self.save_btn.clicked.connect(self._save)
        self.export_btn.clicked.connect(self._export_script)
        btn_row.addWidget(self.preview_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.export_btn)
        self._left_layout.addLayout(btn_row)

        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        self._left_layout.addWidget(self.status_label)
        self._left_layout.addStretch()

        # ---- Right: preview ----
        self.canvas = PlotCanvas()
        main_split.addWidget(self.canvas)
        main_split.setSizes([400, 700])

        # Default bands: Gas all-ODP scenarios for each ISP release
        _GPG_ODP_DEFAULTS = [
            {
                "label":     "2022 Final ISP – all ODP scenarios",
                "colour":    "#E07B39",
                "alpha":     0.35,
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
                "label":     "2024 Final ISP – all ODP scenarios",
                "colour":    "#4C72B0",
                "alpha":     0.35,
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
                "label":     "2026 Draft ISP – all ODP scenarios",
                "colour":    "#2CA02C",
                "alpha":     0.35,
                "isp":       "2026 Draft ISP",
                "cdp":       "CDP4 (ODP)",
                "scenarios": [
                    "Slower Growth - Core",
                    "Accelerated Transition - Core",
                    "Step Change - Core",
                ],
            },
        ]
        for preset in _GPG_ODP_DEFAULTS:
            self._add_band(preset=preset)

    def _add_band(self, preset: dict | None = None):
        idx    = len(self._band_widgets)
        colour = _BAND_COLOURS[idx % len(_BAND_COLOURS)]
        widget = BandGroupWidget(idx, colour, preset=preset)
        widget.remove_requested.connect(self._remove_band)
        self._bands_layout.addWidget(widget)
        self._band_widgets.append(widget)

    def _remove_band(self, widget: BandGroupWidget):
        if len(self._band_widgets) <= 1:
            self.status_label.setText("At least one band is required.")
            return
        self._band_widgets.remove(widget)
        widget.setParent(None)
        widget.deleteLater()

    def _browse_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Choose output directory", str(OUTPUT_DIR))
        if d:
            self.dir_label.setText(d)

    def _get_tech_filter(self) -> list[str]:
        if self.coal_rb.isChecked():
            return ["Black Coal", "Brown Coal"]
        return ["Mid-merit Gas", "Mid-merit Gas with CCS",
                "Peaking Gas+Liquids", "Flexible Gas", "Flexible Gas with CCS"]

    def _prepare_data(self):
        if self._data_cache is None:
            self.status_label.setText("Loading data from ISP.db…")
            QApplication.processEvents()
            capacity, generation, _ = load_data_from_db()
            self._data_cache = (capacity, generation)

        capacity, generation = self._data_cache
        tech = self._get_tech_filter()

        cap = capacity[capacity.Technology.isin(tech)].copy()
        gen = generation[generation.Technology.isin(tech)].copy()
        cap["max_annual_gen"] = cap["Value"] * 24 * 365

        grp = ["Data_source", "Scenario_1", "Scenario_2", "Year"]
        cap_sum = cap.groupby(grp, as_index=False).agg({"Value": "sum", "max_annual_gen": "sum"})
        gen_sum = gen.groupby(grp, as_index=False)["Value"].sum()

        uf = cap_sum[grp + ["max_annual_gen"]].merge(gen_sum, on=grp, how="inner")
        uf["Value"] = uf["Value"] / uf["max_annual_gen"] * 100

        return cap_sum, uf, gen_sum

    def _build_comparison_set(self) -> dict:
        groups = []
        for w in self._band_widgets:
            g = w.to_group_dict()
            if g:
                groups.append(g)
        if not groups:
            raise ValueError("No bands configured. Add at least one band with scenarios selected.")
        return {"title": self.title_edit.text(), "groups": groups}

    def _run(self, save: bool) -> list:
        comparison_set = self._build_comparison_set()
        cap_sum, uf, gen_sum = self._prepare_data()

        metrics = []
        if self.cb_cap.isChecked():
            metrics.append((cap_sum, "Capacity [GW]",         self.ylim_cap.value(), "Capacity"))
        if self.cb_uf.isChecked():
            metrics.append((uf,      "UF [GWh / GWh × 100]",  self.ylim_uf.value(),  "UF"))
        if self.cb_gen.isChecked():
            metrics.append((gen_sum, "Generation [GWh]",       self.ylim_gen.value(), "Generation"))

        if not metrics:
            raise ValueError("No metrics selected.")

        prefix  = self.filename_edit.text()
        out_dir = Path(self.dir_label.text())
        figures = []

        for df_metric, ylabel, ymax, metric_name in metrics:
            page_set = {
                **comparison_set,
                "title": f"{metric_name}: {comparison_set['title']}",
            }
            fig = plot_comparison_page(df_metric, page_set, ylabel, ymax)
            figures.append((fig, metric_name))

            if save:
                out_path = out_dir / f"{prefix}_{metric_name}.pdf"
                with PdfPages(out_path) as pdf:
                    pdf.savefig(fig)
                plt.close(fig)

        return figures

    def _export_script(self):
        """Generate a standalone filled-band reproduction script from current GUI state."""
        try:
            comparison_set = self._build_comparison_set()
        except ValueError as e:
            QMessageBox.warning(self, "Export error", str(e))
            return

        tech       = self._get_tech_filter()
        is_coal    = self.coal_rb.isChecked()
        tech_label = "Coal" if is_coal else "Gas"
        tech_var   = "coal" if is_coal else "gpg"

        import datetime
        now    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        prefix = self.filename_edit.text()

        def _fmt_scenarios(scenarios):
            return "\n".join(f'                    {s!r},' for s in scenarios)

        groups_code = ""
        for g in comparison_set["groups"]:
            groups_code += f'''\
            {{
                "label":     {g["label"]!r},
                "colour":    {g["colour"]!r},
                "alpha":     {g["alpha"]},
                "isp":       {g["isp"]!r},
                "cdp":       {g["cdp"]!r},
                "scenarios": [
{_fmt_scenarios(g["scenarios"])}
                ],
            }},
'''

        comp_set_code = f'''\
    {{
        "title": {comparison_set["title"]!r},
        "groups": [
{groups_code}        ],
    }},'''

        sql_filters = "\n".join(
            f"          ('{g['isp']}', '{s}', '{g['cdp']}'),"
            for g in comparison_set["groups"]
            for s in g["scenarios"]
        )

        max_cap = self.ylim_cap.value()
        max_uf  = self.ylim_uf.value()
        max_gen = self.ylim_gen.value()

        metrics_entries = []
        if self.cb_cap.isChecked():
            metrics_entries.append(
                f'    {{"name": "Capacity",   "df": "{tech_var}_cap_sum", "ylabel": "Capacity [GW]",        "max_value": {max_cap}, "outfile": "{prefix}_Capacity.pdf"}},')
        if self.cb_uf.isChecked():
            metrics_entries.append(
                f'    {{"name": "UF",         "df": "{tech_var}_uf",      "ylabel": "UF [GWh / GWh x 100]", "max_value": {max_uf},  "outfile": "{prefix}_UF.pdf"}},')
        if self.cb_gen.isChecked():
            metrics_entries.append(
                f'    {{"name": "Generation", "df": "{tech_var}_gen_sum", "ylabel": "Generation [GWh]",      "max_value": {max_gen}, "outfile": "{prefix}_Generation.pdf"}},')
        metrics_code = "\n".join(metrics_entries)

        script = f'''\
# -*- coding: utf-8 -*-
"""
{prefix}_filled.py
Generated by isp_plot_gui.py on {now}

Produces filled-band comparison charts for {tech_label} technology.
One PDF per metric (Capacity, UF, Generation); one page per COMPARISON_SET.

Run from the project root (where ISP.db lives):
    python GUI/{prefix}_filled.py

======================================================================
SQL — rows that feed the bands
======================================================================

    SELECT a.Id, a.Variable, a.Year, a.Value,
           v.Data_source, v.Scenario_1, v.Scenario_2,
           v.State, v.Region, v.Technology
    FROM data a
    INNER JOIN v_context_with_region v ON a.Id = v.Id
    WHERE a.Variable IN (\'capacity\', \'generation\')
      -- Technology filter applied in Python after load:
      -- Technology IN {tech}
      -- Each row below is one (ISP release, Scenario_1, CDP) combination
      -- that contributes to a band. The band spans min-to-max across all
      -- Scenario_1 values sharing the same ISP and CDP.
      AND (v.Data_source, v.Scenario_1, v.Scenario_2) IN (
{sql_filters}
      )
    ORDER BY v.Data_source, v.Scenario_1, v.Scenario_2, a.Year;

Notes:
  v_context_with_region fills NULL Region with synthetic state codes
  (N0, Q0, V0, S0, T0) for state-level ISP releases (2022 Final,
  2024 Draft). NEM totals are obtained by groupby without Region to
  avoid double-counting across storage levels.
======================================================================
"""

import sqlite3
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# ============================================================================
# CONFIGURE COMPARISON SETS — captured from GUI session {now}
# ============================================================================
# Each dict produces one page per output PDF.
# "scenarios": the Scenario_1 values whose min/max envelope forms the band.
# A single-item list collapses to a line.
# ============================================================================

COMPARISON_SETS = [
{comp_set_code}
]

MAX_CAPACITY   = {max_cap}
MAX_UF         = {max_uf}
MAX_GENERATION = {max_gen}

TECH_FILTER = {repr(tech)}


# ============================================================================
# DATA LOADING
# ============================================================================

def get_data():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ISP.db")
    conn    = sqlite3.connect(db_path)
    cap = pd.read_sql(
        """SELECT a.Id, a.Variable, a.Year, a.Value,
                  v.Data_source, v.Scenario_1, v.Scenario_2,
                  v.State, v.Region, v.Technology
           FROM data a
           INNER JOIN v_context_with_region v ON a.Id = v.Id
           WHERE a.Variable = \'capacity\'""", conn)
    gen = pd.read_sql(
        """SELECT a.Id, a.Variable, a.Year, a.Value,
                  v.Data_source, v.Scenario_1, v.Scenario_2,
                  v.State, v.Region, v.Technology
           FROM data a
           INNER JOIN v_context_with_region v ON a.Id = v.Id
           WHERE a.Variable = \'generation\'""", conn)
    conn.close()
    for df in (cap, gen):
        df.drop(df[df.Year.isin(["Existing and Committed", "Un33"])].index, inplace=True)
        df.dropna(subset=["Value", "Scenario_2"], inplace=True)
        df["Year"] = df["Year"].astype(int)
    cap["Value"] = cap["Value"].astype(float) / 1000
    gen["Value"] = gen["Value"].astype(float)
    return cap, gen


# ============================================================================
# AGGREGATION
# ============================================================================

print("Loading data from ISP.db...")
cap_raw, gen_raw = get_data()

cap = cap_raw[cap_raw.Technology.isin(TECH_FILTER)].copy()
gen = gen_raw[gen_raw.Technology.isin(TECH_FILTER)].copy()
cap["max_annual_gen"] = cap["Value"] * 24 * 365

grp = ["Data_source", "Scenario_1", "Scenario_2", "Year"]
{tech_var}_cap_sum = cap.groupby(grp, as_index=False).agg({{"Value": "sum", "max_annual_gen": "sum"}})
{tech_var}_gen_sum = gen.groupby(grp, as_index=False)["Value"].sum()
{tech_var}_uf      = (
    {tech_var}_cap_sum[grp + ["max_annual_gen"]]
    .merge({tech_var}_gen_sum, on=grp, how="inner")
)
{tech_var}_uf["Value"] = {tech_var}_uf["Value"] / {tech_var}_uf["max_annual_gen"] * 100

print(f"Data loaded. Rendering {{len(COMPARISON_SETS)}} comparison set(s) x 3 metrics...\\n")


# ============================================================================
# PLOT FUNCTIONS
# ============================================================================

def build_band(df_metric, isp, cdp, scenarios):
    pieces = []
    for scen in scenarios:
        sub = df_metric[
            (df_metric["Data_source"] == isp) &
            (df_metric["Scenario_1"]  == scen) &
            (df_metric["Scenario_2"]  == cdp)
        ][["Year", "Value"]].set_index("Year").rename(columns={{"Value": scen}})
        if not sub.empty:
            pieces.append(sub)
    if not pieces:
        return None, None
    wide = pd.concat(pieces, axis=1).sort_index()
    return wide.min(axis=1), wide.max(axis=1)


def plot_comparison_page(df_metric, comparison_set, ylabel, max_value):
    fig, ax = plt.subplots(figsize=(13, 8))
    for group in comparison_set["groups"]:
        env_min, env_max = build_band(
            df_metric, group["isp"], group["cdp"], group["scenarios"])
        if env_min is None:
            print(f"  WARNING: no data for \'{{group[\'label\']}}\' "
                  f"(ISP={{group[\'isp\']}}, CDP={{group[\'cdp\']}})")
            continue
        ax.fill_between(env_min.index, env_min, env_max,
                        alpha=group["alpha"], color=group["colour"],
                        label=group["label"], linewidth=0)
    ax.set_title(comparison_set["title"], fontweight="bold", fontsize=15)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=13)
    ax.set_ylim(0, max_value)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="best", fontsize=11, framealpha=0.9)
    plt.tight_layout()
    return fig


# ============================================================================
# RENDER
# ============================================================================

METRICS = [
{metrics_code}
]

for metric in METRICS:
    with PdfPages(metric["outfile"]) as pdf:
        for cset in COMPARISON_SETS:
            page_cset = {{**cset, "title": f"{{metric[\'name\']}}: {{cset[\'title\']}}"}}
            fig = plot_comparison_page(
                df_metric      = locals()[metric["df"]],
                comparison_set = page_cset,
                ylabel         = metric["ylabel"],
                max_value      = metric["max_value"],
            )
            pdf.savefig(fig)
            plt.close(fig)
            print(f"  [{{metric[\'name\']}}] {{cset[\'title\']}}")
    print(f"Saved: {{metric[\'outfile\']}}\\n")

print("Done.")
'''

        out_dir  = Path(self.dir_label.text())
        out_path = out_dir / f"{prefix}_filled.py"
        out_path.write_text(script, encoding="utf-8")
        self.status_label.setText(f"Script exported: {out_path.name}")
        import subprocess, sys as _sys
        if _sys.platform == "win32":
            os.startfile(out_path)
        elif _sys.platform == "darwin":
            subprocess.Popen(["open", str(out_path)])
        else:
            subprocess.Popen(["xdg-open", str(out_path)])

    def _preview(self):
        self.preview_btn.setEnabled(False)
        self.status_label.setText("Generating preview…")
        QApplication.processEvents()
        try:
            figures = self._run(save=False)
            if figures:
                # Show first metric in canvas
                self.canvas.show_figure(figures[0][0])
                shown = figures[0][1]
                rest  = [f[1] for f in figures[1:]]
                msg   = f"Previewing: {shown}."
                if rest:
                    msg += f"  ({', '.join(rest)} also generated — save to PDF to view all.)"
                self.status_label.setText(msg)
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            QMessageBox.critical(self, "Preview error", str(e))
        finally:
            self.preview_btn.setEnabled(True)

    def _save(self):
        self.save_btn.setEnabled(False)
        out_dir = Path(self.dir_label.text())
        prefix  = self.filename_edit.text()
        self.status_label.setText(f"Saving to {out_dir}…")
        QApplication.processEvents()
        try:
            self._run(save=True)
            self.status_label.setText(f"Saved: {prefix}_Capacity/UF/Generation.pdf → {out_dir}")
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            QMessageBox.critical(self, "Save error", str(e))
        finally:
            self.save_btn.setEnabled(True)



# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ISP Coal Plot Generator")
        self.resize(1100, 750)

        # Status bar shows DB path
        self.statusBar().showMessage(f"Database: {DB_PATH}")

        tabs = QTabWidget()
        tabs.addTab(CoreSensitivityTab(), "Core / Sensitivity / All-CDP")
        tabs.addTab(FilledBandTab(),      "Filled Band Comparison")
        self.setCentralWidget(tabs)

        if not _PLOTS_LOADED:
            QMessageBox.warning(
                self,
                "Could not load plot functions",
                f"isp_plots.py could not be imported:\n\n{_PLOT_LOAD_ERROR}\n\n"
                "Check that isp_plots.py is in the parent directory.",
            )

        if not DB_PATH.exists():
            QMessageBox.warning(
                self,
                "Database not found",
                f"ISP.db not found at:\n{DB_PATH}\n\n"
                "Check that the GUI/ folder is inside the project directory.",
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    font = QFont("Segoe UI", 9) if sys.platform == "win32" else QFont("SF Pro Text", 10)
    app.setFont(font)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name_
_ == "__main__":
    main()