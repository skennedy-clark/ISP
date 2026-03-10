# -*- coding: utf-8 -*-
"""
isp_plot_gui.py
===============
PySide6 GUI for generating ISP analysis plots.

Sits in the GUI/ subdirectory. The database and plotting script live one
level up:
    ../ISP.db          — SQLite database (source of scenarios/CDPs)
    ../isp_plots.py    — plot functions imported directly

Run from the GUI/ directory:
    python isp_plot_gui.py

Dependencies:
    pip install PySide6 matplotlib pandas pyyaml
"""

import sys
import os
import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("QtAgg")
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
# Paths
# ---------------------------------------------------------------------------
GUI_DIR     = Path(__file__).parent.resolve()
PROJECT_DIR = GUI_DIR.parent
CONFIG_PATH = GUI_DIR / "config.yaml"
OUTPUT_DIR  = GUI_DIR


# ---------------------------------------------------------------------------
# Config — read/write a simple YAML file
# Uses PyYAML if available, otherwise a minimal hand-rolled parser sufficient
# for the flat key: value structure this file will always have.
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        import yaml
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        pass
    # Minimal fallback parser: key: value, one per line, # comments stripped
    cfg = {}
    for line in CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if ":" in line:
            k, _, v = line.partition(":")
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _save_config(cfg: dict):
    try:
        import yaml
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
        return
    except ImportError:
        pass
    # Minimal fallback writer
    lines = ["# isp_plot_gui configuration\n"]
    for k, v in cfg.items():
        lines.append(f'{k}: "{v}"\n')
    CONFIG_PATH.write_text("".join(lines), encoding="utf-8")


# Initialise DB_PATH from config, fall back to default ../ISP.db
_cfg = _load_config()
DB_PATH: Path = Path(_cfg["db_path"]) if "db_path" in _cfg else PROJECT_DIR / "ISP.db"


def set_db_path(new_path: Path):
    """Update the runtime DB path and persist to config.yaml."""
    global DB_PATH
    DB_PATH = new_path
    cfg = _load_config()
    cfg["db_path"] = str(new_path)
    _save_config(cfg)



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
    Return the ODP Scenario_2 for a release only if a CDP is explicitly
    labelled with '(ODP)' in its name (e.g. 'CDP4 (ODP)').
    Returns None if no such label exists — the caller must handle this
    and require the user to make an explicit selection.
    Never guesses or falls back to highest-numbered CDP.
    """
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT DISTINCT Scenario_2 FROM context
               WHERE Data_source = ?
               ORDER BY Scenario_2""",
            (release,),
        ).fetchall()
    for (cdp,) in rows:
        if cdp and ("(ODP)" in cdp or "(odp)" in cdp.lower()):
            return cdp
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
# Technology groups — all standardised names as stored in ISP.db
# Load rows are excluded from generation groups by default.
# ---------------------------------------------------------------------------

TECH_GROUPS: dict[str, list[str]] = {
    "Coal": [
        "Black Coal",
        "Brown Coal",
    ],
    "Gas": [
        "Mid-merit Gas",
        "Mid-merit Gas with CCS",
        "Peaking Gas+Liquids",
        "Flexible Gas",
        "Flexible Gas with CCS",
    ],
    "Wind": [
        "Wind",
        "Offshore Wind",
    ],
    "Solar": [
        "Utility-scale Solar",
        "Rooftop and Other Small-scale Solar",
        "Solar Thermal",
        "Distributed PV",
    ],
    "Storage": [
        "Large-scale Storage",
        "Medium Storage",
        "Shallow Storage",
        "Deep Storage",
        "Utility-scale Storage",
        # Utility-scale Storage Load excluded by default
        "Distributed Storage",
        # Distributed Storage Load excluded by default
    ],
    "Coordinated CER": [
        "Coordinated CER Storage",
        # Coordinated CER Storage Load excluded by default
        "Passive CER Storage",
        # Passive CER Storage Load excluded by default
    ],
    "Coordinated DER": [
        "Coordinated DER Storage",
        # Coordinated DER Storage Load excluded by default
    ],
    "Hydro": [
        "Hydro",
        "Snowy 2.0",
        "Borumba",
    ],
    "Hydrogen": [
        "Hydrogen Turbine",
        "Alkaline Electrolyser",
    ],
    "Other": [
        "Biomass",
        "Other Renewable Fuels",
        "DSP",
    ],
    "Custom": [],   # populated dynamically from all technologies
}

# Full list for the Custom option — load rows available but unchecked by default
ALL_TECHNOLOGIES = [
    "Alkaline Electrolyser",
    "Biomass",
    "Black Coal",
    "Borumba",
    "Brown Coal",
    "Coordinated CER Storage",
    "Coordinated CER Storage Load",
    "Coordinated DER Storage",
    "Coordinated DER Storage Load",
    "DSP",
    "Deep Storage",
    "Distributed PV",
    "Distributed Storage",
    "Distributed Storage Load",
    "Flexible Gas",
    "Flexible Gas with CCS",
    "Hydro",
    "Hydrogen Turbine",
    "Large-scale Storage",
    "Medium Storage",
    "Mid-merit Gas",
    "Mid-merit Gas with CCS",
    "Offshore Wind",
    "Other Renewable Fuels",
    "Passive CER Storage",
    "Passive CER Storage Load",
    "Peaking Gas+Liquids",
    "Rooftop and Other Small-scale Solar",
    "Shallow Storage",
    "Snowy 2.0",
    "Solar Thermal",
    "Utility-scale Solar",
    "Utility-scale Storage",
    "Utility-scale Storage Load",
    "Wind",
]

# Technologies ending in "Load" — excluded (unchecked) by default in Custom
_LOAD_TECHS = {t for t in ALL_TECHNOLOGIES if t.endswith("Load")}


class TechnologySelector(QGroupBox):
    """
    Dropdown of technology group presets + an expandable Custom area.
    Call .get_tech_filter() to get the active list of technology strings.
    Call .get_group_label() to get a short name for script generation.
    """

    def __init__(self, default_group: str = "Gas", parent=None):
        super().__init__("Technology", parent)
        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        # Group preset dropdown
        row = QHBoxLayout()
        row.addWidget(QLabel("Group:"))
        self.group_combo = QComboBox()
        self.group_combo.addItems(list(TECH_GROUPS.keys()))
        self.group_combo.setCurrentText(default_group)
        self.group_combo.currentTextChanged.connect(self._on_group_changed)
        row.addWidget(self.group_combo, 1)
        layout.addLayout(row)

        # Summary label showing active techs (hidden for Custom)
        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        self.summary_label.setStyleSheet("color: #555; font-size: 8pt;")
        layout.addWidget(self.summary_label)

        # Custom checkbox area (visible only when Custom selected)
        self.custom_area = CheckboxGroup([])
        self.custom_area.set_items(ALL_TECHNOLOGIES, checked=False)
        self.custom_area.setMinimumHeight(130)
        self.custom_area.setVisible(False)
        # Pre-check non-load techs for Custom
        for name, cb in self.custom_area._checkboxes.items():
            cb.setChecked(name not in _LOAD_TECHS)
        layout.addWidget(self.custom_area)

        # All / None buttons for Custom (hidden when not Custom)
        self._custom_btns = QWidget()
        btn_layout = QHBoxLayout(self._custom_btns)
        btn_layout.setContentsMargins(0, 0, 0, 0)
        btn_all  = QPushButton("All");  btn_all.setFixedWidth(40)
        btn_none = QPushButton("None"); btn_none.setFixedWidth(44)
        btn_all.clicked.connect(lambda: self.custom_area.check_all(True))
        btn_none.clicked.connect(lambda: self.custom_area.check_all(False))
        btn_layout.addWidget(btn_all)
        btn_layout.addWidget(btn_none)
        btn_layout.addStretch()
        self._custom_btns.setVisible(False)
        layout.addWidget(self._custom_btns)

        self._on_group_changed(default_group)

    def _on_group_changed(self, group: str):
        is_custom = (group == "Custom")
        self.custom_area.setVisible(is_custom)
        self._custom_btns.setVisible(is_custom)
        self.summary_label.setVisible(not is_custom)
        if not is_custom:
            techs = TECH_GROUPS.get(group, [])
            self.summary_label.setText(", ".join(techs) if techs else "—")

    def get_tech_filter(self) -> list[str]:
        group = self.group_combo.currentText()
        if group == "Custom":
            return self.custom_area.checked_items()
        return TECH_GROUPS.get(group, [])

    def get_group_label(self) -> str:
        return self.group_combo.currentText()


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

# ---------------------------------------------------------------------------
# Matplotlib canvas widget
# ---------------------------------------------------------------------------

class PagedPlotCanvas(QWidget):
    """
    Embeds a matplotlib figure with Prev / Next page navigation.
    Call show_figures(list_of_figs) to load a set of figures.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._figures: list = []
        self._index: int = 0

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        # Canvas
        self._placeholder_fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, "Configure selections and click Preview",
                ha="center", va="center", fontsize=14, color="#888888",
                transform=ax.transAxes)
        ax.axis("off")
        self._canvas = FigureCanvas(self._placeholder_fig)
        self._canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        layout.addWidget(self._canvas)

        # Navigation bar
        nav = QHBoxLayout()
        self._prev_btn  = QPushButton("◀  Prev")
        self._next_btn  = QPushButton("Next  ▶")
        self._page_label = QLabel("")
        self._page_label.setAlignment(Qt.AlignCenter)
        self._prev_btn.setFixedWidth(80)
        self._next_btn.setFixedWidth(80)
        self._prev_btn.clicked.connect(self._prev)
        self._next_btn.clicked.connect(self._next)
        nav.addWidget(self._prev_btn)
        nav.addStretch()
        nav.addWidget(self._page_label)
        nav.addStretch()
        nav.addWidget(self._next_btn)
        layout.addLayout(nav)

        self._update_nav()

    def show_figures(self, figures: list):
        """Load a list of matplotlib figures and show the first one."""
        self._figures = figures
        self._index   = 0
        self._show_current()

    def show_figure(self, fig):
        """Convenience — show a single figure."""
        self.show_figures([fig])

    def _show_current(self):
        if not self._figures:
            return
        fig = self._figures[self._index]
        self._canvas.figure = fig
        self._canvas.draw()
        self._update_nav()

    def _prev(self):
        if self._index > 0:
            self._index -= 1
            self._show_current()

    def _next(self):
        if self._index < len(self._figures) - 1:
            self._index += 1
            self._show_current()

    def _update_nav(self):
        n = len(self._figures)
        self._page_label.setText(f"{self._index + 1} / {n}" if n else "")
        self._prev_btn.setEnabled(self._index > 0)
        self._next_btn.setEnabled(self._index < n - 1)

    def get_figure(self):
        if self._figures:
            return self._figures[self._index]
        return self._placeholder_fig


# Default colours cycled across scenario rows
_ROW_COLOURS = [
    "#000000",  # black
    "#E07B39",  # orange
    "#4C72B0",  # blue
    "#2CA02C",  # green
    "#9467BD",  # purple
    "#D62728",  # red
    "#8C564B",  # brown
    "#E377C2",  # pink
    "#7F7F7F",  # grey
    "#17BECF",  # cyan
]

_LINESTYLES = [
    ("Solid",        "-"),
    ("Dashed",       "--"),
    ("Dotted",       ":"),
    ("Dash-dot",     "-."),
]


# ---------------------------------------------------------------------------
# Core / Sensitivity tab
# ---------------------------------------------------------------------------

class ScenarioRowWidget(QWidget):
    """
    One row: Release | CDP | Scenario | colour | linestyle | Ref | ✕
    """
    remove_requested = Signal(object)

    def __init__(self, index: int, parent=None):
        super().__init__(parent)
        self._index  = index
        self._colour = _ROW_COLOURS[index % len(_ROW_COLOURS)]

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 2, 0, 2)
        layout.setSpacing(4)

        # Release
        self.release_combo = QComboBox()
        self.release_combo.setMinimumWidth(130)
        try:
            self.release_combo.addItems(query_releases())
        except Exception:
            pass
        self.release_combo.currentTextChanged.connect(self._refresh_cdps)
        layout.addWidget(self.release_combo)

        # CDP
        self.cdp_combo = QComboBox()
        self.cdp_combo.setFixedWidth(100)
        self.cdp_combo.currentTextChanged.connect(self._refresh_scenarios)
        layout.addWidget(self.cdp_combo)

        # Scenario
        self.scenario_combo = QComboBox()
        self.scenario_combo.setMinimumWidth(180)
        layout.addWidget(self.scenario_combo, 1)

        # Colour picker
        self.colour_btn = QPushButton()
        self.colour_btn.setFixedWidth(26)
        self.colour_btn.setFixedHeight(24)
        self.colour_btn.setToolTip("Line colour")
        self._update_colour_btn()
        self.colour_btn.clicked.connect(self._pick_colour)
        layout.addWidget(self.colour_btn)

        # Linestyle
        self.style_combo = QComboBox()
        self.style_combo.setFixedWidth(82)
        for label, _ in _LINESTYLES:
            self.style_combo.addItem(label)
        layout.addWidget(self.style_combo)

        # Ref (highlight / thick line)
        self.highlight_cb = QCheckBox("Ref")
        self.highlight_cb.setToolTip("Reference scenario — plotted with linewidth 3.5")
        self.highlight_cb.setFixedWidth(42)
        layout.addWidget(self.highlight_cb)

        # Remove
        rm_btn = QPushButton("✕")
        rm_btn.setFixedWidth(26)
        rm_btn.setFixedHeight(24)
        rm_btn.clicked.connect(lambda: self.remove_requested.emit(self))
        layout.addWidget(rm_btn)

        # Populate CDPs for initial release
        self._refresh_cdps(self.release_combo.currentText())

    # ------------------------------------------------------------------
    def _refresh_cdps(self, release: str):
        self.cdp_combo.blockSignals(True)
        self.cdp_combo.clear()
        cdps = query_all_cdps_for_release(release) if release else []
        self.cdp_combo.addItems(cdps)
        self.cdp_combo.blockSignals(False)
        self._refresh_scenarios(self.cdp_combo.currentText())

    def _refresh_scenarios(self, cdp: str):
        self.scenario_combo.clear()
        release   = self.release_combo.currentText()
        scenarios = query_scenarios_for_release(release, cdp) if (release and cdp) else []
        self.scenario_combo.addItems(scenarios)

    def _pick_colour(self):
        from PySide6.QtWidgets import QColorDialog
        from PySide6.QtGui import QColor
        c = QColorDialog.getColor(QColor(self._colour), self, "Choose line colour")
        if c.isValid():
            self._colour = c.name()
            self._update_colour_btn()

    def _update_colour_btn(self):
        self.colour_btn.setStyleSheet(
            f"background-color: {self._colour}; border: 1px solid #888;"
        )

    # ------------------------------------------------------------------
    def set_values(self, release: str, cdp: str, scenario: str,
                   highlight: bool = False, colour: str = None, linestyle: str = "-"):
        if release in [self.release_combo.itemText(i) for i in range(self.release_combo.count())]:
            self.release_combo.setCurrentText(release)
        cdps = query_all_cdps_for_release(release)
        self.cdp_combo.clear()
        self.cdp_combo.addItems(cdps)
        if cdp in cdps:
            self.cdp_combo.setCurrentText(cdp)
        scenarios = query_scenarios_for_release(release, cdp)
        self.scenario_combo.clear()
        self.scenario_combo.addItems(scenarios)
        if scenario in scenarios:
            self.scenario_combo.setCurrentText(scenario)
        self.highlight_cb.setChecked(highlight)
        if colour:
            self._colour = colour
            self._update_colour_btn()
        for i, (_, ls) in enumerate(_LINESTYLES):
            if ls == linestyle:
                self.style_combo.setCurrentIndex(i)
                break

    def get_row(self) -> dict:
        ls_index = self.style_combo.currentIndex()
        linestyle = _LINESTYLES[ls_index][1] if ls_index >= 0 else "-"
        return {
            "ISP":       self.release_combo.currentText(),
            "core":      self.scenario_combo.currentText(),
            "ODP":       self.cdp_combo.currentText(),
            "highlight": self.highlight_cb.isChecked(),
            "colour":    self._colour,
            "linestyle": linestyle,
        }


class CoreSensitivityTab(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data_cache = None
        self._row_widgets: list[ScenarioRowWidget] = []

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
        left_layout.setSpacing(8)
        main_split.addWidget(left_scroll)

        # Technology
        self.tech_selector = TechnologySelector(default_group="Coal")
        left_layout.addWidget(self.tech_selector)

        # Scenario rows
        rows_box = QGroupBox("Scenarios to plot")
        rows_box_layout = QVBoxLayout(rows_box)

        # Column headers
        hdr = QHBoxLayout()
        for text, width in [("Release", 130), ("CDP", 100), ("Scenario", 0),
                            ("Col", 26), ("Style", 82), ("Ref", 42), ("", 26)]:
            lbl = QLabel(text)
            lbl.setStyleSheet("font-weight: bold; font-size: 8pt; color: #555;")
            if width:
                lbl.setFixedWidth(width)
            hdr.addWidget(lbl, 0 if width else 1)
        rows_box_layout.addLayout(hdr)

        # Scrollable rows container
        rows_scroll = QScrollArea()
        rows_scroll.setWidgetResizable(True)
        rows_scroll.setMinimumHeight(200)
        rows_scroll.setFrameShape(QFrame.NoFrame)
        self._rows_container = QWidget()
        self._rows_layout = QVBoxLayout(self._rows_container)
        self._rows_layout.setSpacing(2)
        self._rows_layout.setContentsMargins(0, 0, 0, 0)
        self._rows_layout.addStretch()
        rows_scroll.setWidget(self._rows_container)
        rows_box_layout.addWidget(rows_scroll)

        # Add row button
        add_btn = QPushButton("＋ Add scenario row")
        add_btn.setFixedHeight(28)
        add_btn.clicked.connect(lambda: self._add_row())
        rows_box_layout.addWidget(add_btn)
        left_layout.addWidget(rows_box)

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

        # Plot types
        type_box = QGroupBox("Plot types")
        type_layout = QVBoxLayout(type_box)
        self.cb_core_plot = QCheckBox("Core — one line per scenario row above")
        self.cb_sens_plot = QCheckBox("Sensitivity — all CDPs for each scenario")
        self.cb_core_plot.setChecked(True)
        self.cb_sens_plot.setChecked(True)
        type_layout.addWidget(self.cb_core_plot)
        type_layout.addWidget(self.cb_sens_plot)
        left_layout.addWidget(type_box)

        # Output
        out_box = QGroupBox("Output")
        out_layout = QVBoxLayout(out_box)
        fn_row = QHBoxLayout()
        fn_row.addWidget(QLabel("Filename:"))
        self.filename_edit = QLineEdit("isp_line_plots.pdf")
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

        # Buttons
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
        self.canvas = PagedPlotCanvas()
        main_split.addWidget(self.canvas)
        main_split.setSizes([560, 540])

        # Start with three blank rows
        for _ in range(3):
            self._add_row()

    # ------------------------------------------------------------------
    def _add_row(self, release: str = None, cdp: str = None,
                 scenario: str = None, highlight: bool = False,
                 colour: str = None, linestyle: str = "-"):
        idx    = len(self._row_widgets)
        widget = ScenarioRowWidget(idx)
        widget.remove_requested.connect(self._remove_row)
        if release and cdp and scenario:
            widget.set_values(release, cdp, scenario, highlight, colour, linestyle)
        elif colour:
            widget._colour = colour
            widget._update_colour_btn()
        # Insert before the stretch at the end
        self._rows_layout.insertWidget(self._rows_layout.count() - 1, widget)
        self._row_widgets.append(widget)

    def _remove_row(self, widget: ScenarioRowWidget):
        if len(self._row_widgets) <= 1:
            self.status_label.setText("At least one scenario row is required.")
            return
        self._row_widgets.remove(widget)
        widget.setParent(None)
        widget.deleteLater()

    # ------------------------------------------------------------------
    def _browse_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Choose output directory", str(OUTPUT_DIR))
        if d:
            self.dir_label.setText(d)

    def _get_output_path(self) -> Path:
        return Path(self.dir_label.text()) / self.filename_edit.text()

    def _build_scenario_tables(self):
        """Build core_scenarios, odp, reference_scenarios from the row widgets."""
        rows = [w.get_row() for w in self._row_widgets
                if w.get_row()["core"] and w.get_row()["ISP"]]
        if not rows:
            raise ValueError("No scenario rows configured.")

        core_scenarios = pd.DataFrame([
            {"ISP": r["ISP"], "core": r["core"], "ODP": r["ODP"],
             "colour": r["colour"], "linestyle": r["linestyle"]}
            for r in rows
        ])

        # odp: unique (release, CDP) pairs from the rows
        odp = (
            core_scenarios[["ISP", "ODP"]]
            .drop_duplicates()
            .rename(columns={"ISP": "Data_source", "ODP": "Scenario_2"})
        )

        # reference: rows with highlight checked
        ref_rows = [r for r in rows if r["highlight"]]
        reference_scenarios = pd.DataFrame([
            {"ISP": r["ISP"], "core": r["core"]}
            for r in ref_rows
        ]) if ref_rows else pd.DataFrame(columns=["ISP", "core"])

        return core_scenarios, odp, reference_scenarios

    def _get_tech_filter(self) -> list[str]:
        return self.tech_selector.get_tech_filter()

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
        """Build all selected figures. Returns list of figures."""
        core_scenarios, odp, reference_scenarios = self._build_scenario_tables()
        cap_sum, gen_sum, uf, all_scenarios = self._prepare_data()

        all_scenarios_odp = all_scenarios.merge(odp, how="inner")

        # Per-row style info keyed by (ISP, core)
        row_styles = {
            (r.get_row()["ISP"], r.get_row()["core"]): r.get_row()
            for r in self._row_widgets
        }

        highlight_isp  = reference_scenarios.iloc[0]["ISP"]  if not reference_scenarios.empty else ""
        highlight_core = reference_scenarios.iloc[0]["core"] if not reference_scenarios.empty else ""

        metrics = []
        if self.cb_capacity.isChecked():
            metrics.append((cap_sum, "Capacity [GW]",        "Capacity",   self.ylim_cap.value()))
        if self.cb_uf.isChecked():
            metrics.append((uf,      "UF [GWh / GWh x 100]", "UF",         self.ylim_uf.value()))
        if self.cb_generation.isChecked():
            metrics.append((gen_sum, "Generation [GWh]",      "Generation", self.ylim_gen.value()))

        def _draw_core(df_sum, ylabel, title, ymax):
            fig, ax = plt.subplots(figsize=(12, 8))
            for _, row in core_scenarios.iterrows():
                subset = df_sum[
                    (df_sum.Data_source == row["ISP"]) &
                    (df_sum.Scenario_1  == row["core"]) &
                    (df_sum.Scenario_2  == row["ODP"])
                ]
                style   = row_styles.get((row["ISP"], row["core"]), {})
                colour  = style.get("colour", "#000000")
                ls      = style.get("linestyle", "-")
                is_ref  = (row["ISP"] == highlight_isp and row["core"] == highlight_core)
                ax.plot(
                    subset.Year, subset.Value,
                    color     = "black" if is_ref else colour,
                    linestyle = "-" if is_ref else ls,
                    linewidth = 3.5 if is_ref else 1.5,
                    label     = f"{row['ISP']} – {row['core']}" + (" ★" if is_ref else ""),
                )
            ax.set_title(f"Core Scenarios – {title}", fontweight="bold", fontsize=16)
            ax.set_ylabel(ylabel, fontweight="bold", fontsize=14)
            ax.set_ylim(0, ymax)
            ax.legend(loc="best", fontsize=11)
            ax.grid()
            plt.tight_layout()
            return fig

        def _draw_sensitivity(df_sum, ylabel, title, ymax):
            """One figure per ISP release — all CDPs for each scenario.
            Core scenarios drawn solid, sensitivities dashed.
            Ref row drawn thick black. All other colours from row_styles."""
            figs = []
            for isp in core_scenarios["ISP"].unique():
                isp_data   = all_scenarios_odp[all_scenarios_odp.Data_source == isp].copy()
                core_names = set(core_scenarios[core_scenarios.ISP == isp]["core"].values)
                ref_match  = reference_scenarios[reference_scenarios.ISP == isp]
                highlight  = ref_match["core"].iloc[0] if not ref_match.empty else ""

                fig, ax = plt.subplots(figsize=(12, 8))
                for _, row in isp_data.iterrows():
                    subset = df_sum[
                        (df_sum.Data_source == isp) &
                        (df_sum.Scenario_1  == row.Scenario_1) &
                        (df_sum.Scenario_2  == row.Scenario_2)
                    ]
                    is_ref  = (row.Scenario_1 == highlight)
                    is_core = (row.Scenario_1 in core_names)
                    style   = row_styles.get((isp, row.Scenario_1), {})
                    colour  = style.get("colour", "#888888")
                    ls      = style.get("linestyle", "-")
                    ax.plot(
                        subset.Year, subset.Value,
                        color     = "black" if is_ref else colour,
                        linestyle = "-" if (is_ref or is_core) else "--",
                        linewidth = 3.5 if is_ref else 1.0,
                        label     = f"{row.Scenario_1} – {row.Scenario_2}" + (" ★" if is_ref else ""),
                        alpha     = 1.0 if (is_ref or is_core) else 0.6,
                    )
                ax.set_title(f"{title} – core & sensitivity: {isp}", fontweight="bold", fontsize=16)
                ax.set_ylabel(ylabel, fontweight="bold", fontsize=14)
                ax.set_ylim(0, ymax)
                ax.legend(loc="best", fontsize=9)
                ax.grid()
                plt.tight_layout()
                figs.append(fig)
            return figs

        figures = []

        if save_path:
            with PdfPages(save_path) as pdf:
                for df_sum, ylabel, title, ymax in metrics:
                    if self.cb_core_plot.isChecked():
                        fig = _draw_core(df_sum, ylabel, title, ymax)
                        pdf.savefig(fig)
                        plt.close(fig)
                        figures.append(fig)
                    if self.cb_sens_plot.isChecked():
                        for fig in _draw_sensitivity(df_sum, ylabel, title, ymax):
                            pdf.savefig(fig)
                            plt.close(fig)
                            figures.append(fig)
        else:
            for df_sum, ylabel, title, ymax in metrics:
                if self.cb_core_plot.isChecked():
                    figures.append(_draw_core(df_sum, ylabel, title, ymax))
                if self.cb_sens_plot.isChecked():
                    figures.extend(_draw_sensitivity(df_sum, ylabel, title, ymax))

        return figures


    def _preview(self):
        self.status_label.setText("Generating preview…")
        self.preview_btn.setEnabled(False)
        QApplication.processEvents()
        try:
            figs = self._build_figures(save_path=None)
            if figs:
                self.canvas.show_figures(figs)
                n = len(figs)
                self.status_label.setText(f"Preview ready — {n} page{'s' if n > 1 else ''}. Use ◀ ▶ to page through.")
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
        tech_label = self.tech_selector.get_group_label()
        tech_list  = repr(tech)

        hi_isp  = reference_scenarios.iloc[0]["ISP"]  if not reference_scenarios.empty else ""
        hi_core = reference_scenarios.iloc[0]["core"] if not reference_scenarios.empty else ""

        core_rows_code = "\n".join(
            f'    {{"ISP": {row["ISP"]!r}, "core": {row["core"]!r}, "ODP": {row["ODP"]!r}, '
            f'"colour": {row["colour"]!r}, "linestyle": {row["linestyle"]!r}}},'
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

Reproduces Core / Sensitivity line plots for {tech_label}.
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

def plot_core(df, ylabel, title, ymax):
    fig, ax = plt.subplots(figsize=(12, 8))
    for _, row in core_scenarios.iterrows():
        sub    = df[(df.Data_source==row["ISP"]) & (df.Scenario_1==row["core"]) & (df.Scenario_2==row["ODP"])]
        is_ref = (row["ISP"]==HIGHLIGHT_ISP and row["core"]==HIGHLIGHT_CORE)
        ax.plot(sub.Year, sub.Value,
                color     = "black" if is_ref else row["colour"],
                linestyle = "-" if is_ref else row["linestyle"],
                linewidth = 3.5 if is_ref else 1.5,
                label     = f"{{row[\'ISP\']}} – {{row[\'core\']}}" + (" \u2605" if is_ref else ""))
    ax.set_title(f"Core Scenarios – {{title}}", fontweight="bold", fontsize=16)
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

    def get_state(self) -> dict:
        """Return the full tab state as a serialisable dict."""
        return {
            "technology_group": self.tech_selector.group_combo.currentText(),
            "technology_custom": self.tech_selector.custom_area.checked_items(),
            "rows": [w.get_row() for w in self._row_widgets],
            "metrics": {
                "capacity":   self.cb_capacity.isChecked(),
                "uf":         self.cb_uf.isChecked(),
                "generation": self.cb_generation.isChecked(),
            },
            "ylim": {
                "cap": self.ylim_cap.value(),
                "uf":  self.ylim_uf.value(),
                "gen": self.ylim_gen.value(),
            },
            "plot_types": {
                "core":        self.cb_core_plot.isChecked(),
                "sensitivity": self.cb_sens_plot.isChecked(),
            },
            "filename":   self.filename_edit.text(),
            "output_dir": self.dir_label.text(),
        }

    def load_state(self, state: dict):
        """Restore tab state from a dict previously produced by get_state()."""
        # Technology
        group = state.get("technology_group", "Coal")
        self.tech_selector.group_combo.setCurrentText(group)
        if group == "Custom":
            custom = state.get("technology_custom", [])
            for name, cb in self.tech_selector.custom_area._checkboxes.items():
                cb.setChecked(name in custom)

        # Rows — clear existing then rebuild
        for w in list(self._row_widgets):
            self._row_widgets.remove(w)
            w.setParent(None)
            w.deleteLater()
        for row in state.get("rows", []):
            self._add_row(
                release   = row.get("ISP", ""),
                cdp       = row.get("ODP", ""),
                scenario  = row.get("core", ""),
                highlight = row.get("highlight", False),
                colour    = row.get("colour"),
                linestyle = row.get("linestyle", "-"),
            )

        # Metrics
        m = state.get("metrics", {})
        self.cb_capacity.setChecked(m.get("capacity", True))
        self.cb_uf.setChecked(m.get("uf", True))
        self.cb_generation.setChecked(m.get("generation", True))

        # Y-axis
        y = state.get("ylim", {})
        self.ylim_cap.setValue(y.get("cap", 26))
        self.ylim_uf.setValue(y.get("uf", 100))
        self.ylim_gen.setValue(y.get("gen", 140000))

        # Plot types
        pt = state.get("plot_types", {})
        self.cb_core_plot.setChecked(pt.get("core", True))
        self.cb_sens_plot.setChecked(pt.get("sensitivity", True))

        # Output
        if "filename" in state:
            self.filename_edit.setText(state["filename"])
        if "output_dir" in state:
            self.dir_label.setText(state["output_dir"])


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
        # Apply preset CDP if provided and valid
        preset_cdp = self._preset.get("cdp")
        if preset_cdp and preset_cdp in cdps:
            self.cdp_combo.setCurrentText(preset_cdp)
        else:
            # Only auto-select if explicitly labelled (ODP) in the DB — never guess
            odp = query_odp_for_release(release)
            if odp and odp in cdps:
                self.cdp_combo.setCurrentText(odp)
            # else: leave combo on whatever the DB returns first and let user choose
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
        self.tech_selector = TechnologySelector(default_group="Gas")
        self._left_layout.addWidget(self.tech_selector)

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
        self.canvas = PagedPlotCanvas()
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
        return self.tech_selector.get_tech_filter()

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
        tech_label = self.tech_selector.get_group_label()
        # Use a safe Python identifier for the variable name in the generated script
        tech_var   = tech_label.lower().replace(" ", "_").replace("/", "_")

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
                figs  = [f[0] for f in figures]
                names = [f[1] for f in figures]
                self.canvas.show_figures(figs)
                n = len(figs)
                self.status_label.setText(
                    f"Preview ready — {n} page{'s' if n > 1 else ''} "
                    f"({', '.join(names)}). Use ◀ ▶ to page through."
                )
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

    def get_state(self) -> dict:
        """Return full tab state as a serialisable dict."""
        return {
            "technology_group":  self.tech_selector.group_combo.currentText(),
            "technology_custom": self.tech_selector.custom_area.checked_items(),
            "title":             self.title_edit.text(),
            "bands": [w.to_group_dict() or {} for w in self._band_widgets],
            "metrics": {
                "capacity":   self.cb_cap.isChecked(),
                "uf":         self.cb_uf.isChecked(),
                "generation": self.cb_gen.isChecked(),
            },
            "ylim": {
                "cap": self.ylim_cap.value(),
                "uf":  self.ylim_uf.value(),
                "gen": self.ylim_gen.value(),
            },
            "filename":   self.filename_edit.text(),
            "output_dir": self.dir_label.text(),
        }

    def load_state(self, state: dict):
        """Restore tab state from a dict previously produced by get_state()."""
        # Technology
        group = state.get("technology_group", "Gas")
        self.tech_selector.group_combo.setCurrentText(group)
        if group == "Custom":
            custom = state.get("technology_custom", [])
            for name, cb in self.tech_selector.custom_area._checkboxes.items():
                cb.setChecked(name in custom)

        # Title
        if "title" in state:
            self.title_edit.setText(state["title"])

        # Bands — clear existing then rebuild
        for w in list(self._band_widgets):
            self._band_widgets.remove(w)
            w.setParent(None)
            w.deleteLater()
        for band in state.get("bands", []):
            if band:
                self._add_band(preset=band)

        # Metrics
        m = state.get("metrics", {})
        self.cb_cap.setChecked(m.get("capacity", True))
        self.cb_uf.setChecked(m.get("uf", True))
        self.cb_gen.setChecked(m.get("generation", True))

        # Y-axis
        y = state.get("ylim", {})
        self.ylim_cap.setValue(y.get("cap", 26))
        self.ylim_uf.setValue(y.get("uf", 100))
        self.ylim_gen.setValue(y.get("gen", 140000))

        # Output
        if "filename" in state:
            self.filename_edit.setText(state["filename"])
        if "output_dir" in state:
            self.dir_label.setText(state["output_dir"])


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ISP Plot Generator")
        self.resize(1100, 780)

        central = QWidget()
        central_layout = QVBoxLayout(central)
        central_layout.setContentsMargins(6, 6, 6, 4)
        central_layout.setSpacing(4)
        self.setCentralWidget(central)

        # ---- DB settings bar ----
        db_box = QGroupBox("Database")
        db_box.setMaximumHeight(64)
        db_layout = QHBoxLayout(db_box)
        db_layout.setContentsMargins(6, 4, 6, 4)

        db_layout.addWidget(QLabel("ISP.db path:"))
        self.db_path_edit = QLineEdit(str(DB_PATH))
        self.db_path_edit.setReadOnly(True)
        self.db_path_edit.setStyleSheet(
            "background: #f5f5f5;" if DB_PATH.exists()
            else "background: #fdecea; color: #c0392b;"
        )
        db_layout.addWidget(self.db_path_edit, 1)

        browse_btn = QPushButton("Browse…")
        browse_btn.setFixedWidth(75)
        browse_btn.clicked.connect(self._browse_db)
        db_layout.addWidget(browse_btn)

        self.db_status_label = QLabel(
            "✔  Found" if DB_PATH.exists() else "✘  Not found"
        )
        self.db_status_label.setStyleSheet(
            "color: #27ae60;" if DB_PATH.exists() else "color: #c0392b;"
        )
        self.db_status_label.setFixedWidth(90)
        db_layout.addWidget(self.db_status_label)

        # Session save/load buttons in the DB bar
        db_layout.addSpacing(12)
        save_session_btn = QPushButton("💾  Save session")
        load_session_btn = QPushButton("📂  Load session")
        save_session_btn.setFixedWidth(115)
        load_session_btn.setFixedWidth(115)
        save_session_btn.setToolTip("Save current tab state to a YAML session file")
        load_session_btn.setToolTip("Load a previously saved YAML session file")
        save_session_btn.clicked.connect(self._save_session)
        load_session_btn.clicked.connect(self._load_session)
        db_layout.addWidget(save_session_btn)
        db_layout.addWidget(load_session_btn)

        central_layout.addWidget(db_box)

        # ---- Tabs ----
        self.tabs = QTabWidget()
        self._line_tab   = CoreSensitivityTab()
        self._filled_tab = FilledBandTab()
        self.tabs.addTab(self._line_tab,   "Line Plots")
        self.tabs.addTab(self._filled_tab, "Filled Band Comparison")
        central_layout.addWidget(self.tabs)

        self.statusBar().showMessage(f"Config: {CONFIG_PATH}")

        if not DB_PATH.exists():
            QMessageBox.warning(
                self,
                "Database not found",
                f"ISP.db not found at:\n{DB_PATH}\n\n"
                "Use the Browse button to locate it. "
                "The path will be saved to config.yaml for future sessions.",
            )

    def _browse_db(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Locate ISP.db", str(DB_PATH.parent),
            "SQLite databases (*.db *.sqlite *.sqlite3);;All files (*)"
        )
        if not path:
            return
        new_path = Path(path)
        set_db_path(new_path)
        self.db_path_edit.setText(str(new_path))
        exists = new_path.exists()
        self.db_path_edit.setStyleSheet(
            "background: #f5f5f5;" if exists else "background: #fdecea; color: #c0392b;"
        )
        self.db_status_label.setText("✔  Found" if exists else "✘  Not found")
        self.db_status_label.setStyleSheet(
            "color: #27ae60;" if exists else "color: #c0392b;"
        )
        self.statusBar().showMessage(f"DB path updated and saved to {CONFIG_PATH}")

    def _save_session(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save session", str(OUTPUT_DIR / "session.yaml"),
            "YAML files (*.yaml *.yml);;All files (*)"
        )
        if not path:
            return
        session = {
            "line_plots":   self._line_tab.get_state(),
            "filled_bands": self._filled_tab.get_state(),
        }
        _save_config(session)  # reuse the same writer
        # _save_config writes to CONFIG_PATH — we want a separate file here
        try:
            import yaml
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(session, f, default_flow_style=False, allow_unicode=True)
        except ImportError:
            lines = ["# isp_plot_gui session\n"]
            import json
            lines.append(json.dumps(session, indent=2))
            Path(path).write_text("".join(lines), encoding="utf-8")
        self.statusBar().showMessage(f"Session saved: {path}")

    def _load_session(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load session", str(OUTPUT_DIR),
            "YAML files (*.yaml *.yml);;All files (*)"
        )
        if not path:
            return
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                session = yaml.safe_load(f)
        except ImportError:
            import json
            text = Path(path).read_text(encoding="utf-8")
            # Strip leading comment line if present
            lines = [l for l in text.splitlines() if not l.startswith("#")]
            session = json.loads("\n".join(lines))
        except Exception as e:
            QMessageBox.critical(self, "Load error", f"Could not read session file:\n{e}")
            return

        if "line_plots" in session:
            self._line_tab.load_state(session["line_plots"])
        if "filled_bands" in session:
            self._filled_tab.load_state(session["filled_bands"])
        self.statusBar().showMessage(f"Session loaded: {path}")


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


if __name__ == "__main__":
    main()