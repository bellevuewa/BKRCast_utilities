"""
Parcel Processor – Windows Python Program (PyQt6)

Jan 9, 2026

Purpose:
Process a unified parcel dataset from multiple jurisdiction-specific source files:
- Bellevue
- Bellevue Fringe
- Kirkland
- Kirkland Fringe
- Redmond
- Redmond Fringe
- Outside BKR

User selects source files via GUI. Program extracts appropriate parcels from each source
(using jurisdiction/fringe flags or other rules) and merges them into a single output file.

Log file records all input files, record counts, errors, and summary statistics.

summary statistics are displayed in the GUI and include:
- Total number of parcels
- Number of source files used
- Summary tables by jurisdiction, TAZ, and subarea  
- Basic validation checks on the assembled data

summary files are also exported as CSV files in the output folder.

"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime
from PyQt6.QtWidgets import (
    QDialog, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox,
    QTableWidget, QTableWidgetItem, QTabWidget
)

sys.path.append(os.getcwd())
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction
from utility import (IndentAdapter, dialog_level, SynPopAssumptions, Parcel_Data_Format, Data_Scale_Method,
                     Summary_Categories, ThreadWrapper)

from GUI_support_utilities import (Shared_GUI_Widgets, NumericTableWidgetItem)
# -------------------------
# Configuration section
# -------------------------

REQUIRED_COLUMNS = ["PARCELID"]

FILTER_RULES = {
    "Bellevue": lambda df: df[df["Jurisdiction"] == "BELLEVUE"],
    "Bellevue Fringe": lambda df: df[df["Jurisdiction"] == "BellevueFringe"],
    "Kirkland": lambda df: df[df["Jurisdiction"] == "KIRKLAND"],
    "Kirkland Fringe": lambda df: df[df["Jurisdiction"] == "KirklandFringe"],
    "Redmond": lambda df: df[df["Jurisdiction"] == "REDMOND"],
    "Redmond Fringe": lambda df: df[df["Jurisdiction"] == "RedmondFringe"],
    "Outside BKR": lambda df: df[(df["Jurisdiction"] == "Rest of KC") | (df["Jurisdiction"] == "External")]
}

class ParcelProcessor(QDialog, Shared_GUI_Widgets):
    def __init__(self, project_settings, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)
        self.project_settings = project_settings

        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)
        self.logger.info("Popsim Data UI initialized.")

        self.output_dir = self.project_settings['output_dir']
        self.horizon_year = self.project_settings['horizon_year']
        self.scenario_name = self.project_settings['scenario_name']

        self.file_inputs = {
            "Bellevue": "",
            "Bellevue Fringe": "",
            "Kirkland": "",            
            "Kirkland Fringe": "",
            "Redmond": "",
            "Redmond Fringe": "",
            "Outside BKR": ""

            # "Bellevue": r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\parcels_urbansim.txt",
            # "Bellevue Fringe": r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\parcels_urbansim.txt",
            # "Kirkland": r"Z:\Modeling Group\BKRCast\KirklandSupport\Kirkland2044Complan\preferred_2044\parcels_urbansim.txt",
            # "Kirkland Fringe": r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\parcels_urbansim.txt",
            # "Redmond": r"Z:\Modeling Group\BKRCast\LandUse\2044_long_term_planning\parcels_urbansim.txt",
            # "Redmond Fringe": r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\parcels_urbansim.txt",
            # "Outside BKR": r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\parcels_urbansim.txt"
        }
        self._init_ui()
        self.create_status_bar(self, 4)

    def _init_ui(self):
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        title = QLabel("Parcel Data Processor")
        title.setStyleSheet("font-size: 18px; font-weight: bold; height: 40px; padding: 5px;")
        title.setFixedHeight(40)
        self.main_layout.addWidget(title)

        # parcel file inputs
        for name in FILTER_RULES.keys():
            row = QHBoxLayout()
            label = QLabel(name)
            label.setMinimumWidth(160)
            entry = QLineEdit()
            browse = QPushButton("Browse")
            browse.clicked.connect(lambda _, n=name, e=entry: self.browse_file(n, e))

            row.addWidget(label)
            row.addWidget(entry)
            row.addWidget(browse)

            self.main_layout.addLayout(row)
            # Set pre-populated value if it exists, then store the widget
            if self.file_inputs[name] is not None:
                entry.setText(self.file_inputs[name])
            self.file_inputs[name] = entry


        # assemble button
        assemble_btn = QPushButton("Process Parcel File")
        assemble_btn.clicked.connect(self.assemble)
        self.assemble_btn = assemble_btn
        self.main_layout.addWidget(assemble_btn)

        # Summary table for the assembled data
        self.tabs = QTabWidget()
        self.summary_table = QTableWidget()
        self.summary_table.setSortingEnabled(True)
        self.summary_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.summary_table.customContextMenuRequested.connect(
            lambda pos: self.create_context_menu(self.summary_table, pos)
        )
        self.summary_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.summary_table: self.on_table_selection_changed(t)
        )
        self.summary_table.setColumnCount(2)
        self.summary_table.setHorizontalHeaderLabels([
            "Jurisdiction", "Parcel Count"
        ])
        self.tabs.addTab(self.summary_table, "Summary")

        # Raw data sample table
        self.raw_table = QTableWidget()
        self.raw_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.raw_table: self.on_table_selection_changed(t)
        )
        self.tabs.addTab(self.raw_table, "Raw Data Samples")
    
        # Validation table
        self.valid_table = QTableWidget()
        self.valid_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.valid_table.customContextMenuRequested.connect(
            lambda pos: self.create_context_menu(self.valid_table, pos)
        )
        self.valid_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.valid_table: self.on_table_selection_changed(t)
        )
        self.tabs.addTab(self.valid_table, "Validation")

        # Summary tables by jurisdiction
        self.jurisdiction_table = QTableWidget()
        self.jurisdiction_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.jurisdiction_table.customContextMenuRequested.connect(
            lambda pos: self.create_context_menu(self.jurisdiction_table, pos)
        )
        self.jurisdiction_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.jurisdiction_table: self.on_table_selection_changed(t)
        )
        self.tabs.addTab(self.jurisdiction_table, "Jurisdiction")

        # Summary tables by TAZ
        self.taz_table = QTableWidget()
        self.taz_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.taz_table.customContextMenuRequested.connect(
            lambda pos: self.create_context_menu(self.taz_table, pos)
        )
        self.taz_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.taz_table: self.on_table_selection_changed(t)
        )
        self.tabs.addTab(self.taz_table, "TAZ")

        # Summary tables by Subarea
        self.subarea_table = QTableWidget()
        self.subarea_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu) 
        self.subarea_table.customContextMenuRequested.connect(
            lambda pos: self.create_context_menu(self.subarea_table, pos)
        )
        self.subarea_table.selectionModel().selectionChanged.connect(
            lambda sel, des, t=self.subarea_table: self.on_table_selection_changed(t)
        )
        self.tabs.addTab(self.subarea_table, "Subarea")

        self.main_layout.addWidget(QLabel("Aggregated Summary"))
        self.main_layout.addWidget(self.tabs)


    def browse_file(self, name, entry):
        path, _ = QFileDialog.getOpenFileName(
            self, f"Select {name} parcel file", "",
            "Data Files (*.csv *.txt *.*)"
        )
        if path:
            entry.setText(path)

    def browse_output_file(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Output Folder", os.getcwd(),
        )
        if path:
            self.output_input.setText(path)

    def assemble(self):
        """Start assembly in a background thread."""
        self.disableAllButtons()
        self.status_sections[0].setText("Running")
        
        self.worker = ThreadWrapper(self._assemble_worker)
        self.worker.finished.connect(self._on_assembly_finished)
        self.worker.error.connect(self._on_assembly_error)
        self.worker.start()

    def _assemble_worker(self):
        """Main assembly logic - runs in background thread."""
        self.logger.info("Parcel data process started")
        
        # Log all file inputs
        self.logger.info("=" * 60)
        self.logger.info("ALL FILE INPUTS:")
        for name, entry in self.file_inputs.items():
            path = entry.text().strip()
            self.logger.info(f"  {name}: {path if path else '(not selected)'}")
         
        dfs = []
        summary_rows = []

        subarea_df = self.project_settings['subarea_df']
        # subarea_df = pd.read_csv(r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv")
        
        for name, entry in self.file_inputs.items():
            path = entry.text().strip()
            if not path:
                continue

            df = pd.read_csv(path, low_memory=False, sep = " ")
            df = df.merge(subarea_df[["BKRCastTAZ", "Jurisdiction", "Subarea", "SubareaName"]], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
            filtered = FILTER_RULES[name](df)
            # filtered["SOURCE"] = name
            self.logger.info(f"{name}: {len(filtered)} records selected")
            dfs.append(filtered)
            summary_rows.append((name, os.path.basename(path), len(filtered)))

        if not dfs:
            raise Exception("No input files selected")

        result = pd.concat(dfs, ignore_index=True)

        self.summarize_parcel_data(result, subarea_df=subarea_df, output_dir=self.output_dir.strip())

        result = result.drop(columns=["BKRCastTAZ", "Jurisdiction", "Subarea", "SubareaName"], errors='ignore')
        result = result.sort_values(by="PARCELID", ascending=True)
        output_filename = os.path.join(self.output_dir, f"{self.horizon_year}_{self.scenario_name}_assembled_parcel_urbansim.txt")

        if output_filename:
            result.to_csv(output_filename, index=False, sep =" ")
            self.logger.info(f"Assembled file saved to: {output_filename}")
            
            # Update UI from worker thread safely
            self.status_sections[0].setText("Done")
            self.status_sections[1].setText(f"Parcels: {len(result)} Cols: {len(result.columns)}")
            self.status_sections[2].setText(f"Sources: {len(summary_rows)}")
            self.status_sections[3].setText(f"Output: {os.path.basename(output_filename)}")
            
            # Update summary table
            self.summary_table.setRowCount(len(summary_rows))
            for row, (jur, src, cnt) in enumerate(summary_rows):
                self.summary_table.setItem(row, 0, QTableWidgetItem(jur))
                self.summary_table.setItem(row, 1, QTableWidgetItem(str(cnt)))

        self.populate_raw_table(result)
        self.validation_checks(result)

    def summarize_parcel_data(self, df, subarea_df=None, output_dir=None):
        
        cols = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']
        summary_jurisdictions = df.groupby('Jurisdiction')[cols].sum().reset_index()
        summary_taz = df.groupby('TAZ_P')[cols].sum().reset_index()
        summary_subarea = df.groupby('Subarea')[cols].sum().reset_index()
        summary_subarea = summary_subarea.merge(subarea_df[['Subarea', 'SubareaName']].drop_duplicates(), on='Subarea', how='left')

        if output_dir is None:
            output_dir = os.getcwd()
        summary_jurisdictions.to_csv(os.path.join(output_dir, 'parcel_summary_by_jurisdiction.csv'), index=False)
        summary_taz.to_csv(os.path.join(output_dir, 'parcel_summary_by_taz.csv'), index=False)
        summary_subarea.to_csv(os.path.join(output_dir, 'parcel_summary_by_subarea.csv'), index=False)

        self.add_dataframe_to_table(summary_jurisdictions, self.jurisdiction_table)
        self.add_dataframe_to_table(summary_taz, self.taz_table)
        self.add_dataframe_to_table(summary_subarea, self.subarea_table)

        
    def add_dataframe_to_table(self, df, table):
        table.setRowCount(len(df))
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels(df.columns.tolist())

        for row in range(len(df)):
            for col in range(len(df.columns)):
                val = df.iat[row, col]
                item = NumericTableWidgetItem(str(val))
                table.setItem(row, col, item)
        table.setSortingEnabled(True)

    def validation_checks(self, df):
        self.valid_table.setSortingEnabled(False)
        header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicates", "Min", "Max", "Mean"]
        self.valid_table.clear()
        self.valid_table.setRowCount(len(df.columns))
        self.valid_table.setColumnCount(len(header))
        self.valid_table.setHorizontalHeaderLabels(header)

        for row, col in enumerate(df.columns):
            series = df[col]
            dtype = str(series.dtype)
            unique_vals = series.nunique()
            missing_vals = series.isna().sum()
            duplicate_vals = len(series) - unique_vals - missing_vals  # Total rows - unique values - missing values
            min_val = series.min() if pd.api.types.is_numeric_dtype(series) else ""
            max_val = series.max() if pd.api.types.is_numeric_dtype(series) else ""
            mean_val = series.mean() if pd.api.types.is_numeric_dtype(series) else ""

            self.valid_table.setItem(row, 0, QTableWidgetItem(col))
            self.valid_table.setItem(row, 1, QTableWidgetItem(dtype))
            self.valid_table.setItem(row, 2, NumericTableWidgetItem(str(unique_vals)))
            self.valid_table.setItem(row, 3, NumericTableWidgetItem(str(missing_vals)))
            self.valid_table.setItem(row, 4, NumericTableWidgetItem(str(duplicate_vals)))
            self.valid_table.setItem(row, 5, NumericTableWidgetItem(str(min_val)))
            self.valid_table.setItem(row, 6, NumericTableWidgetItem(str(max_val)))
            self.valid_table.setItem(row, 7, NumericTableWidgetItem(str(mean_val)))

        self.valid_table.setSortingEnabled(True)

    def populate_raw_table(self, df=None):
        """Populate the first 100 rows of the raw data table with the given DataFrame."""
        if df is None:
            return
        
        self.raw_table.clear()
        self.raw_table.setRowCount(min(100, df.shape[0]))
        self.raw_table.setColumnCount(len(df.columns))
        self.raw_table.setHorizontalHeaderLabels(df.columns.tolist())

        for row in range(min(100, df.shape[0])):
            for col in range(len(df.columns)):
                val = df.iat[row, col]
                item = NumericTableWidgetItem(str(val))
                self.raw_table.setItem(row, col, item)

    def _on_assembly_finished(self):
        """Called when assembly thread finishes."""
        self.enableAllButtons()

    def _on_assembly_error(self, error_obj):
        """Called when assembly thread encounters an error."""
        self.logger.error(f"Assemble process failed: {error_obj}", exc_info=True)
        self.status_sections[0].setText("Error")
        self.status_sections[1].setText(str(error_obj))
        self.enableAllButtons()
        QMessageBox.critical(self, "Error", str(error_obj))
