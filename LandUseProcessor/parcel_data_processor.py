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
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox,
    QTableWidget, QTableWidgetItem, QMainWindow, QMenu, QTabWidget
)

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction

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

# -------------------------
# Logging setup
# -------------------------

LOG_FILE = "parcel_processor.log"


class ProcessorWorker(QThread):
    """Worker thread to run the assemble operation without freezing the GUI."""
    finished = pyqtSignal()
    error = pyqtSignal(str)
    status_update = pyqtSignal(str, str, str, str)  # status, records, sources, output
    
    def __init__(self, assembler):
        super().__init__()
        self.assembler = assembler
    
    def run(self):
        try:
            self.assembler._assemble_worker()
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.finished.emit()

class NumbericTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem that treats numbers correctly for sorting."""
    def __init__(self, text):
        text = "" if text is None else str(text)
        super().__init__(text)
        try:
            self.numeric_value = float(text)
            self.is_numeric = True
        except ValueError:
            self.numeric_value = text
            self.is_numeric = False

    def __lt__(self, other):
        if isinstance(other, NumbericTableWidgetItem):
            if self.is_numeric and other.is_numeric:
                return self.numeric_value < other.numeric_value
            
            if self.is_numeric != other.is_numeric:
                return self.is_numeric
        return super().__lt__(other)
        
class ParcelProcessor(QMainWindow, Shared_GUI_Widgets):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)
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

        # subarea file input
        label = QLabel("Subarea Definition File")
        label.setMinimumWidth(160)
        sub_row = QHBoxLayout()

        self.subarea_input = QLineEdit()
        self.subarea_input.setPlaceholderText("Select CSV/Excel containing Subarea mapping...")
        sub_browse = QPushButton("Browse")
        sub_browse.clicked.connect(lambda: self.browse_file("Subarea File", self.subarea_input))
        sub_row.addWidget(label)
        sub_row.addWidget(self.subarea_input)
        sub_row.addWidget(sub_browse)
        self.main_layout.addLayout(sub_row)

        # output folder input
        label = QLabel("Output File")
        label.setMinimumWidth(160)
        output_row = QHBoxLayout()
        self.output_input = QLineEdit()
        self.output_input.setPlaceholderText("Select location to save assembled parcel file...")
        output_browse = QPushButton("Browse")
        output_browse.clicked.connect(self.browse_output_file)
        output_row.addWidget(label)
        output_row.addWidget(self.output_input)
        output_row.addWidget(output_browse)
        self.main_layout.addLayout(output_row)

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

        central = QWidget()
        central.setLayout(self.main_layout)
        self.setCentralWidget(central)

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
        self.assemble_btn.setEnabled(False)
        self.status_sections[0].setText("Running")
        
        self.worker = ProcessorWorker(self)
        self.worker.finished.connect(self._on_assembly_finished)
        self.worker.error.connect(self._on_assembly_error)
        self.worker.start()

    def _assemble_worker(self):
        """Main assembly logic - runs in background thread."""
        logging.basicConfig(
            filename=os.path.join(self.output_input.text().strip(), "parcel_processor.log"),
            level=logging.INFO,
            format="%(asctime)s | %(message)s",
        )

        logging.info("Parcel data process started")
        
        # Log all file inputs
        logging.info("=" * 60)
        logging.info("ALL FILE INPUTS:")
        for name, entry in self.file_inputs.items():
            path = entry.text().strip()
            logging.info(f"  {name}: {path if path else '(not selected)'}")
        logging.info(f"  Subarea File: {self.subarea_input.text().strip() if self.subarea_input.text().strip() else '(not selected)'}")
        logging.info("=" * 60)
        
        dfs = []
        summary_rows = []

        subarea_df = pd.read_csv(self.subarea_input.text())
        # subarea_df = pd.read_csv(r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv")
        
        for name, entry in self.file_inputs.items():
            path = entry.text().strip()
            if not path:
                continue

            df = pd.read_csv(path, low_memory=False, sep = " ")
            df = df.merge(subarea_df[["BKRCastTAZ", "Jurisdiction", "Subarea", "SubareaName"]], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
            filtered = FILTER_RULES[name](df)
            # filtered["SOURCE"] = name
            logging.info(f"{name}: {len(filtered)} records selected")
            dfs.append(filtered)
            summary_rows.append((name, os.path.basename(path), len(filtered)))

        if not dfs:
            raise Exception("No input files selected")

        result = pd.concat(dfs, ignore_index=True)

        self.summarize_parcel_data(result, subarea_df=subarea_df, output_dir=self.output_input.text().strip())

        result = result.drop(columns=["BKRCastTAZ", "Jurisdiction", "Subarea", "SubareaName"], errors='ignore')
        result = result.sort_values(by="PARCELID", ascending=True)
        output_filename = os.path.join(self.output_input.text().strip(), "assembled_parcel_urbansim.txt")
        # output_filename = r"Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning\parcel_urbansim.txt"
        if output_filename:
            result.to_csv(output_filename, index=False, sep =" ")
            logging.info(f"Assembled file saved to: {output_filename}")
            
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
                item = NumbericTableWidgetItem(str(val))
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
            self.valid_table.setItem(row, 2, NumbericTableWidgetItem(str(unique_vals)))
            self.valid_table.setItem(row, 3, NumbericTableWidgetItem(str(missing_vals)))
            self.valid_table.setItem(row, 4, NumbericTableWidgetItem(str(duplicate_vals)))
            self.valid_table.setItem(row, 5, NumbericTableWidgetItem(str(min_val)))
            self.valid_table.setItem(row, 6, NumbericTableWidgetItem(str(max_val)))
            self.valid_table.setItem(row, 7, NumbericTableWidgetItem(str(mean_val)))

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
                item = NumbericTableWidgetItem(str(val))
                self.raw_table.setItem(row, col, item)

    def _on_assembly_finished(self):
        """Called when assembly thread finishes."""
        self.assemble_btn.setEnabled(True)

    def _on_assembly_error(self, error_msg):
        """Called when assembly thread encounters an error."""
        logging.error(f"Assemble process failed: {error_msg}", exc_info=True)
        self.status_sections[0].setText("Error")
        self.status_sections[1].setText(error_msg)
        self.assemble_btn.setEnabled(True)
        QMessageBox.critical(self, "Error", error_msg)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ParcelProcessor()
    window.show()
    sys.exit(app.exec())
