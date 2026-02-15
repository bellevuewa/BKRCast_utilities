import sys, os
sys.path.append(os.getcwd())
import logging
import traceback

import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy, QSplitter,
    QTableWidget, QTableWidgetItem, QMainWindow, QTabWidget, QListWidget, QDialog, QHeaderView
)
from PyQt6.QtCore import Qt
from enum import Enum
from GUI_support_utilities import (Shared_GUI_Widgets, NumericTableWidgetItem, BaseDataGenerator, ValidationAndSummary)

from LandUseUtilities.Parcels import Parcels
from ParcelDataOperations import ParcelDataOperations
from utility import (IndentAdapter, dialog_level, Parcel_Data_Format, Data_Scale_Method, ThreadWrapper)

_LOGGING_CONFIGURED = False
class ParcelDataUserInterface(QDialog, Shared_GUI_Widgets):
    """Main window for the Parcel Data Processor application."""
    def __init__(self, project_settings, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parcel Data Processor")
        self.setMinimumWidth(750)

        self.project_settings = project_settings
        self.base_file = r"Z:\Modeling Group\BKRCast\LandUse\Complan\Complan2044\2044LU\DT_rebalance_btw_job_category\parcels_urbansim.txt"
        self.landuse_rules = []
        self.base_parcel : Parcels = None
        self.final_parcel  : Parcels = None

        self.output_dir = self.project_settings['output_dir']
        self.horizon_year = self.project_settings['horizon_year']
        self.scenario_name = self.project_settings['scenario_name']

        self._init_ui() 
        # create_status bar from Shared_UI_Widgets
        self.create_status_bar(self, 4)

        self.process_rules = [
            {"Jurisdiction": "Bellevue", 
             "File": r"Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning\2044_long_range_planning_bellevue_jobs.csv",
             "Data_Format": "Processed_Parcel_Data",
             "Scale_Method": "Keep_the_Data_from_the_Partner_City"},
            {"Jurisdiction": "Kirkland", 
             "File": r"Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning\input for test\parcel_fixed_Kirkland_Complan_2044_target_Landuse_by_BKRCastTAZ.csv",
             "Data_Format": "BKRCastTAZ_Format",
             "Scale_Method": "Scale_by_Total_Jobs_by_TAZ"},
            {"Jurisdiction": "Redmond", 
             "File": r"Z:\Modeling Group\BKRCast\LandUse\2044_long_term_planning\2044_Redmond_estimated_jobs_by_BKRTMTAZ.csv",
             "Data_Format": "BKR_Trip_Model_TAZ_Format",
             "Scale_Method": "Scale_by_Total_Jobs_by_TAZ"}
            ]
        
        self.preload_rules()
        base_logger = logging.getLogger(__name__)
        self.indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, self.indent)
        self.logger.info("Parcel Data Processor initialized.")

    def _init_ui(self):
        """Initialize the user interface."""
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Horizon Year"))
        year_box = QLabel(str(self.horizon_year))
        hbox.addWidget(year_box)
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Output Directory"))
        hbox.addWidget(QLabel(self.output_dir))
        self.main_layout.addLayout(hbox) 
        
        hbox = QHBoxLayout()    
        base_button = QPushButton("Select Base Parcel Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected") 
        # base_button.clicked.connect(lambda: self.select_files("Select Base Parcel File", self.base_file_label))  
        base_button.clicked.connect(self.select_base_parcel_file)
        hbox.addWidget(self.base_file_label)  
        self.main_layout.addLayout(hbox)

        #### create controls for input data
        groupbox_container = QWidget()
        groupbox_layout = QVBoxLayout(groupbox_container)
        groupbox_layout.setContentsMargins(0, 0, 0, 0)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        partner_container, self.jurisdiction_list_box = self.make_list_panel("Partner Cities", ["Bellevue", "Kirkland", "Redmond"])
        splitter.addWidget(partner_container)

        # Data Format
        format_container, self.method_list_box = self.make_list_panel(
            "Data Format",
            [item.name for item in Parcel_Data_Format]
        )
        self.method_list_box.itemClicked.connect(self.on_format_clicked)
        splitter.addWidget(format_container)

        # Scale By
        scaleby_container, self.scaleby_list_box = self.make_list_panel(
            "Scale By",
            [item.name for item in Data_Scale_Method],
            v_policy=QSizePolicy.Policy.Minimum
        )
        splitter.addWidget(scaleby_container)

        # Initial splitter sizes
        splitter.setSizes([200, 300, 250])

        groupbox_layout.addWidget(splitter)
        self.main_layout.addWidget(groupbox_container)

        add_rules_button = QPushButton("Add Rules")
        add_rules_button.clicked.connect(self.add_rules)
        self.main_layout.addWidget(add_rules_button)

        vbox = QVBoxLayout()
        vbox.addWidget(QLabel("Processing Rules"))
        self.rule_table = QTableWidget()
        self.rule_table.setColumnCount(4)
        self.rule_table.horizontalHeader().setStretchLastSection(True)
        self.rule_table.setHorizontalHeaderLabels(["Jurisdiction", "File", "Data Format", "Scale Method"])
        self.rule_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.rule_table.customContextMenuRequested.connect(lambda pos: self.create_context_menu(self.rule_table, pos))
        vbox.addWidget(self.rule_table)

        self.main_layout.addLayout(vbox)
        
        self.process_btn = QPushButton("Start Processing")
        self.process_btn.clicked.connect(self.process_btn_clicked)
        self.main_layout.addWidget(self.process_btn)

        self.sync_btn = QPushButton("Sync Synthetic Population")
        self.sync_btn.clicked.connect(self.sync_btn_clicked)
        self.main_layout.addWidget(self.sync_btn)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_files)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_parcel_data) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)

    def on_format_clicked(self, item):
        if item is None:
            return
        
        if item.text() == Parcel_Data_Format.Processed_Parcel_Data.value:
            self.scaleby_list_box.clear()
            self.scaleby_list_box.addItem(Data_Scale_Method.Keep_the_Data_from_the_Partner_City.value)
        elif item.text() == Parcel_Data_Format.BKRCastTAZ_Format.value:
            self.scaleby_list_box.clear()
            self.scaleby_list_box.addItem(Data_Scale_Method.Scale_by_Job_Category.value)           
            self.scaleby_list_box.addItem(Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value)  
        elif item.text() ==   Parcel_Data_Format.BKR_Trip_Model_TAZ_Format.value:
            self.scaleby_list_box.clear()
            self.scaleby_list_box.addItem(Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value)              

    def sync_btn_clicked(self):
        if self.final_parcel == None:
            # load file and create Parcel Object
            parcel_name, _ = QFileDialog.getOpenFileName(self, "Select the Parcel File for Syncing", "", "txt Files (*.txt);;All Files (*)")
            if parcel_name == '':
                QMessageBox.critical(self, "Error", "Please select the parcel file.")
                return
            
            self.final_parcel = Parcels(self.project_settings['subarea_file'], self.project_settings['lookup_file'], parcel_name, self.horizon_year, self.indent + 1)
        # otherwise use final_parcel
        popsim_name, _ = QFileDialog.getOpenFileName(self, "Select Synthetic Population File", "", "H5 Files (*.h5);;All Files (*)")
        if popsim_name == '':
            QMessageBox.critical(self, "Error", "Please select the synthetic population file.")
            return          
        
        self.status_sections[0].setText("synchronizing")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.final_parcel.sync_with_synthetic_population, popsim_name)
        
        self.worker.finished.connect(lambda: self._on_sync_thread_finished()) # lambda is important
        self.worker.error.connect(lambda message: self._on_valid_thread_error(self.status_sections[0], message))
        self.worker.start()

    def preload_rules(self):
        # self.rule_table.clear()
        for rule in self.process_rules:
            self.rule_table.insertRow(self.rule_table.rowCount())
            row = self.rule_table.rowCount() - 1
            for col, key in enumerate(rule.keys()):
                item = QTableWidgetItem(str(rule[key]))
                if key == "File":
                    item.setToolTip(str(rule[key]))
                self.rule_table.setItem(row, col, item)

    def make_list_panel(self, title, items, v_policy=QSizePolicy.Policy.Expanding):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        label = QLabel(title)
        listbox = QListWidget()
        listbox.addItems(items)
        listbox.setSizePolicy(QSizePolicy.Policy.Preferred, v_policy)
        listbox.setStyleSheet("""
            QListWidget::item:selected {
                background: palette(highlight);
                color: palette(highlighted-text);
            }
            """)

        layout.addWidget(label)
        layout.addWidget(listbox)

        return container, listbox

    def select_base_parcel_file(self):
        base_dialog = BaseDataGenerator(self, "Base Parcel File Processor")
        if base_dialog.exec() == QDialog.DialogCode.Accepted:
            self.base_parcel = base_dialog.base_parcel
            self.base_file_label.setText(base_dialog.base_file)

    def select_files(self, msg, label):
        filename, _ = QFileDialog.getOpenFileName(self, msg, "", "Text Files (*.txt);;All Files (*)")
        if not filename:
            return
        else:
            label.setText(filename)
            return filename

    def validate_files(self):
        self.status_sections[0].setText("running")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.final_parcel.validate_parcel_file)
        self.worker.finished.connect(lambda validate_dict: self._on_valid_thread_finished([self.valid_btn, self.summarize_btn], validate_dict))
        self.worker.error.connect(lambda message: self._on_valid_thread_error([self.valid_btn, self.summarize_btn], self.status_sections[0], message))
        self.worker.start()
        
    def _on_sync_thread_finished(self, btns=None):
        self.enableAllButtons(btns)
        
        self.status_sections[0].setText("Done")   

    def _on_valid_thread_finished(self, btns, validate_dict):
        # called when the thread is finished
        self.enableAllButtons()
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the processed parcel data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def _on_valid_thread_error(self, status_bar_section, message):
        # called when the thread encounters an error
        self.enableAllButtons()
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)

    def summarize_parcel_data(self):
        summary_dict = self.final_parcel.summarize_parcel_data(self.output_dir, 'final')
        summary_dialog = ValidationAndSummary(self, "Processed Parcel File Summary", summary_dict)
        summary_dialog.exec()         
       
    def add_rules(self):
        if (not self.jurisdiction_list_box.selectedItems()) or (not self.method_list_box.selectedItems()) or (not self.scaleby_list_box.selectedItems()):
            QMessageBox.information(self, "Warning", "You cannot leave these boxes blank")
            return

        city = self.jurisdiction_list_box.currentItem().text()
        method = self.method_list_box.currentItem().text()
        scale_method = self.scaleby_list_box.currentItem().text()

        input_filename, _ = QFileDialog.getOpenFileName(self, f"Select input file from {city}", "", "Text Files (*.txt);;All Files (*)")
        rule_dict = {
            "Jurisdiction": city,
            "File": input_filename,
            "Data Format": method,
            "Scale Method": scale_method    
        }
        self.landuse_rules.append(rule_dict)
        # add to the rule table
        self.rule_table.insertRow(self.rule_table.rowCount())
        row = self.rule_table.rowCount() - 1

        for col, key in enumerate(rule_dict.keys()):
            item = QTableWidgetItem(str(rule_dict[key]))
            if key == "File": # File
                item.setToolTip(str(rule_dict[key]))
            self.rule_table.setItem(row, col, item)

    def process_btn_clicked(self):
        rows = self.rule_table.rowCount()
        if rows == 0:
            QMessageBox.critical(self, "Error", "At least one rule is required.")
            return
        
        self.status_sections[0].setText("Processing")
        self.process_rules = self.table_to_list_of_dicts(self.rule_table)

        self.disableAllButtons()
        self.worker = ThreadWrapper(self.parcel_process)
        self.worker.finished.connect(lambda ret: self._on_process_thread_finished(ret))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
        self.worker.start()

    def parcel_process(self) -> dict:
        import debugpy
        debugpy.breakpoint()

        fn = f'{self.horizon_year}_{self.scenario_name}_updated_urbansim_parcels.txt'
        indent = dialog_level(self)
        op = ParcelDataOperations(self.base_parcel, self.output_dir, fn, indent + 1)
        for rule in self.process_rules:
            ret = op.generate_employment_data_for_jurisiction(rule)

        # self.final_parcel = Parcels.from_dataframe(ret['data_frame'], self.horizon_year, os.path.join(self.output_dir, fn), self.project_settings['subarea_df'], self.project_settings['lookup_df'], indent + 1)
        self.final_parcel = op.export_updated_parcels()
        return ret

    def _on_process_thread_finished(self, ret, btns = None):
        # called when the thread is finished
        self.enableAllButtons(btns)
        
        self.status_sections[0].setText("Done")

    def _on_process_thread_error(self, status_bar_section, e):
        # called when the thread encounters an error
        self.enableAllButtons()
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", str(e))

    def closeEvent(self, event):
        """Handle the close event to ensure proper cleanup."""
        self.logger.info("Parcel Data Processor is closed.")
        event.accept()

   
