import sys, os
sys.path.append(os.getcwd())
import logging

from PyQt6.QtWidgets import (
     QWidget, QLabel, QPushButton, QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, 
     QSizePolicy, QSplitter, QTableWidget,
    QTableWidget, QTableWidgetItem,    QDialog, QHeaderView
)
from PyQt6.QtGui import QDoubleValidator
from PyQt6.QtCore import Qt
from enum import Enum
from GUI_support_utilities import (Shared_GUI_Widgets, NumericTableWidgetItem, ThreadWrapper, ValidationAndSummary)

from LandUseUtilities.synpop_interpolation import LinearSynPopInterpolator
from LandUseUtilities.Parcels import Parcels
from ParcelDataOperations import ParcelDataOperations
from utility import (IndentAdapter, dialog_level, SynPopAssumptions, Parcel_Data_Format, Data_Scale_Method,
                     Summary_Categories)
from LandUseUtilities.synthetic_population import SyntheticPopulation
from SynPopDataOperations import SynPopDataOperations


class SynPopDataUserInterface(QDialog, Shared_GUI_Widgets):
    def __init__(self, project_setting, parent = None):
        super().__init__(parent)
        self.project_settings = project_setting
        self.output_dir = self.project_settings['output_dir']
        self.horizon_year = self.project_settings['horizon_year']
        self.scenario_name = self.project_settings['scenario_name']

        self.base_synpop : SyntheticPopulation = None
        self.final_synpop  : SyntheticPopulation = None

        self.__init_ui__()
        self.create_status_bar(self, 4)
        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)
        self.logger.info("Popsim Data UI initialized.")

        self.popsim_control_template_file = ''
        
        self.process_rules = [
            {"Jurisdiction": "Bellevue", 
                "File": r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\test_2044_long_range_planning\2044_long_range_planning_cob_housingunits.csv",
                "Data Format": "Processed_Parcel_Data",
                "Scale Method": "Keep_the_Data_from_the_Partner_City"},
            {"Jurisdiction": "Kirkland", 
                "File": r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\test_2044_long_range_planning\parcel_fixed_Kirkland_Complan_2044_target_Landuse_by_BKRCastTAZ.csv",
                "Data Format": "BKRCastTAZ_Format",
                "Scale Method": "Scale_by_Total_Hhs_by_TAZ"},
            {"Jurisdiction": "Redmond", 
                "File": r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\test_2044_long_range_planning\2044_Redmond_DU.csv",
                "Data Format": "BKR_Trip_Model_TAZ_Format",
                "Scale Method": "Scale_by_Total_Hhs_by_TAZ"}
            ]
        self.preload_rules()

    def __init_ui__(self):
        self.setWindowTitle("Synthetic Population Data Processor")
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
        base_button = QPushButton("Select Base Population Data Files")
        base_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        hbox.addWidget(base_button)
        self.base_file_label = QLabel("No files selected") 
        base_button.clicked.connect(self.select_base_popsim_file)
        hbox.addWidget(self.base_file_label)  
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        popsim_button = QPushButton("Select PopSim control file template")
        popsim_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.popsim_label = QLabel("No template selected")
        popsim_button.clicked.connect(self.popsim_template_btn_clicked)
        hbox.addWidget(popsim_button)
        hbox.addWidget(self.popsim_label)
        self.main_layout.addLayout(hbox)    

        vbox = QVBoxLayout()
        label = QLabel('Household Size and Occupancy Rate')
        vbox.addWidget(label)
        cities = ['Bellevue', 'Kirkland', 'Redmond']
        fields = ['sfhhsize', 'mfhhsize', 'sfhh_occ', 'mfhh_occ']
        self.table = QTableWidget(len(fields), len(cities))
        self.table.setHorizontalHeaderLabels(cities)
        self.table.setVerticalHeaderLabels(fields)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        # load default values hhsize and occ rate
        for row, field in enumerate(fields):
            for col, city in enumerate(cities):
                box = QLineEdit()
                box.setValidator(QDoubleValidator(0.0,5.0, 3))
                self.table.setCellWidget(row, col, box)
                self.table.cellWidget(row, col).setText(str(SynPopAssumptions[city][field]))
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        vbox.addWidget(self.table)
        self.main_layout.addLayout(vbox)

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

    def read_synpop_hhsize_occ(self)->dict:
        import copy
        ret_dict = copy.deepcopy(SynPopAssumptions)
        cities = ['Bellevue', 'Kirkland', 'Redmond']
        fields = ['sfhhsize', 'mfhhsize', 'sfhh_occ', 'mfhh_occ']      

        for col, city in enumerate(cities):
            for row, field in enumerate(fields):
                box = self.table.cellWidget(row, col)
                ret_dict[city][field] = float(box.text())

        return ret_dict

    def select_base_popsim_file(self):
        base_dialog = BaseSynPopDataGenerator(self, "Base PopSim Data Processor")
        if base_dialog.exec() == QDialog.DialogCode.Accepted:
            self.base_synpop = base_dialog.base_synpop
            self.base_file_label.setText(base_dialog.base_file)
        

    def popsim_template_btn_clicked(self):
        path, _ = QFileDialog.getOpenFileName(
                    self, 'Select PopSim control template file', "",
                    "csv (*.csv);;All Files(*.*)"
                )
        
        if path:
            self.popsim_label.setText(path)
            self.popsim_control_template_file = path
            self.status_sections[0].setText("template file selected.")

    def process_btn_clicked(self):
        rows = self.rule_table.rowCount()
        if rows == 0:
            QMessageBox.critical(self, "Error", "At least one rule is required.")
            return
        
        if (self.popsim_control_template_file == ''):
            QMessageBox.critical(self, "Error", "Select the template control file for popsim.")
            return
        
        self.status_sections[0].setText("Processing")
        self.process_rules = self.table_to_list_of_dicts(self.rule_table)

        self.disableAllButtons()
        self.worker = ThreadWrapper(self.synpop_process)
        self.worker.finished.connect(lambda ret: self._on_process_thread_finished(self.status_sections[0], ret))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
        self.worker.start()

    def synpop_process(self) -> dict:
        import debugpy
        debugpy.breakpoint()
        self.logger.info(f'creating the control file for PopulationSim')

        fn = f'ACS2016_{self.horizon_year}_{self.scenario_name}_popsim_control_file.csv'
        indent = dialog_level(self)
        hhs_assumptions = self.read_synpop_hhsize_occ()
        self.logger.info(f'assptions for household size and occupancy rates:')
        import json
        self.logger.info(f'{json.dumps(hhs_assumptions, indent = 3)}')
        op = SynPopDataOperations(self.base_synpop, self.scenario_name, self.output_dir, hhs_assumptions, indent + 1)

        for rule in self.process_rules:
            ret = op.generate_total_hhs_data_for_jurisdiction(rule)

        op.treatment_for_special_GEOID10()
        op.controlled_rounding()
        op.export_popsim_control_file(self.popsim_control_template_file, fn)

        guide_filename = f'ACS2016_{self.horizon_year}_{self.scenario_name}_final_hhs_by_parcel.csv'
        op.export_household_allocation_guide_file(guide_filename)
        return ret

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

        # add to the rule table
        self.rule_table.insertRow(self.rule_table.rowCount())
        row = self.rule_table.rowCount() - 1

        for col, key in enumerate(rule_dict.keys()):
            item = QTableWidgetItem(str(rule_dict[key]))
            if key == "File": # File
                item.setToolTip(str(rule_dict[key]))
            self.rule_table.setItem(row, col, item)
        
        self.status_sections[0].setText('new rule added.')

class BaseSynPopDataGenerator(QDialog, Shared_GUI_Widgets):
    def __init__(self, parent = None, message = None):
        super().__init__(parent)
        self.__init_ui__(message)
        self.create_status_bar(self, 4)
        
        self.base_file = ""
        self.lower_boundary_file = ""
        self.upper_boundary_file = ""
        self.blockgroup_file = ''
        self.base_synpop : SyntheticPopulation = None

        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)

        self.logger.info("Base Synthetic Population Data Generator initialized.")

    def __init_ui__(self, msg):
        """Initialize the user interface."""
        self.setWindowTitle("Base Parcel Process")
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        info_label = QLabel(msg)
        self.main_layout.addWidget(info_label)
        hbox = QHBoxLayout()
        self.op1_label = QLabel("Select a Parcel File as the Base")
        hbox.addWidget(self.op1_label)
        select_btn = QPushButton("Select a Base File")
        select_btn.clicked.connect(lambda: self.select_file("Select a Base Population File", self.op1_label))
        hbox.addWidget(select_btn)
        self.main_layout.addLayout(hbox)

        self.base_filename_label = QLabel("No File is Selected")
        self.main_layout.addWidget(self.base_filename_label)

        groupbox_layout =  QVBoxLayout()
        groupbox_layout.addWidget(QLabel("Interpolate from Two Population Files"))
        hbox = QHBoxLayout()
        self.sel1_btn = QPushButton("Select the Population File for the Lower Boundary")
        self.sel1_btn.clicked.connect(lambda: self.select_file_for_interpolation("lower"))
        self.sel2_btn = QPushButton("Select the Population File for the Upper Boundary")
        self.sel2_btn.clicked.connect(lambda: self.select_file_for_interpolation("upper"))
        hbox.addWidget(self.sel1_btn)
        hbox.addWidget(self.sel2_btn)      
        groupbox_layout.addLayout(hbox)

        ofm_btn = QPushButton("Select a census block group File")
        ofm_btn.clicked.connect(lambda: self.select_block_group_file("Select a census block group File", self.ofm_label))
        self.main_layout.addWidget(ofm_btn)
        self.ofm_label = QLabel("no census block group file selected")
        self.main_layout.addWidget(self.ofm_label)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Side", "Year", "File"])
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(lambda pos: self.create_context_menu(self.table, pos))
        groupbox_layout.addWidget(self.table)

        self.interpolate_btn = QPushButton("Interpolate")
        self.interpolate_btn.clicked.connect(self.interpolation_btn_clicked)
        groupbox_layout.addWidget(self.interpolate_btn)
        self.main_layout.addLayout(groupbox_layout)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_btn_clicked)
        self.valid_btn.setEnabled(False)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_btn_clicked) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)        

    def select_file_for_interpolation(self, side):
        path, _ = QFileDialog.getOpenFileName(
                    self, f"Please select a population file for {side} boundary", "",
                    "HDF5 Files (*.h5);;All Files(*.*)"
                )
        self.op1_label.setText('No file is selected')
        self.base_file = ''
        self.base_synpop = None
        rowCount = self.table.rowCount()
        self.table.insertRow(rowCount)
        self.table.setItem(rowCount, 0, QTableWidgetItem(side))
        year_item = QTableWidgetItem(0)
        self.table.setItem(rowCount, 1, year_item)
        self.table.setItem(rowCount, 2, QTableWidgetItem(path))

    def select_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "HDF5 Files (*.h5);;All Files(*.*)"
                )
        if (path !='') and (label is not None):
            label.setText(path)
            self.sel1_btn.setEnabled(False)
            self.sel2_btn.setEnabled(False)
            self.interpolate_btn.setEnabled(False)
            indent = dialog_level(self)
            self.status_sections[0].setText("Base Population selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
            self.base_file = path
            self.base_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], self.base_file, self.parent().project_settings['horizon_year'], indent + 1)
            self.logger.info(f"Selected base synthetic population file: {path}")
        return
    
    def select_block_group_file(self, message, label = None):
        path, _ = QFileDialog.getOpenFileName(
                    self, message, "",
                    "csv Files (*.csv);;All Files(*.*)"
                )
        if (path is not None) and (label is not None):
            label.setText(path)
            self.sel1_btn.setEnabled(True)
            self.sel2_btn.setEnabled(True)
            indent = dialog_level(self)
            self.status_sections[0].setText("Blockgroup file selected.")
            self.valid_btn.setEnabled(True)
            self.summarize_btn.setEnabled(True)
            self.blockgroup_file = path

            self.logger.info(f"Selected block group template file: {path}")
        return
    
    def interpolation_btn_clicked(self):
        self.status_sections[0].setText("interpolating")
        self.disableAllButtons()

        if (self.table.item(0,1).text().strip().isdigit() == False) or (self.table.item(1,1).text().strip().isdigit() == False):
            QMessageBox.critical(self, "Error", "Check horizon years for interpolation.")
            return
        
        base_year_dict = {}
        num_row = self.table.rowCount()

        if num_row > 2 or num_row < 2:
            QMessageBox.critical(self, "Error", "Too many files for interpolation.")
            return
        
        for row in range(num_row):
            base_year_dict[self.table.item(row, 0).text()] = {"year": int(self.table.item(row, 1).text()), "path": self.table.item(row, 2).text()}
        
        lower = int(base_year_dict['lower']['year'])
        lower_path = base_year_dict['lower']['path']
        upper = int(base_year_dict['upper']['year'])
        upper_path = base_year_dict['upper']['path']

        self.worker = ThreadWrapper(self.interpolate_two_pop_files, lower_path, upper_path, lower, upper, self.parent().horizon_year)
        self.worker.finished.connect(lambda interpolated_synpop: self._on_interpolation_finished(interpolated_synpop))
        self.worker.error.connect(lambda eobj: self._on_interpolation_error(eobj))
        self.worker.start()

    def interpolate_two_pop_files(self, lower_path, upper_path, lower_year, upper_year, horizon_year):
        import debugpy
        debugpy.breakpoint()
        indent = dialog_level(self)
        left_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], lower_path, lower_year, indent + 1)
        right_synpop = SyntheticPopulation(self.parent().project_settings['subarea_file'], self.parent().project_settings['lookup_file'], upper_path, upper_year, indent + 1)

        if self.blockgroup_file == '':
            QMessageBox.critical(self, "Error", 'Select the block group file first')
            return

        interpolation = LinearSynPopInterpolator(self.parent().output_dir, self.blockgroup_file, indent)

        self.logger.info(f"Interpolating synthetic population data between {lower_year} and {upper_year} for horizon year {horizon_year}")
        self.logger.info(f"Lower boundary file: {lower_path}")
        self.logger.info(f"Upper boundary file: {upper_path}")

        # Parcels DataFrame after interpolation
        interpolated_synpop = interpolation.interpolate(left_synpop, right_synpop, horizon_year)
        return interpolated_synpop

    def _on_interpolation_finished(self, synpop : SyntheticPopulation):
        self.base_synpop = synpop
        self.enableAllButtons()
        self.status_sections[0].setText('Done')
        self.base_filename_label.setText(self.base_synpop.filename)
        self.base_file = self.base_synpop.filename

    def _on_interpolation_error(self, exception_obj):
        # called when the thread encounters an error
        self.enableAllButtons()
        self.status_sections[0].setText("interpolation failed")
        QMessageBox.critical(self, "Error", str(exception_obj))
                             
    def validate_btn_clicked(self):
        self.status_sections[0].setText("running")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.base_synpop.validate_hhs_persons)
        self.worker.finished.connect(lambda validate_dict: self._on_validation_finished(validate_dict))
        self.worker.error.connect(lambda message: self._on_validation_error(self.status_sections[0], message))
        self.worker.start()
        
    def _on_validation_finished(self, validate_dict):
        # called when the thread is finished
        self.enableAllButtons()
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the base parcel data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def _on_validation_error(self, status_bar_section, message):
        # called when the thread encounters an error
        self.enableAllButtons()
        status_bar_section.setText("Error")
        QMessageBox.critical(self, "Error", message)

    def summarize_btn_clicked(self):
        self.status_sections[0].setText("running")
        self.disableAllButtons()
        
        self.worker = ThreadWrapper(self.base_synpop.summarize_synpop, self.parent().output_dir, 'base', False, True)
        self.worker.finished.connect(lambda summary_dict: self._on_summary_thread_finished(summary_dict))
        self.worker.error.connect(lambda message: self._on_validation_error(self.status_sections[0], message))
        self.worker.start()

    def _on_summary_thread_finished(self, data_dict):
        self.status_sections[0].setText("Done")
        self.enableAllButtons()

        summary_dialog = ValidationAndSummary(self, "Base Synthetic Population Summary", data_dict)
        summary_dialog.exec()           


    def closeEvent(self, event):
        self.logger.info("Synthetic Population User Interface is closed.")
        self.accept()
        event.accept()   
