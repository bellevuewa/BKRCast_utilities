import sys
import os
import logging
from datetime import datetime
import pandas as pd

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QSizePolicy,
     QMainWindow
)
from PyQt6.QtGui import QIntValidator

from parcel_data_processor import ParcelProcessor
from land_use_data_preprocessor import LUPreprocessUserInterface
import land_use_data_processor_utilities as LU_utility
from GUI_support_utilities import (Shared_GUI_Widgets, ValidationAndSummary)
from SynPopDataUserInterface import SynPopDataUserInterface
from allocate_hhs_to_parcels import HouseholdAllocation

from LandUseUtilities.synthetic_population import SyntheticPopulation
from LandUseUtilities.Parcels import Parcels
from utility import setup_logger_file, dialog_level, _LOGGING_CONFIGURED, ThreadWrapper


class LandUseDataUserInterface(QMainWindow, Shared_GUI_Widgets):
    """Main window for the Land Use Data Processor application."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Land Use Data Processor")
        self.setMinimumWidth(750)

        self.project_settings = {
            'horizon_year': 2044,
            'scenario_name': 'long_range_planning',
            'output_dir': r'Z:\Modeling Group\BKRCast\LandUse\test_2044_long_range_planning',
            'subarea_df': pd.read_csv(r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"),
            'lookup_df': pd.read_csv(r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv"),
            'subarea_file': r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv",
            'lookup_file': r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv",
        }

        self._init_ui() 
        self.create_status_bar(self, 4)

        self.year_box.setText(str(self.project_settings['horizon_year']))
        self.scen_input_editbox.setText(self.project_settings['scenario_name'])

        self.logger = None # to be initialized after output dir is selected.
        self.indent = 0 # for log entry
        
    def _init_ui(self):
        """Initialize the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QVBoxLayout()
        central_widget.setLayout(self.main_layout)

        hbox = QHBoxLayout()
        hbox.addWidget(QLabel("Horizon Year"))
        self.year_box = QLineEdit()
        self.year_box.setValidator(QIntValidator(2000, 2100))
        self.year_box.setMaxLength(4)
        hbox.addWidget(self.year_box)

        # add scenario input box
        self.scen_input_editbox = QLineEdit()
        self.scen_input_editbox.setPlaceholderText("Scenario Name")
        hbox.addWidget(self.scen_input_editbox)
        self.main_layout.addLayout(hbox) 

        hbox = QHBoxLayout()
        output_button = QPushButton("Select Output Location")
        output_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.output_label = QLabel("No Location selected")
        output_button.clicked.connect(self.browse_output_file)
        hbox.addWidget(output_button)
        hbox.addWidget(self.output_label)
        self.main_layout.addLayout(hbox) 

        # add two buttons on one line for selecting subarea and look up files
        hbox = QHBoxLayout()
        subarea_button = QPushButton("Select Subarea File")
        subarea_button.clicked.connect(self.select_subarea_file)
        hbox.addWidget(subarea_button)

        lookup_button = QPushButton("Select Parcel Lookup File")
        lookup_button.clicked.connect(self.select_lookup_file)
        hbox.addWidget(lookup_button)
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        popsim_button = QPushButton("Generate Control File for PopSim")
        popsim_button.clicked.connect(self.popsim_button_clicked)
        hbox.addWidget(popsim_button)
        allocate_parcel_button = QPushButton("Allocate Hhs to Parcel")
        allocate_parcel_button.clicked.connect(self.allocate_parcel_button_clicked)
        hbox.addWidget(allocate_parcel_button)
        wfh_btton = QPushButton('WFH COB Method')
        wfh_btton.clicked.connect(self.wfh_btton_clicked)
        hbox.addWidget(wfh_btton)
        self.main_layout.addLayout(hbox)

        hbox = QHBoxLayout()
        self.sum_parcel_button = QPushButton("Summarize a Parcel File")
        self.sum_parcel_button.clicked.connect(self.sum_parcel_button_clicked)
        hbox.addWidget(self.sum_parcel_button)
        self.sum_popsim_button = QPushButton("Summarize Synthetic Population")
        self.sum_popsim_button.clicked.connect(self.sum_popsim_button_clicked)
        hbox.addWidget(self.sum_popsim_button)
        self.main_layout.addLayout(hbox)

        parcel_button = QPushButton("Assemble a New Parcel Data from Different Parcel Files")
        parcel_button.clicked.connect(self.parcel_btn_clicked)
        self.main_layout.addWidget(parcel_button)

        preprocess_button = QPushButton("Preprocess Land Use Data for BKRCast")
        preprocess_button.clicked.connect(self.preprocess_btn_clicked)
        self.main_layout.addWidget(preprocess_button)

        new_parcels_button = QPushButton("Process Parcel Data from Partner Cities")
        new_parcels_button.clicked.connect(self.new_parcels_btn_clicked)
        self.main_layout.addWidget(new_parcels_button)

        self.disableAllButtons()
        output_button.setEnabled(True)

    def load_settings(self):
        """Load settings from the UI."""

        if (self.year_box.text() == "") :
            QMessageBox.critical(self, "Warning", "Please enter the horizon year")
            return False

        if (self.year_box.hasAcceptableInput()):
            self.horizon_year = int(self.year_box.text())
            if self.horizon_year > 2100 or self.horizon_year < 2000:
                QMessageBox.critical(self, "Warning", "Please double check the horizon year input")
                return False
        else:
            return False
        
        horizon_year = int(self.year_box.text()) if self.year_box.text().isdigit() else -1
        if horizon_year == -1:
            QMessageBox.warning(self, "Input Error", "Please enter a valid horizon year.")
            return False

        self.logger.info(f"Horizon year: {horizon_year}")
        scenario_name = self.scen_input_editbox.text().strip()
        if not scenario_name:
            QMessageBox.warning(self, "Input Error", "Please enter a valid scenario name.")
            return False
        self.logger.info(f"Scenario name: {scenario_name}")
        if self.project_settings["output_dir"] == "" or self.project_settings["output_dir"] is None:
            QMessageBox.warning(self, "Input Error", "Please select an output location.")
            return False
        self.logger.info(f"Output directory: {self.project_settings['output_dir']}")
        if (self.project_settings['subarea_df'] is None) or (self.project_settings['lookup_df'] is None):
            QMessageBox.warning(self, "Input Error", "Please select both subarea and parcel lookup files.")
            return False

        self.project_settings['horizon_year'] = horizon_year
        self.project_settings['scenario_name'] = scenario_name

        return True

    def select_subarea_file(self):
        # open file dialog to select subarea file
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Subarea File", "", "CSV Files (*.csv);;All Files (*)")
        if file_name:
            self.logger.info(f"Selected subarea file: {file_name}")
            self.project_settings['subarea_df'] = pd.read_csv(file_name)
            self.project_settings['subarea_file'] = file_name
            self.status_sections[1].setText("Subarea file loaded.")

    def select_lookup_file(self):
        # open file dialog to select parcel lookup file
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Parcel Lookup File", "", "CSV Files (*.csv);;All Files (*)")
        if file_name:
            self.logger.info(f"Selected parcel lookup file: {file_name}")
            self.project_settings['lookup_df'] = pd.read_csv(file_name, low_memory=False)
            self.project_settings['lookup_file'] = file_name
            self.status_sections[1].setText("Parcel lookup file loaded.")

    def browse_output_file(self):
        path = QFileDialog.getExistingDirectory(
            self, "Select Parcel Output Folder", os.getcwd(),
        )
        if path:
            self.output_label.setText(path)
            self.project_settings["output_dir"] = path
            horizon_year = int(self.year_box.text()) if self.year_box.text().isdigit() else -1
            self.logger = setup_logger_file(path, f'{horizon_year}_{self.scen_input_editbox.text().strip()}_land_use_data_processor_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            self.logger.info(f"Output folder set to: {path}")
            self.status_sections[0].setText("log initialized.")
            self.enableAllButtons()

            self.indent = dialog_level(self)

    def parcel_btn_clicked(self):
        self.load_settings()
        parcel_processor = ParcelProcessor()
        parcel_processor.exec()

    def popsim_button_clicked(self):
        self.load_settings()
        processor = SynPopDataUserInterface(self.project_settings, self)
        processor.exec()
    
    def new_parcels_btn_clicked(self):
        self.load_settings()
        processor = LU_utility.ParcelDataUserInterface(self.project_settings, self)
        processor.exec()

    def preprocess_btn_clicked(self):
        self.load_settings()
        processor = LUPreprocessUserInterface(self.project_settings, self)
        processor.exec()

    def closeEvent(self, event):
        if self.logger != None: 
            self.logger.info("Land Use Data Process closed.")
            logging.shutdown()
        event.accept()  

    def allocate_parcel_button_clicked(self):
        self.load_settings()
        dialog = HouseholdAllocation(self.project_settings, self)
        dialog.exec()

    def sum_popsim_button_clicked(self):
        if (self.project_settings['subarea_df'] is None):
            QMessageBox.critical(self, "Error", "Select the subarea definition file.")
            return
        
        if (self.project_settings['lookup_df'] is None):
            QMessageBox.critical(self, "Error", "Select the lookup file.")
            return
        
        file_name, _ = QFileDialog.getOpenFileName(self, "Select a PopSim h5 File", "", "H5 Files (*.h5);;All Files (*)")
        import h5py
        if file_name == '':
            QMessageBox.critical(self, "Error", "Select a h5 file.")
            return
        
        self.status_sections[0].setText("Summarizing Popsim")
        synpop = SyntheticPopulation(self.project_settings['subarea_file'], self.project_settings['lookup_file'],
                                     file_name, self.project_settings['horizon_year'], self.indent + 1)

        self.disableAllButtons()
        self.worker = ThreadWrapper(synpop.summarize_synpop, self.project_settings['output_dir'], '', False, True)
        self.worker.finished.connect(lambda summary_dict: self._on_summary_thread_finished(summary_dict, "Synthetic Population Summary"))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.summarize_btn, self.status_sections[0], message))
        self.worker.start()        

    def _on_summary_thread_finished(self, data_dict, message):
        self.status_sections[0].setText("Done")
        self.enableAllButtons()

        summary_dialog = ValidationAndSummary(self, message, data_dict)
        summary_dialog.exec()            

    def sum_parcel_button_clicked(self):
        if (self.project_settings['subarea_df'] is None):
            QMessageBox.critical(self, "Error", "Select the subarea definition file.")
            return
        
        if (self.project_settings['lookup_df'] is None):
            QMessageBox.critical(self, "Error", "Select the lookup file.")
            return
        
        file_name, _ = QFileDialog.getOpenFileName(self, "Select a Parcel File", "", "txt File (*.txt);;All Files (*)")
        if file_name == '':
            QMessageBox.critical(self, "Error", "Select the parcel file.")
            return
        
        self.status_sections[0].setText("Summarizing parcel file")
       
        self.worker = ThreadWrapper(self.summarize_parcel_data, file_name)
        self.worker.finished.connect(lambda summary_dict: self._on_summary_thread_finished(summary_dict, "Parcel File Summary"))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.summarize_btn, self.status_sections[0], message))
        self.worker.start()       

    def summarize_parcel_data(self, parcel_filename) -> dict: 
        '''
        summarize parcel file and return the summary in dict
        
        :param parcel_filename: parcel file name (with path)
        :return: parcel data summary by jurisdiction, subarea, and TAZ
        :rtype: dict
        '''
        parcels = Parcels(self.project_settings['subarea_file'], self.project_settings['lookup_file'], parcel_filename, self.project_settings['horizon_year'], self.indent + 1)
        summary_dict = parcels.summarize_parcel_data(self.project_settings['output_dir'], '')
        return summary_dict

    def wfh_btton_clicked(self):
        h5_file_name, _ = QFileDialog.getOpenFileName(self, "Select the Synthetic Population File", "", "h5 File (*.h5);;All Files (*)")
        if h5_file_name == '':
            QMessageBox.critical(self, "Error", "Select the Synthetic Population File.")
            return    

        wfh_rate_file_name, _ = QFileDialog.getOpenFileName(self, "Select the WFH Rate File", "", "csv File (*.csv);;All Files (*)")
        if wfh_rate_file_name == '':
            QMessageBox.critical(self, "Error", "Select the Synthetic Population File.")
            return           

        out_put_h5_file, _ = QFileDialog.getSaveFileName(self, 'Save Synthetic Population', self.project_settings['output_dir'], "H5 Files (*.h5);;All Files (*)")
        if out_put_h5_file:
            self.status_sections[0].setText("Generating")
            self.disableAllButtons()

            self.worker = ThreadWrapper(self.wfh_generating, wfh_rate_file_name, out_put_h5_file, h5_file_name)
            self.worker.finished.connect(lambda: self._on_process_thread_finished( self.status_sections[0], ''))
            self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
            self.worker.start()  
        

    def wfh_generating(self, wfh_rate_file_name, output_h5_file, input_popsim_file):
        import debugpy
        debugpy.breakpoint()

        synpop = SyntheticPopulation(self.project_settings['subarea_file'], self.project_settings['lookup_file'],
                                     input_popsim_file, self.project_settings['horizon_year'], self.indent + 1)   
        synpop.adjust_worker_status_for_WFH(wfh_rate_file_name, output_h5_file)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = LandUseDataUserInterface()
    window.show()
    sys.exit(app.exec())