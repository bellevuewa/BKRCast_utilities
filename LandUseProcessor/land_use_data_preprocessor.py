import sys, os
sys.path.append(os.getcwd())
import logging
from datetime import datetime
import pandas as pd
from PyQt6.QtWidgets import (
    QWidget, QLabel, QPushButton, QCheckBox,
    QFileDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QDialog,
    QListWidget, QSizePolicy
)

from utility import *
from GUI_support_utilities import (Shared_GUI_Widgets)

class LUPreprocessUserInterface(QDialog, Shared_GUI_Widgets):
    """Main window for the Land Use Data Preprocessor application."""
    def __init__(self, project_settings, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Land Use Data Preprocessor")
        self.setMinimumWidth(750)

        self.subarea_df = project_settings['subarea_df']
        self.lookup_df = project_settings['lookup_df']
        self.output_dir = project_settings['output_dir']
        self.horizon_year = project_settings['horizon_year']
        self.scenario_name = project_settings['scenario_name']
        self.project_settings = project_settings

        # Further UI initialization code would go here
        self._init_ui()
        self.create_status_bar(self, 4)
        base_logger = logging.getLogger(__name__)
        indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, indent)
        self.logger.info("Land Use Data Preprocessor UI initialized.")


    def _init_ui(self):
        """Initialize the user interface."""
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)
     
        hbox = QHBoxLayout()
        # selection of output folder
        hbox.addWidget(QLabel("Output Folder:"))
        self.output_folder_label = QLabel(self.project_settings['output_dir'] if self.project_settings['output_dir'] != "" else "No folder selected")
        hbox.addWidget(self.output_folder_label)
        self.main_layout.addLayout(hbox)
        
        # add a list box with items showing subarea
        hbox = QHBoxLayout()
        subarea_list_label = QLabel("Selected Jurisdictions for Processing:")
        subset_area = ['BELLEVUE', 'KIRKLAND','REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe'] 
        self.subarea_list = QListWidget()
        self.subarea_list.addItems(subset_area)
        self.subarea_list.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.subarea_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.subarea_list.setStyleSheet("""
            QListWidget::item:selected {
                background: palette(highlight);
                color: palette(highlighted-text);
            }
            """)
        self.subarea_list.itemSelectionChanged.connect(self.on_subarea_selection_changed)
        vbox = QVBoxLayout()
        vbox.addWidget(subarea_list_label)
        vbox.addWidget(self.subarea_list)
        hbox.addLayout(vbox)

        vbox = QVBoxLayout()

        hbox2 = QHBoxLayout()
        # add a checkbox for SQFT data availability
        self.sqft_checkbox = QCheckBox("SQFT data included")
        self.sqft_checkbox.setChecked(False)
        hbox2.addWidget(self.sqft_checkbox)

        vbox.addLayout(hbox2)
        # add buttons for Bellevue, Kirkland, Redmond
        bellevue_button = QPushButton("Preprocess Bellevue Land Use Data")
        bellevue_button.clicked.connect(self.bellevue_btn_clicked)
        vbox.addWidget(bellevue_button)

        kirkland_button = QPushButton("Preprocess Kirkland Land Use Data")
        kirkland_button.clicked.connect(self.kirkland_btn_clicked)
        vbox.addWidget(kirkland_button)

        redmond_button = QPushButton("Preprocess Redmond Land Use Data")
        redmond_button.clicked.connect(self.redmond_btn_clicked)    
        vbox.addWidget(redmond_button)
        hbox.addLayout(vbox)
        self.main_layout.addLayout(hbox)  

    def on_subarea_selection_changed(self):
        if len(self.subarea_list.selectedItems()) == 0:
            self.status_sections[1].setText("")
            return
        self.status_sections[1].setText(f"{len(self.subarea_list.selectedItems())} jurisdictions selected")

    def kirkland_btn_clicked(self):

        self.status_sections[0].setText("Kirkland data processed.")
        pass

    def redmond_btn_clicked(self):
        self.status_sections[0].setText("Redmond data processed.")
        pass

    def bellevue_btn_clicked(self):
        # open file dialog to select Bellevue land use data file
        self.logger.info("Bellevue land use data preprocessing started.")
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Bellevue Land Use Data File", "", "CSV Files (*.csv);;All Files (*)")
        if file_name:
            # call preprocessing function for Bellevue
            self.logger.info(f"Selected Bellevue land use data file: {file_name}")
            # Further processing code would go here
         
        data_df = pd.read_csv(file_name)
        SQFT_data_available = self.sqft_checkbox.isChecked()
        subset_area = [item.text() for item in self.subarea_list.selectedItems()]
        self.logger.info(f"SQFT data available: {SQFT_data_available}") 
        self.logger.info(f"Selected jurisdictions for processing: {subset_area}")
        if not subset_area:
            QMessageBox.warning(self, "No jurisdiction Selected", "Please select at least one jurisdiction for processing.")
            return

        ## rename columns to fit modeling input format
        data_df.rename(columns = job_rename_dict, inplace = True)
        data_df.rename(columns = sqft_rename_dict, inplace = True)
        data_df.rename(columns = du_rename_dict, inplace = True)

        # create output file names based on scenario name and selected jurisdictions
        cob_du_file = f"{self.horizon_year}_{self.scenario_name}_cob_housingunits.csv" if self.scenario_name != "" else f"{self.horizon_year}_cob_housingunits.csv"
        if len(subset_area) > 1:
            kc_job_file = f"{self.horizon_year}_{self.scenario_name}_bkr_jobs.csv" if self.scenario_name != "" else f"{self.horizon_year}_bkr_jobs.csv"
            kc_SQFT_file = f"{self.horizon_year}_{self.scenario_name}_bkr_sqft.csv" if self.scenario_name != "" else f"{self.horizon_year}_bkr_sqft.csv"
            kc_du_file = f"{self.horizon_year}_{self.scenario_name}_bellevue_housingunits.csv" if self.scenario_name != "" else f"{self.horizon_year}_bellevue_housingunits.csv"
            error_parcel_file = f"bkr_parcels_not_in_2014_PSRC_parcels.csv"
        else:
            kc_job_file = f"{self.horizon_year}_{self.scenario_name}_{subset_area[0].lower()}_jobs.csv" if self.scenario_name != "" else f"{self.horizon_year}_{subset_area[0].lower()}_jobs.csv"
            kc_SQFT_file = f"{self.horizon_year}_{self.scenario_name}_{subset_area[0].lower()}_sqft.csv" if self.scenario_name != "" else f"{self.horizon_year}_{subset_area[0].lower()}_sqft.csv"
            kc_du_file = f"{self.horizon_year}_{self.scenario_name}_{subset_area[0].lower()}_housingunits.csv" if self.scenario_name != "" else f"{self.horizon_year}_{subset_area[0].lower()}_housingunits.csv"
            error_parcel_file = f"{subset_area[0].lower()}_parcels_not_in_2014_PSRC_parcels.csv"

        jobs_columns_List = ["PSRC_ID"] + Job_Categories + ["EMPTOT_P"]
        updated_jobs_kc = data_df[jobs_columns_List].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
        updated_jobs_kc = updated_jobs_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
        if not subset_area:
            updated_jobs_kc = updated_jobs_kc[updated_jobs_kc['Jurisdiction'].isin(subset_area)]
        updated_jobs_kc['EMPTOT_P'] = updated_jobs_kc[Job_Categories].sum(axis = 1)    
        updated_jobs_kc.to_csv(os.path.join(self.output_dir, kc_job_file), sep = ',', index = False)
        self.logger.info(f"Exported job file: {os.path.join(self.output_dir, kc_job_file)}")

        if SQFT_data_available:  
            print('Exporting sqft file...')
            
            sqft_cat_list = ['SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH']
            sqft_columns_list = ['PSRC_ID'] + sqft_cat_list + ['SQFT_TOT']
            updated_sqft_kc = data_df[sqft_columns_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
            updated_sqft_kc = updated_sqft_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
            if not subset_area:
                updated_sqft_kc = updated_sqft_kc[updated_sqft_kc['Jurisdiction'].isin(subset_area)]
            updated_sqft_kc['SQFT_TOT'] = updated_sqft_kc[sqft_cat_list].sum(axis = 1)       
            updated_sqft_kc.to_csv(os.path.join(self.output_dir, kc_SQFT_file), sep = ',', index = False)
            self.logger.info(f"Exported sqft file: {os.path.join(self.output_dir, kc_SQFT_file)}")

        dwellingunits_list = ['PSRC_ID', 'SFUnits', 'MFUnits']
        du_kc = data_df[dwellingunits_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
        du_kc = du_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
        if not subset_area:
            du_kc = du_kc[du_kc['Jurisdiction'].isin(subset_area)]
        du_kc.to_csv(os.path.join(self.output_dir, kc_du_file), sep  = ',', index = False)
        self.logger.info(f"Exported dwelling units file: {os.path.join(self.output_dir, kc_du_file)}")
        du_cob = du_kc[du_kc['Jurisdiction'] == 'BELLEVUE']
        du_cob.to_csv(os.path.join(self.output_dir, cob_du_file), sep = ',', index = False)
        self.logger.info(f"Exported COB dwelling units file: {os.path.join(self.output_dir, cob_du_file)}")
        print('Exporting error file...')
        error_parcels = data_df[~data_df['PSRC_ID'].isin(self.lookup_df['PSRC_ID'])]
        error_parcels.to_csv(os.path.join(self.output_dir, error_parcel_file), sep = ',', index = False)

        if error_parcels.shape[0] > 0:
            self.logger.info(f"Please check the error file first: {os.path.join(self.output_dir, error_parcel_file)}")
        self.logger.info("Bellevue land use data preprocessing completed.")
        self.status_sections[0].setText("Bellevue data processed.")

    def closeEvent(self, event):
        self.logger.info("Land Use Data Preprocessor closed.")
        event.accept()