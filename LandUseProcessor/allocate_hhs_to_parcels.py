import sys, os
sys.path.append(os.getcwd())
import logging
import pandas as pd
import numpy as np
import h5py
from pathlib import Path
from PyQt6.QtWidgets import (
      QLabel, QPushButton, QFileDialog, QVBoxLayout, QHBoxLayout, QSizePolicy, QDialog)
from GUI_support_utilities import (Shared_GUI_Widgets)
from utility import IndentAdapter, dialog_level, df_to_h5
from synthetic_population import SyntheticPopulation
from land_use_data_processor_utilities import ThreadWrapper, ValidationAndSummary

class HouseholdAllocation(QDialog, Shared_GUI_Widgets):
    def __init__(self, project_setting, parent = None):
        super().__init__(parent)
        self.project_settings = project_setting
        self.output_dir = self.project_settings['output_dir']
        self.horizon_year = self.project_settings['horizon_year']
        self.scenario_name = self.project_settings['scenario_name']

        self.synthetic_household_filename = ''
        self.synthetic_person_filename = ''
        self.guide_filename = ''

        self.final_synpop : SyntheticPopulation = None
        self.__init_ui__()
        self.create_status_bar(self, 4)
        base_logger = logging.getLogger(__name__)
        self.indent = dialog_level(self)
        self.logger = IndentAdapter(base_logger, self.indent)
        self.logger.info("Household Allocation UI initialized.")

    def __init_ui__(self):
        self.setWindowTitle("Synthetic Household Allocation")
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

        vbox = QVBoxLayout()    
        popsim_btn = QPushButton("Select Base Population Data Files")
        popsim_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        vbox.addWidget(popsim_btn)
        hbox = QHBoxLayout()
        label1 = QLabel("Synthetic Household")
        hbox.addWidget(label1)        
        self.household_label = QLabel("No files selected") 
        hbox.addWidget(self.household_label)
        vbox.addLayout(hbox)
        hbox = QHBoxLayout()
        label2 = QLabel('Synthetic Persons')
        hbox.addWidget(label2)
        self.person_label = QLabel("No files selected")
        hbox.addWidget(self.person_label)
        vbox.addLayout(hbox)
        popsim_btn.clicked.connect(self.select_popsim_file)
        vbox.addWidget(self.household_label)  
        vbox.addWidget(self.person_label)
        self.main_layout.addLayout(vbox)

        hbox = QVBoxLayout()
        guide_button = QPushButton("Select Allocation Guide File")
        hbox.addWidget(guide_button)
        self.guide_label = QLabel("No file selected")
        hbox.addWidget(self.guide_label)
        guide_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        guide_button.clicked.connect(self.select_guide_file)
        self.main_layout.addLayout(hbox)

        allocate_button = QPushButton("Allocate Households")
        allocate_button.clicked.connect(self.allocate_button_clicked)
        self.main_layout.addWidget(allocate_button)

        hbox = QHBoxLayout()
        self.valid_btn = QPushButton("Validate")
        self.valid_btn.clicked.connect(self.validate_button_clicked)
        hbox.addWidget(self.valid_btn)

        self.summarize_btn = QPushButton("Summarize")
        self.summarize_btn.clicked.connect(self.summarize_button_clicked) 
        self.summarize_btn.setEnabled(False)
        hbox.addWidget(self.summarize_btn)
        self.main_layout.addLayout(hbox)

    def select_popsim_file(self):
        # select synthetic household file (popsim output)
        path, _ = QFileDialog.getOpenFileName(
            self, 'Select synthetic household file', "",
            "csv (*.csv);;All Files(*.*)"
        )

        if path:
            self.synthetic_household_filename = path
            self.household_label.setText(path)
            self.logger.info(f'synthetic households from popsim is {self.synthetic_household_filename}')

        # select synthetic person file (popsim output)
        path, _ = QFileDialog.getOpenFileName(
            self, 'Select synthetic person file', "",
            "csv (*.csv);;All Files(*.*)"
        )

        if path:
            self.synthetic_person_filename = path
            self.person_label.setText(path)
            self.logger.info(f'synthetic persons from popsim is {self.synthetic_person_filename}')

        self.status_sections[0].setText("synthetic population flies are selected.")

    def select_guide_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, 'Select allocation guide file', "",
            "csv (*.csv);;All Files(*.*)"
        )

        if path:
            self.guide_filename = path
            self.guide_label.setText(path)
            self.logger.info(f'Household allocation guide file is {self.synthetic_household_filename}')
       
    def allocate_button_clicked(self):
        self.status_sections[0].setText("Allocating households...")
        self.disableAllButtons()

        output_filename = f'{self.horizon_year}_{self.scenario_name}_hhs_and_persons.h5'
        self.worker = ThreadWrapper(self.allocate_households, output_filename)
        self.worker.finished.connect(lambda ret: self._on_process_thread_finished(self.status_sections[0], ret))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
        self.worker.start()
 
    def allocate_households(self, output_filename):
        import debugpy
        debugpy.breakpoint()
        self.final_synpop_h5_name = output_filename

        hhs_df = pd.read_csv(os.path.join(self.output_dir, self.synthetic_household_filename))
        hhs_df['hhparcel'] = 0
        hhs_by_GEOID10 = hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()

        parcels_for_allocation_df = pd.read_csv(os.path.join(self.output_dir, self.guide_filename))
        # remove any blockgroup ID is Nan.
        all_blcgrp_ids = hhs_df['block_group_id'].unique()
        mask = np.isnan(all_blcgrp_ids)
        all_blcgrp_ids = sorted(all_blcgrp_ids[~mask])

        # special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
        # we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
        parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == 530619900020) & (parcels_for_allocation_df['total_hhs'] > 0), 'GEOID10'] = 530610521042

        hhs_by_blkgrp_popsim = hhs_df.groupby('block_group_id')[['hhexpfac', 'hhsize']].sum()
        hhs_by_blkgrp_parcel = parcels_for_allocation_df.groupby('GEOID10')[['total_hhs']].sum()
        final_hhs_df = pd.DataFrame()

        for blcgrpid in all_blcgrp_ids:
            # if (hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac'] != hhs_by_blkgrp_parcel.loc[blcgrpid, 'total_hhs']):
            #     print(f"GEOID10 {blcgrpid}:  popsim: {hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac']}, parcel: {hhs_by_blkgrp_parcel.loc[blcgrpid, 'total_hhs']}")
            #     print('popsim should equal parcel. You need to fix this issue before moving forward.')
            #     exit(-1)
            num_parcels = 0 
            num_hhs = 0
            parcels_in_GEOID10_df = parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == blcgrpid) & (parcels_for_allocation_df['total_hhs'] > 0)]
            subtotal_parcels = parcels_in_GEOID10_df.shape[0]
            control_total = parcels_in_GEOID10_df['total_hhs'].sum()
            j_start_index = 0
            selected_hhs_df = hhs_df.loc[(hhs_df['block_group_id'] == blcgrpid) & (hhs_df['hhparcel'] == 0)].copy()
            numhhs_avail_for_alloc = selected_hhs_df['hhexpfac'].sum()
            index_hhparcel = selected_hhs_df.columns.get_loc('hhparcel')
            for i in range(subtotal_parcels):
                numHhs = parcels_in_GEOID10_df['total_hhs'].iat[i]
                parcelid = parcels_in_GEOID10_df['PSRC_ID'].iat[i]
                for j in range(int(numHhs)):
                    if num_hhs < numhhs_avail_for_alloc:
                        selected_hhs_df.iat[j + j_start_index, index_hhparcel] = parcelid 
                        num_hhs += 1          
                num_parcels += 1
                j_start_index += int(numHhs)

            ## take care some unallocated hhs here
            unallocated_num = numhhs_avail_for_alloc - control_total
            if unallocated_num > 0:
                for j in range(int(unallocated_num)):
                    if (j + j_start_index) < selected_hhs_df.shape[0]:
                        random_picked_pids = parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == blcgrpid) & (parcels_for_allocation_df['total_hhs'] > 0)].sample(n = unallocated_num)['PSRC_ID'].to_numpy()
                        selected_hhs_df.iat[j + j_start_index, index_hhparcel] = random_picked_pids[j] 

            final_hhs_df = pd.concat([final_hhs_df, selected_hhs_df])

            print(f"Control: {control_total}, {hhs_by_GEOID10.loc[blcgrpid, 'hhexpfac']} (actual {num_hhs}) hhs allocated to GEOID10 {blcgrpid}, {num_parcels} parcels are processed")

        final_hhs_df = final_hhs_df.merge(parcels_for_allocation_df[['PSRC_ID', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        final_hhs_df.rename(columns = {'BKRCastTAZ': 'hhtaz'}, inplace = True)
        final_hhs_df.drop(columns = ['PSRC_ID'], axis = 1, inplace = True)

        # -1 pdairy ppaidprk pspcl,pstaz ptpass,puwarrp,puwdepp,puwmode,pwpcl,pwtaz 
        # pstyp is covered by pptyp and pwtyp, misssing: puwmode -1 puwdepp -1 puwarrp -1 pwpcl -1 pwtaz -1 ptpass -1  pspcl,pstaz 
        # 1 psexpfac 
        extra_cols = {
            'pdairy': -1, 'pno': -1, 'ppaidprk': -1, 'psexpfac': 1, 'pspcl': -1,
            'pstaz': -1, 'pptyp': -1, 'ptpass': -1, 'puwarrp': -1, 'puwdepp': -1,
            'puwmode': -1,'pwpcl': -1, 'pwtaz': -1
        }

        pop_df = pd.read_csv(os.path.join(self.output_dir, self.synthetic_person_filename)) 
        pop_df.rename(columns={'household_id':'hhno', 'SEX':'pgend'}, inplace = True)
        for col, val in extra_cols.items():
            pop_df[col] = val
        pop_df.sort_values(by = 'hhno', inplace = True)
        ages = pop_df['pagey']
        pop_df['WKW'] = pop_df['WKW'].fillna(-1)
        pop_df['pstyp'] = pop_df['pstyp'].fillna(-1)      
        ####here assign household size in household size and person numbers in person file
        hhsize_df = pop_df.groupby('hhno')[['psexpfac']].sum().reset_index()
        final_hhs_df.rename(columns = {'household_id':'hhno'}, inplace = True)
        final_hhs_df = final_hhs_df.merge(hhsize_df, how = 'inner', left_on = 'hhno', right_on = 'hhno')
        final_hhs_df['hhsize'] = final_hhs_df['psexpfac']
        final_hhs_df.drop(['psexpfac'], axis = 1, inplace = True)
        final_hhs_df['hownrent'] = -1
        #=========================================
     
        pwtype = pop_df['WKW']
        pstype = pop_df['pstyp']
        set(pwtype)

        fullworkers = pop_df['WKW'].isin([1, 2])
        # fullworkers=[1, 2]
        partworkers = pop_df['WKW'].isin([3.0, 4.0, 5.0, 6.0])
        # partworkers=[3.0, 4.0, 5.0, 6.0]
        noworker = pop_df['WKW'].isin([-1])
        # noworker=[-1]

        fullstudents = pop_df['pstyp'].between(3, 16) # inclusive both ends
        # fullstudents=[3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]
        nostudents = pop_df['pstyp'].isin([-1, 0, 1.0, 2.0])
        # nostudents = [-1, 0, 1.0, 2.0]
        pp5 = pop_df['pstyp'].isin([15, 16])
        # pp5=[15, 16]
        pp6 = pop_df['pstyp'].isin([13.0, 14.0])
        # pp6=[13.0, 14.0]
        pp7 = pop_df['pstyp'].between(2.0, 12.0) # inclusive both ends
        # pp7=[2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        pp8 = pop_df['pstyp'].isin([1])
        # pp8=[1]

        lenpersons=pop_df.shape[0] #3726050
        #pptyp Person type (1=full time worker, 2=part time worker, 3=non-worker age 65+, 4=other non-working adult, 
        #5=university student, 6=grade school student/child age 16+, 7=child age 5-15, 8=child age 0-4); 
        #this could be made optional and computed within DaySim for synthetic populations based on ACS PUMS; for other survey data, the coding and rules may be more variable and better done outside DaySim

        pop_df['pno'] = pop_df.groupby('hhno').cumcount() + 1
        pop_df.loc[nostudents, 'pstyp'] = 0
        pop_df.loc[fullstudents, 'pstyp'] = 1
        pop_df['pwtyp'] = 0
        pop_df.loc[fullworkers, ['pwtyp', 'pptyp']]= 1
        pop_df.loc[partworkers, ['pwtyp', 'pptyp']] = 2
        pop_df.loc[noworker & ages.between(0, 5, inclusive='left'), 'pptyp'] = 8
        pop_df.loc[noworker & ages.between(5, 15), 'pptyp'] = 7
        pop_df.loc[noworker & (ages.between(15, 65, inclusive='left')), 'pptyp'] = 4        
        pop_df.loc[noworker & (ages >= 65), 'pptyp'] = 3

        pop_df.loc[partworkers & fullstudents, 'pstyp'] = 2

        pop_df.loc[pp5, 'pptyp'] = 5
        pop_df.loc[pp6, 'pptyp'] = 6        
        pop_df.loc[pp7, 'pptyp'] = 7 
        pop_df.loc[pp8, 'pptyp'] = 8 

        pop_df.drop(['block_group_id', 'hh_id', 'PUMA', 'WKW'], axis = 1, inplace = True)

        pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]

        output_fn = os.path.join(self.output_dir, output_filename)
        with h5py.File(output_fn, 'w') as output_h5_file:
            df_to_h5(final_hhs_df, output_h5_file, 'Household')
            df_to_h5(pop_df, output_h5_file, 'Person')
        self.logger.info(f'After allcoation, the synthetic population is saved in {output_filename}.')

        updated_persons_file_name = f'updated_{Path(self.synthetic_person_filename).name}'
        updated_hhs_file_name = f'updated_{Path(self.synthetic_household_filename).name}'
        pop_df.to_csv(os.path.join(self.output_dir, updated_persons_file_name), sep = ',', index = False)  
        final_hhs_df.to_csv(os.path.join(self.output_dir, updated_hhs_file_name), sep = ',', index = False)
        self.final_synpop = SyntheticPopulation(self.project_settings['subarea_file'], self.project_settings['lookup_file'], output_fn, self.project_settings['horizon_year'], self.indent + 1)
 
        self.logger.info(f'Total census block groups: {len(all_blcgrp_ids)}')
        self.logger.info(f'Final number of households: {final_hhs_df.shape[0]}')
        self.logger.info(f'Final number of persons: {pop_df.shape[0]}')

    def validate_button_clicked(self):
        self.status_sections[0].setText("Validating")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.final_synpop.validate_hhs_persons)
        self.worker.finished.connect(lambda validate_dict: self._on_valid_thread_finished(validate_dict))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
        self.worker.start()
        

    def _on_valid_thread_finished(self, validate_dict):
        # called when the thread is finished
        self.enableAllButtons()
        
        self.status_sections[0].setText("Done")
        # Add validation logic here
        valid_dialog = ValidationAndSummary(self, "Validation and Summary of the synthetic population h5 data", validate_dict)
        valid_dialog.exec()
        self.status_sections[0].setText("")

    def summarize_button_clicked(self):
        self.status_sections[0].setText("Summarizing")
        self.disableAllButtons()

        self.worker = ThreadWrapper(self.final_synpop.summarize_synpop, self.output_dir, 'final synthetic population')
        self.worker.finished.connect(lambda summary_dict: self._on_summary_thread_finished(summary_dict))
        self.worker.error.connect(lambda message: self._on_process_thread_error(self.status_sections[0], message))
        self.worker.start()

    def _on_summary_thread_finished(self, data_dict):
        self.status_sections[0].setText("Done")
        self.enableAllButtons()

        summary_dialog = ValidationAndSummary(self, "Base Synthetic Population Summary", data_dict)
        summary_dialog.exec()       
 