import sys, os
sys.path.append(os.getcwd())
import logging
import pandas as pd
import numpy as np
import h5py
from pathlib import Path
from PyQt6.QtWidgets import (
      QLabel, QPushButton, QFileDialog, QVBoxLayout, QHBoxLayout, QSizePolicy, QDialog)
from GUI_support_utilities import (Shared_GUI_Widgets, ValidationAndSummary)
from utility import IndentAdapter, dialog_level, df_to_h5, ThreadWrapper
from LandUseUtilities.synthetic_population import SyntheticPopulation
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
        popsim_btn = QPushButton("Select Population Data Files")
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
        self.valid_btn.setEnabled(False)
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

        output_filename = f'{self.horizon_year}_{self.scenario_name}_hh_and_persons.h5'
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
        if 'VEH' in hhs_df.columns:
            hhs_df.rename(columns = {'VEH': 'hhvehs'}, inplace = True)

        hhs_by_GEOID10 = hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()
        parcels_for_allocation_df = pd.read_csv(os.path.join(self.output_dir, self.guide_filename))
        # remove any blockgroup ID is Nan.
        all_blcgrp_ids = hhs_df['block_group_id'].unique()
        mask = np.isnan(all_blcgrp_ids)
        all_blcgrp_ids = sorted(all_blcgrp_ids[~mask])

        # special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
        # we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
        parcels_for_allocation_df.loc[(parcels_for_allocation_df['GEOID10'] == 530619900020) & (parcels_for_allocation_df['total_hhs'] > 0), 'GEOID10'] = 530610521042
        parcels_for_allocation_df = parcels_for_allocation_df.loc[parcels_for_allocation_df['total_hhs'] > 0].copy()

        parcel_groups = {k: v.copy() for k, v in parcels_for_allocation_df.groupby('GEOID10')}
        hh_groups = {k: v.copy() for k, v in hhs_df.groupby('block_group_id')}
        hhs_by_GOEID10 = hhs_df.groupby('block_group_id')[['hhexpfac']].sum()
        final_hhs_list = []

        for idx, blcgrpid in enumerate(all_blcgrp_ids):
            if idx % 100 == 0:
                print(f'{idx} block group processed.')

            if blcgrpid not in parcel_groups:
                print(f'No parcel records for GEOID10 {blcgrpid}')
                continue

            if blcgrpid not in hh_groups:
                print(f'No households for GEOID10 {blcgrpid}')
                continue            

            parcels_in_GEOID10_df = parcel_groups[blcgrpid]
            selected_hhs_df = hh_groups[blcgrpid].copy()
            control_total = int(parcels_in_GEOID10_df['total_hhs'].sum())
            numhhs_avail_for_alloc = int(selected_hhs_df['hhexpfac'].sum())

            # create repeated parcel list
            parcel_ids = np.repeat(parcels_in_GEOID10_df['PSRC_ID'].to_numpy(), parcels_in_GEOID10_df['total_hhs'].astype(int).to_numpy())
            allocation_size = min(len(parcel_ids), selected_hhs_df.shape[0])
            selected_hhs_df.iloc[:allocation_size, selected_hhs_df.columns.get_loc('hhparcel')] = parcel_ids[:allocation_size]
            unallocated_num = (numhhs_avail_for_alloc - control_total)

            if unallocated_num < 0: 
                self.logger.info(f'Error GEOID10 {blcgrpid}: parcel control {control_total} is greater than available hhs {numhhs_avail_for_alloc}. ')
                continue

            if unallocated_num > 0:
                valid_pids = parcels_in_GEOID10_df['PSRC_ID']

                if len(valid_pids) == 0:
                    self.logger.info(f'Warning: No valid parcels for unallocated hhs in GEOID10 {blcgrpid}. Unallocated hhs: {unallocated_num}')
                    continue

                random_picked_pids = valid_pids.sample(n = unallocated_num, replace = True).to_numpy()
                start = allocation_size
                end = min(allocation_size + unallocated_num, selected_hhs_df.shape[0])
                selected_hhs_df.iloc[start:end, selected_hhs_df.columns.get_loc('hhparcel')] = random_picked_pids[: end - start]

            final_hhs_list.append(selected_hhs_df)
            self.logger.info(f'GEOID10 {blcgrpid}: control: {control_total} available: {numhhs_avail_for_alloc} allocated: {allocation_size}')

        self.logger.info('Concatenating households...')
        final_hhs_df = pd.concat(final_hhs_list, ignore_index = True, sort = False)

        self.logger.info('Adding TAZ info...')
        parcel_lookup = parcels_for_allocation_df[['PSRC_ID', 'BKRCastTAZ']].drop_duplicates('PSRC_ID')
        final_hhs_df = final_hhs_df.merge(parcel_lookup, how='left', left_on='hhparcel', right_on='PSRC_ID')
        final_hhs_df.rename(columns={'BKRCastTAZ': 'hhtaz'}, inplace=True)
        final_hhs_df.drop(columns=['PSRC_ID'], inplace=True, errors='ignore')

        self.logger.info('processing persons...')
        ### process other attributes to match required columns
        pop_df = pd.read_csv(os.path.join(self.output_dir, self.synthetic_person_filename)) 
        pop_df.rename(columns={'household_id':'hhno', 'SEX':'pgend'}, inplace = True)
        pop_df.sort_values(by = 'hhno', inplace = True)

        # initialize columns
        pop_df['pdairy'] = -1
        pop_df['ppaidprk'] = -1
        pop_df['psexpfac'] = 1
        pop_df['pspcl'] = -1
        pop_df['pstaz'] = -1
        pop_df['pptyp'] = -1
        pop_df['ptpass'] = -1
        pop_df['puwarrp'] = -1
        pop_df['puwdepp'] = -1
        pop_df['puwmode'] = -1
        pop_df['pwpcl'] = -1
        pop_df['pwtaz'] = -1

        self.logger.info('assigning person numbers...')
        pop_df['pno'] = pop_df.groupby('hhno').cumcount() + 1

        self.logger.info('processing person types...')
        pop_df['WKW'] = pop_df['WKW'].fillna(-1)
        pop_df['pstyp'] = pop_df['pstyp'].fillna(-1)
        ages = pop_df['pagey']
        # full time and part time workers: WKW from PUMS data
        fullworkers=[1, 2]
        partworkers=[3, 4, 5, 6]
        nonworkers=[-1]
        fullstudents = list(range(3, 17))
        nonstudents = [-1, 0, 1, 2]
        pp5=[15, 16]
        pp6=[13, 14]
        pp7=list(range(2, 13))
        pp8=[1]
        pop_df['pwtyp'] = 0

        pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pwtyp'] = 1
        pop_df.loc[pop_df['WKW'].isin(partworkers), 'pwtyp'] = 2
        pop_df.loc[pop_df['pstyp'].isin(nonstudents), 'pstyp'] = 0
        pop_df.loc[pop_df['pstyp'].isin(fullstudents), 'pstyp'] = 1
        pop_df.loc[pop_df['WKW'].isin(partworkers) & pop_df['pstyp'] == 1, 'pstyp'] = 2
        pop_df['pptyp'] = 4
        mask_nonworkers = pop_df['WKW'].isin(nonworkers) 
        pop_df.loc[mask_nonworkers & (ages >= 65), 'pptyp'] = 3
        pop_df.loc[mask_nonworkers & (ages.between(16, 64)), 'pptyp'] = 4 # inclusive on both ends
        pop_df.loc[mask_nonworkers & (ages.between(5, 15)), 'pptyp'] = 7
        pop_df.loc[mask_nonworkers & (ages < 5), 'pptyp'] = 8
        pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pptyp'] = 1
        pop_df.loc[pop_df['WKW'].isin(partworkers), 'pptyp'] = 2
        pop_df.loc[pop_df['pstyp'].isin(pp5), 'pptyp'] = 5
        pop_df.loc[pop_df['pstyp'].isin(pp6), 'pptyp'] = 6
        pop_df.loc[pop_df['pstyp'].isin(pp7), 'pptyp'] = 7
        pop_df.loc[pop_df['pstyp'].isin(pp8), 'pptyp'] = 8

        self.logger.info('updating household size...')
        hhsize_df = pop_df.groupby('hhno')[['psexpfac']].sum().reset_index()
        final_hhs_df.rename(columns = {'household_id': 'hhno'}, inplace = True)
        final_hhs_df = final_hhs_df.merge(hhsize_df, how = 'inner', on = 'hhno')
        final_hhs_df['hhsize'] = final_hhs_df['psexpfac']
        final_hhs_df.drop(columns = ['psexpfac'], inplace = True)

        dropcols = ['block_group_id', 'hh_id', 'PUMA', 'WKW']
        pop_df.drop(columns = dropcols, inplace = True, errors = 'ignore')
        if 'hownrent' in final_hhs_df.columns:
            final_hhs_df.drop(columns = ['hownrent'], inplace = True)

        final_hhs_df['hownrent'] = -1
        pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]

        self.logger.info('exporting h5 file...')
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
 