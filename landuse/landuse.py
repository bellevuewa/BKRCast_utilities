import os, sys
sys.path.append(os.getcwd())

import h5py
import logging
import pandas as pd

import utility
from config import *

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

logging.basicConfig(level=logging.INFO)


class LandUse:
    def __init__(self):
        """
        Initialize the class with data.
        :param data: Dictionary containing land use data.
        """
        logging.info('Loading....')
        #####
        # Step 1
        #####
        self.lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
        self.kc_df = pd.read_csv(os.path.join(working_folder_lu, kingcsqft), sep = ',', low_memory = False)
        self.subarea_df = pd.read_csv(subarea_file, sep = ',')

        # rename columns to fit modeling input format
        self.kc_df.rename(columns = job_rename_dict, inplace = True)
        self.kc_df.rename(columns = sqft_rename_dict, inplace = True)
        self.kc_df.rename(columns = du_rename_dict, inplace = True)

        #####
        # Step 2
        #####
        self.parcel_lookup_df = pd.read_csv(parcel_lookup_File_Name, sep  = ',', low_memory = False)
        self.parcels_df = pd.read_csv(os.path.join(working_folder_lu, parcel_data_file_name), sep = ',')

        #####
        # Step 3
        #####
        self.parcel_earlier_df = pd.read_csv(parcel_file_name_ealier, sep = ' ')
        self.parcel_earlier_df.columns = [i.upper() for i in self.parcel_earlier_df.columns]
        self.parcel_latter_df = pd.read_csv(parcel_file_name_latter, sep = ' ')
        self.parcel_latter_df.columns = [i.upper() for i in self.parcel_latter_df.columns]

        #####
        # Step 4
        #####
        self.new_parcel_data_df = pd.read_csv(os.path.join(working_folder_lu, new_parcel_data_file_name), sep = ',', low_memory = False)
        self.original_parcel_data_df = pd.read_csv(os.path.join(working_folder_lu, original_parcel_file_name), sep = ' ', low_memory = False)

    
    def step_1_prepare_land_use(self):
        """
        Step 1: prepare land use data for analysis.
        :return: Prepared land use data.
        """
        # 2/23/2021
        # this script is used to join parcel data from Community Development to BKRCastTAZ and subarea
        # # via PSRC_ID, and save jobs and sqft data to different data files. IF the parcels provided in the kingsqft file are not valid parcels in
        # lookup_file, these invalid parcels will be exported to error_parcel_file for further investigation. 
        # Sometimes BKRCastTAZ and subarea column in the data from CD are a little mismatched
        # # so it is always good to remap parcel data to lookup file to ensure we always
        # # summarize land use  on the same base data.

        # 2/28/2022
        # upgrade to Python 3.7

        # 5/1/2025
        # move the paths into config.py

        logging.info('Exporting job file...')
        updated_jobs_kc = self.kc_df[jobs_columns_List].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
        updated_jobs_kc = updated_jobs_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
        if subset_area != []:
            updated_jobs_kc = updated_jobs_kc[updated_jobs_kc['Jurisdiction'].isin(subset_area)]
        updated_jobs_kc['EMPTOT_P'] = updated_jobs_kc[job_cat_list].sum(axis = 1)    
        updated_jobs_kc.to_csv(os.path.join(working_folder_lu, kc_job_file), sep = ',', index = False)

        if SQFT_data_available: 
            logging.info('Exporting sqft file...')
            updated_sqft_kc = self.kc_df[sqft_columns_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
            updated_sqft_kc = updated_sqft_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
            if subset_area != []:
                updated_sqft_kc = updated_sqft_kc[updated_sqft_kc['Jurisdiction'].isin(subset_area)]
            updated_sqft_kc['SQFT_TOT'] = updated_sqft_kc[sqft_cat_list].sum(axis = 1)       
            updated_sqft_kc.to_csv(os.path.join(working_folder_lu, kc_SQFT_file), sep = ',', index = False)

        logging.info('Exporting King County dwelling units...')
        du_kc = self.kc_df[dwellingunits_list].merge(self.lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
        du_kc = du_kc.merge(self.subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
        if subset_area != []:
            du_kc = du_kc[du_kc['Jurisdiction'].isin(subset_area)]
        du_kc.to_csv(os.path.join(working_folder_lu, kc_du_file), sep  = ',', index = False)

        du_cob = du_kc[du_kc['Jurisdiction'] == 'BELLEVUE']
        du_cob.to_csv(os.path.join(working_folder_lu, cob_du_file), sep = ',', index = False)

        error_parcels = self.kc_df[~self.kc_df['PSRC_ID'].isin(self.lookup_df['PSRC_ID'])]
        error_parcels.to_csv(os.path.join(working_folder_lu, error_parcel_file), sep = ',', index = False)
        if error_parcels.shape[0] > 0:
            logging.warning('Exporting error file...')
            logging.warning(f'Please check the error file first: {os.path.join(working_folder_lu, error_parcel_file)}')

        utility.backupScripts(__file__, os.path.join(working_folder_lu, os.path.basename(__file__)))
        logging.info('Step 1 done.\n')

    def step_2_validate_input_parcels(self):
        """
        Step 2: Validate parcel data from local jurisdiction.
        Validate the input parcel land use data file provided by community development. It checks:
                1. uniqueness of PSRC_ID
                2. parcels in lookup file but missing in the land use data file. 
                3. parcels in land use data file but not in lookup file. 
        :return: Validation results.
        """

        # 5/1/2025
        # move the paths into config.py
        # remove the year of 2014, replace it with year_parcel

        duplicated_parcels_df = self.parcels_df[self.parcels_df.duplicated('PSRC_ID', keep = False)]
        if duplicated_parcels_df.shape[0] != 0:
            logging.info('Some parcels have duplicated PSRC_ID. See duplicated_parcels.csv for details.')
            duplicated_parcels_df.to_csv(os.path.join(working_folder_lu, 'duplicated_parcels.csv'))
            # export cleaned copy, only keep the first one if duplicated.
            self.parcels_df = self.parcels_df[~self.parcels_df.duplicated('PSRC_ID', keep = 'first')]
            self.parcels_df.to_csv(os.path.join(working_folder_lu, 'cleaned_' + parcel_data_file_name), index = False)
        else:
            logging.info('No parcels with duplicated PSRC_ID is found. ')

        parcels_df = self.parcels_df.groupby('PSRC_ID').sum()

        # export parcels that are given in the parcel_data_file_name but are not included in parcel_lookup_File_Name
        not_in_year_PSRC_parcels = parcels_df.loc[~parcels_df.index.isin(self.parcel_lookup_df['PSRC_ID'])]
        if not_in_year_PSRC_parcels.empty == False:
            not_in_year_PSRC_parcels.to_csv(os.path.join(working_folder_lu, f'parcels_not_in_{year_parcel}PSRC_parcels.csv'))
        else:
            logging.info('All parcels given are within parcel lookup file.')

        if Jurisdiction != None:
            selected_parcels_lookup_df = self.parcel_lookup_df.loc[self.parcel_lookup_df['Jurisdiction'] == Jurisdiction]
        else:
            selected_parcels_lookup_df = self.parcel_lookup_df

        # export parcels that are in parcel_lookup_File_Name but not in the parcel_data_file_name.
        not_in_given_parcel_dataset = selected_parcels_lookup_df.loc[~selected_parcels_lookup_df['PSRC_ID'].isin(parcels_df.index)]
        if not_in_given_parcel_dataset.empty == False:
            not_in_given_parcel_dataset.to_csv(os.path.join(working_folder_lu, f'{year_parcel}PSRC_parcels_not_in_given_parcel_dataset.csv'))
            logging.warning(f'Some {year_parcel} PSRC parcels are missing from the given parcel dataset.')
            logging.warning(f"Go to {os.path.join(working_folder_lu, f'{year_parcel}PSRC_parcels_not_in_given_parcel_dataset.csv')} and check the output error file for details. ")
        else:
            logging.info(f'No {year_parcel} PSRC parcels are missing in the given parcel dataset.')

        logging.info('Step 2 done.')
        logging.info('Please make sure the output numbers making sense. Then continue the steps in the Synthetic Population folder.\n')

    def step_3_interpolate_parcel_files(self):
        """
        Step 3: Interpolate_parcel_files2.py
            Interpolate parcel files between what PSRC provided and the parcel data in the horizon year
            Very often PSRC will not have a parcel file consistent with our horizon year. Interpolation bewteen two different horizon years is unavoidable. We use
            interpolated parcel file for outside of King County or outside of BKR area. Inside BKR area, we always have our own local estimates of jobs.
            Create a new parcel file by interpolating employment bewteen two parcel files. The newly created parcel file has other non-job values
            from parcel_file_name_ealier.
        """
        columns = job_cat_list
        columns.append('PARCELID')
        job_std = copy.copy(job_cat_list)
        job_std.extend(['STUGRD_P', 'STUHGH_P', 'STUUNI_P'])

        self.parcel_latter_df.set_index('PARCELID', inplace = True)
        parcels_from_latter_df = self.parcel_latter_df.loc[:, job_std].copy(deep=True)
        parcels_from_latter_df.columns = [i + '_L' for i in parcels_from_latter_df.columns]
        parcels_from_latter_df['EMPTOT_L'] = 0
        for cat in job_cat_list:
            parcels_from_latter_df['EMPTOT_L'] = parcels_from_latter_df[cat + '_L'] + parcels_from_latter_df['EMPTOT_L']

        logging.info('Total jobs in year ', future_year, ' are ', parcels_from_latter_df['EMPTOT_L'].sum())
        parcel_horizon_df = self.parcel_earlier_df.merge(parcels_from_latter_df.reset_index(), how = 'inner', left_on = 'PARCELID', right_on = 'PARCELID')

        parcel_horizon_df['EMPTOT_E'] = 0
        for cat in job_cat_list:
            parcel_horizon_df['EMPTOT_E'] = parcel_horizon_df['EMPTOT_E'] + parcel_horizon_df[cat]
        parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_E']
        logging.info('Total jobs in year ', base_year, ' are ', parcel_horizon_df['EMPTOT_P'].sum())

        # interpolate number of jobs, and round to integer.
        for cat in job_std:
            parcel_horizon_df[cat] = parcel_horizon_df[cat] + ((target_year - base_year) * 1.0 / (future_year - base_year) * (parcel_horizon_df[cat + '_L'] - parcel_horizon_df[cat])) 
            parcel_horizon_df[cat] = parcel_horizon_df[cat].round(0).astype(int)

        parcel_horizon_df['EMPTOT_P'] = 0
        for cat in job_cat_list:
            parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_P'] + parcel_horizon_df[cat]

        parcel_horizon_df = parcel_horizon_df.drop([i + '_L' for i in job_std], axis = 1)
        parcel_horizon_df = parcel_horizon_df.drop(['EMPTOT_L', 'EMPTOT_E'], axis = 1)
        parcel_horizon_df.to_csv(os.path.join(working_folder_lu, new_parcel_file_name), index = False, sep = ' ')
        logging.info('After interpolation, total jobs are ', parcel_horizon_df['EMPTOT_P'].sum())

        utility.backupScripts(__file__, os.path.join(working_folder_lu, os.path.basename(__file__)))

        logging.info('Step 3 done.\n')

    def step_4_update_parcel_columns(self):
        """
        Step 4: replace_parcel_columns_with_new_tables_v2.py
            Replace parcel columns with new tables. Since CD will provide numebr of jobs instead of sqft, we will use this sript to 
            replace PSRC's pacel data within King County with sqft converted jobs from CD.
            We do not need to run other scripts to handle sqft and conversion so the land use
            preparation process becomes more straightforward and clean.
        """

        # 3/9/2022
        # upgraded to python 3.7

        # 2023
        # allow input jobs file using old trip model TAZ (originally for kirkland complan support)

        # 05/01/2025
        # move the paths into config.py

        logging.info('Processing Bellevue jobs...')
        full_bellevue_parcels_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == 'BELLEVUE']
        actual_bel_parcels_df = new_parcel_data_df.loc[new_parcel_data_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
        not_in_full_bellevue_parcels = actual_bel_parcels_df.loc[~actual_bel_parcels_df['PSRC_ID'].isin(full_bellevue_parcels_df['PSRC_ID'])]
        missing_bellevue_parcels_df = self.original_parcel_data_df.loc[self.original_parcel_data_df['PARCELID'].isin(full_bellevue_parcels_df.loc[~full_bellevue_parcels_df['PSRC_ID'].isin(new_parcel_data_df['PSRC_ID']), 'PSRC_ID'])]
        missing_bellevue_parcels_df.to_csv(os.path.join(working_folder_lu, 'missing_bellevue_parcels.csv'), sep = ',', index = False)
        not_in_full_bellevue_parcels.to_csv(os.path.join(working_folder_lu, 'not_valid_bellevue_parcels.csv'), sep = ',', index = False)

        newjobs = new_parcel_data_df['EMPTOT_P'].sum() 
        logging.info('new parcel data file has ' + str(newjobs)  + ' jobs.')
        new_parcel_data_df = new_parcel_data_df.set_index('PSRC_ID')
        updated_parcel_df = self.original_parcel_data_df.copy()
        updated_parcel_df = updated_parcel_df.set_index('PARCELID')
        oldjobs = updated_parcel_df.loc[updated_parcel_df.index.isin(new_parcel_data_df.index), 'EMPTOT_P'].sum()
        logging.info('Bellevue jobs before change: ' + str(oldjobs))
        logging.info('              after change: ' + str(newjobs))
        logging.info('Bellevue jobs gained ' + str(newjobs - oldjobs))
        updated_parcel_df.loc[updated_parcel_df.index.isin(new_parcel_data_df.index), columns_list] = new_parcel_data_df[columns_list]

        # update the total jobs 
        updated_parcel_df['EMPTOT_P'] = 0
        for col in columns_list:
            updated_parcel_df['EMPTOT_P'] += updated_parcel_df[col]     

        if set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File == True:
            jobs_to_be_zeroed_out = updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
            updated_parcel_df.loc[updated_parcel_df.index.isin(missing_bellevue_parcels_df['PARCELID']), columns_list] = 0
            logging.info('-----------------------------------------')
            logging.warning('Some COB parcels are not provided in the ' + new_parcel_data_file_name + '.')
            logging.warning('But they exist in ' + original_parcel_file_name + '.')
            logging.warning('Number of jobs in these parcels are now zeroed out: ' + str(jobs_to_be_zeroed_out))

        logging.info('processing Kirkland land use input...')
        # if jobs by old trip model taz (BKRTMTAZ) is provided (either by Kirkland or Redmond), we need to apply the following assumptions to 
        # scale BKRCast jobs up to match the provided jobs.
        '''
        Left is job category in old BKR model. Kirkland is still using these land use categories.
        Right is job category used by BKRCast.
        Use Kirkland's job estimate as control total for each BKRTMTAZ, scale the PSRC jobs in each parcel such that total scaled jobs matches Kirkland's estimate
        in BKRTMTAZ level.
        '''
        kirk_control_jobs_by_BKRTMTAZ_df = pd.read_csv(os.path.join(working_folder_lu, jobs_by_old_BKRTMTAZ_file))
        kirk_parcels_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == 'KIRKLAND']
        kirk_parcels_df = updated_parcel_df.reset_index().merge(kirk_parcels_df[['PSRC_ID', 'BKRTMTAZ']], left_on = 'PARCELID', right_on = 'PSRC_ID')
        temp_col_list = columns_list.copy()
        temp_col_list.append('EMPTOT_P')
        temp_col_list.append('BKRTMTAZ')
        psrc_kirk_jobs = kirk_parcels_df['EMPTOT_P'].sum()
        kirk_psrc_jobs_by_BKRTMTAZ_df = kirk_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
        kirk_control_jobs_by_BKRTMTAZ_df = kirk_control_jobs_by_BKRTMTAZ_df.merge(kirk_psrc_jobs_by_BKRTMTAZ_df.reset_index(), on = 'BKRTMTAZ', how = 'left')
        kirk_control_jobs_by_BKRTMTAZ_df.loc[kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] != 0, 'scale'] = kirk_control_jobs_by_BKRTMTAZ_df['KirklandControlTotal'] / kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P']
        kirk_control_jobs_by_BKRTMTAZ_df.loc[kirk_control_jobs_by_BKRTMTAZ_df['EMPTOT_P'] == 0, 'scale'] = 1
        kirk_control_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder_lu, 'Kirkland_job_scale_comparison.csv'), index = True)

        kirk_parcels_df = kirk_parcels_df.merge(kirk_control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'scale']], on = 'BKRTMTAZ', how = 'left')
        kirk_parcels_df['EMPTOT_P'] = 0
        for col in columns_list:
            kirk_parcels_df[col] = kirk_parcels_df[col] * kirk_parcels_df['scale']
            kirk_parcels_df[col] = kirk_parcels_df[col].round(0).astype(int)
            kirk_parcels_df['EMPTOT_P'] += kirk_parcels_df[col]    

        kirk_parcels_df.to_csv(os.path.join(working_folder_lu, 'adjusted_jobs_by_parcel_in_Kirkland.csv'), index = False)
        kirk_jobs_by_BKRTMTAZ_df = kirk_parcels_df[temp_col_list].groupby('BKRTMTAZ').sum()
        kirk_jobs_by_BKRTMTAZ_df = kirk_jobs_by_BKRTMTAZ_df.merge(kirk_control_jobs_by_BKRTMTAZ_df[['BKRTMTAZ', 'KirklandControlTotal']], on = 'BKRTMTAZ')
        kirk_jobs_by_BKRTMTAZ_df.to_csv(os.path.join(working_folder_lu, 'Kirkland_job_comparison_by_BKRTMTAZ.csv'), index = True)

        updated_parcel_df = updated_parcel_df.loc[~updated_parcel_df.index.isin(kirk_parcels_df['PARCELID'])]
        kirk_parcels_df.drop(columns = ['PSRC_ID', 'BKRTMTAZ', 'scale'], inplace = True)
        updated_parcel_df = pd.concat([updated_parcel_df.reset_index(), kirk_parcels_df])
        updated_parcel_df = updated_parcel_df.sort_values(by = ['PARCELID'], ascending = True)

        # update total jobs in each parcel
        updated_parcel_df['EMPTOT_P'] = 0
        for col in columns_list:
            updated_parcel_df['EMPTOT_P'] = updated_parcel_df['EMPTOT_P'] + updated_parcel_df[col]
            
        new_kirk_jobs = updated_parcel_df.loc[updated_parcel_df.index.isin(kirk_parcels_df['PARCELID']), 'EMPTOT_P'].sum()

        logging.info('Kirkland jobs before change: ' + str(psrc_kirk_jobs))
        logging.info('              after change: ' + str(new_kirk_jobs))
        logging.info('Kirkland jobs gained ' + str(new_kirk_jobs - psrc_kirk_jobs))

        logging.info('total jobs before change: ' + str(self.original_parcel_data_df['EMPTOT_P'].sum()))
        logging.info('total jobs after change: ' + str(updated_parcel_df['EMPTOT_P'].sum()))
        logging.info('')

        logging.info('Exporting parcel files...')
        updated_parcel_df.to_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
        utility.backupScripts(__file__, os.path.join(working_folder_lu, os.path.basename(__file__)))


        logging.info('Done')


    def step_5_sync_pop2parcels(self):
        """
        Step 5: Sync population to parcels.
            This program is used to pass number of households by parcel from synthetic population to parcel file. After the program,
            the households in parcel file is consistent with synthetic population file.
        """

        # 3/9/2022
        # upgraded to python 3.7

        # 05/01/2025
        # move the paths into config.py
        
        logging.info('Loading hh_and_persons.h5...')
        hdf_file = h5py.File(hh_and_person_file, "r")
        hh_df = utility.h5_to_df(hdf_file, 'Household')

        logging.info('Updating number of households...')
        hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
        parcel_df = pd.read_csv(os.path.join(parcel_folder, input_parcel_file), sep = ' ')
        parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

        parcel_df['HH_P']  = 0
        parcel_df['HH_P'] = parcel_df['hhexpfac']
        parcel_df.fillna(0, inplace = True)
        parcel_df.drop(['hhexpfac', 'hhsize', 'hhparcel'], axis = 1, inplace = True)
        parcel_df['HH_P'] = parcel_df['HH_P'].round(0).astype(int)


        logging.info('Exporting future parcel file...')
        parcel_df.to_csv(os.path.join(parcel_folder, output_parcel_file), sep = ' ', index = False)

        utility.backupScripts(__file__, os.path.join(parcel_folder, os.path.basename(__file__)))

        logging.info('Step 5 done.')
        logging.info('Land use process is complete. Please check the output numbers.\n')