import os
import sys

import math
import logging
import numpy as np
import pandas as pd
import h5py

import utility
from config import *

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

log_fname = os.path.join(working_folder_synpop, f"log_synpop_{modeller_initial}_{version}_step{step}_{timestamp}.log")
logging.basicConfig(filename=log_fname, level=logging.INFO, format="%(asctime)s: %(levelname)s - %(message)s")
# also log info to console
console_handler = logging.StreamHandler()
logger = logging.getLogger()
logger.addHandler(console_handler)


class SynPop:
    def __init__(self, run_step, backup_folder='backup'):
        logging.info(f'Creating SynPop object...I/O {version} by modeller: {modeller_initial}')
        logging.info('Loading synthetic populations...')
        self.run_step = run_step
        self.backup_folder = backup_folder
        logging.info(f'Running step {self.run_step}...\n')
    
        # step A
        self.future_hdf_file = None
        self.base_hdf_file = None

        self.future_hh_df = None
        self.base_hh_df = None

        self.parcel_df = None
        self.ofm_df = None

        # step B
        self.lookup_df = None
        self.hhs_by_parcel_df = None
        self.cob_du_df = None

        # step C
        self.hhs_df = None

        print('Logging file created: ', log_fname)

    def step_A_interpolate_hhps(self):
        """
        Step A: interpolate hhs and persons by GEOID between two horizon years.
        This tool is to create an interpolated number of households and persons by blockgroup between two horizon years. It takes synthetic population in h5 format
        as input files, and parcel lookup table and ofm_estimate_template_file as well. 

        The output file is an input to generate_COB_local_hhs_estimate.py.

        03/03/2022
        now it also exports total_hhs_by_parcel and total_persons_by_parcel to file.   Both households and persons have decimal points. 
        The output file is an input to Prepare_Hhs_for_future_using_KR_oldTAZ_COB_parcel_forecast.py

        05/01/2025
        Move the paths to config.py
        """
        # load inputs
        self.future_hdf_file = h5py.File(future_year_synpop_file, "r")
        self.base_hdf_file = h5py.File(base_year_synpop_file, "r")

        self.future_hh_df = utility.h5_to_df(self.future_hdf_file, 'Household')
        self.base_hh_df = utility.h5_to_df(self.base_hdf_file, 'Household')

        self.future_hh_df['future_total_persons'] = self.future_hh_df['hhexpfac'] * self.future_hh_df['hhsize']
        self.future_hh_df['future_total_hhs'] = self.future_hh_df['hhexpfac']

        self.base_hh_df['base_total_persons'] = self.base_hh_df['hhexpfac'] * self.base_hh_df['hhsize']
        self.base_hh_df['base_total_hhs'] = self.base_hh_df['hhexpfac']

        self.parcel_df = pd.read_csv(parcel_filename, low_memory=False)
        self.future_hh_df = self.future_hh_df.merge(self.parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        self.future_hhs_by_geoid10 = self.future_hh_df.groupby('GEOID10')[['future_total_hhs', 'future_total_persons']].sum()
        self.base_hh_df = self.base_hh_df.merge(self.parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        self.base_hhs_by_geoid10 = self.base_hh_df.groupby('GEOID10')[['base_total_hhs', 'base_total_persons']].sum()

        logging.info(f"Future total hhs: {self.future_hh_df['future_total_hhs'].sum():,.0f}")
        logging.info(f"Future total persons: {self.future_hh_df['future_total_persons'].sum():,.0f}\n")
        logging.info(f"Base total hhs: {self.base_hh_df['base_total_hhs'].sum():,.0f}")
        logging.info(f"Base total persons: {self.base_hh_df['base_total_persons'].sum():,.0f}\n")

        self.ofm_df = pd.read_csv(ofm_estimate_template_file)
        self.ofm_df = self.ofm_df.merge(self.future_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
        self.ofm_df = self.ofm_df.merge(self.base_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)

        # start processing
        if target_year <= future_year and target_year >= base_year:
            # right between the bookends.
            logging.info('Interpolating...')
        else:
            logging.info('Extropolating...')
        
            
        self.ofm_df.fillna(0, inplace = True)
        ratio = (target_year - base_year) * 1.0 / (future_year - base_year)
        self.ofm_df['OFM_groupquarters'] = 0
        self.ofm_df['OFM_hhs'] = ((self.ofm_df['future_total_hhs'] - self.ofm_df['base_total_hhs']) * ratio + self.ofm_df['base_total_hhs']).round(0)
        self.ofm_df['OFM_persons'] = ((self.ofm_df['future_total_persons'] - self.ofm_df['base_total_persons']) * ratio + self.ofm_df['base_total_persons']).round(0)

        logging.info(f"Estimated total hhs: {self.ofm_df['OFM_hhs'].sum():,.0f}")
        logging.info(f"Estimated total persons: {self.ofm_df['OFM_persons'].sum():,.0f}")
        self.ofm_df[['GEOID10', 'OFM_groupquarters', 'OFM_hhs', 'OFM_persons']].to_csv(interploated_ofm_estimate_by_GEOID, index = False)

        # summarize total households and persons by parcel id
        base_hhs_by_parcel = self.base_hh_df[['PSRC_ID', 'base_total_hhs', 'base_total_persons']].groupby('PSRC_ID').sum()
        future_hhs_by_parcel  = self.future_hh_df[['PSRC_ID', 'future_total_hhs', 'future_total_persons']].groupby('PSRC_ID').sum()
        target_hhs_by_parcel = pd.merge(base_hhs_by_parcel, future_hhs_by_parcel, on = 'PSRC_ID', how = 'outer')
        target_hhs_by_parcel.fillna(0, inplace = True)
        # interpolate linearly hhs and persons by parcel id: hhs_target_per_parcel = hhs_base_per_parcel + delta_hhs_btw_base_and_future * ratio
        target_hhs_by_parcel['total_hhs_by_parcel'] = target_hhs_by_parcel['base_total_hhs'] + (target_hhs_by_parcel['future_total_hhs'] - target_hhs_by_parcel['base_total_hhs']) * ratio
        target_hhs_by_parcel['total_persons_by_parcel'] = target_hhs_by_parcel['base_total_persons'] + (target_hhs_by_parcel['future_total_persons'] - target_hhs_by_parcel['base_total_persons']) * ratio
        # organize columns of the target table
        target_hhs_by_parcel.drop(['base_total_hhs', 'base_total_persons', 'future_total_hhs', 'future_total_persons'], axis = 1, inplace = True)
        target_hhs_by_parcel.reset_index(inplace = True)
        # merge the target table into the parcel ids and BKRCAST taz ids
        target_hhs_by_parcel = self.parcel_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ', 'GEOID10']].merge(target_hhs_by_parcel[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], on = 'PSRC_ID', how = 'left')
        target_hhs_by_parcel.fillna(0, inplace= True)
        target_hhs_by_parcel.to_csv(hhs_by_parcel_filename, index = False)
        logging.info(f'Estimated households by parcel exported: {hhs_by_parcel_filename}')

        # calcuate the average number of persons per household by jurisdiction
        avg_person_per_hhs_df = target_hhs_by_parcel[['Jurisdiction', 'total_hhs_by_parcel', 'total_persons_by_parcel']].groupby('Jurisdiction').sum()
        avg_person_per_hhs_df['avg_persons_per_hh'] = avg_person_per_hhs_df['total_persons_by_parcel'] / avg_person_per_hhs_df['total_hhs_by_parcel']
        logging.info('%s' % avg_person_per_hhs_df)

        logging.info('\nGenerating households by TAZ... ')
        target_hhs_by_taz = target_hhs_by_parcel[['BKRCastTAZ', 'total_hhs_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
        target_hhs_by_taz = target_hhs_by_taz.loc[target_hhs_by_taz['total_hhs_by_parcel'] > 0]
        self.future_hh_df.drop(['PSRC_ID', 'future_total_persons', 'future_total_hhs', 'GEOID10', 'BKRCastTAZ'], axis = 1, inplace=True)
        target_hhs_df = pd.DataFrame()

        for taz in target_hhs_by_taz['BKRCastTAZ'].tolist():
            hhs_in_taz = self.future_hh_df.loc[self.future_hh_df['hhtaz'] == taz]
            num_hhs_popsim = hhs_in_taz['hhexpfac'].sum()
            num_hhs = int(target_hhs_by_taz.loc[target_hhs_by_taz['BKRCastTAZ'] == taz, 'total_hhs_by_parcel'].tolist()[0])
            if num_hhs_popsim > num_hhs:
                # sample num_hhs households
                target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs)])
            else:
                # sample num_hhs_popsim households
                target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs_popsim)])
        logging.info(f"Total households: {target_hhs_df['hhexpfac'].sum():,.0f}")
        logging.info(f"estimated_hhs_from_ofm - total_hhs_by_taz = {(self.ofm_df['OFM_hhs'].sum() - target_hhs_df['hhexpfac'].sum()):,.0f}")

        logging.info('\nGenerating persons...')
        future_persons_df = utility.h5_to_df(self.future_hdf_file, 'Person')
        target_persons_df = future_persons_df.loc[future_persons_df['hhno'].isin(target_hhs_df['hhno'])]
        logging.info(f"Total persons: {target_persons_df['psexpfac'].sum():,.0f}")
        logging.info(f"estimated_persons_from_ofm - total_persons_by_taz = {(self.ofm_df['OFM_persons'].sum() - target_persons_df['psexpfac'].sum()):,.0f}")
        logging.info('Exporting the estimated households and persons by TAZ into hdf5 format...')
        output_h5_file = h5py.File(final_output_pop_file, 'w')
        utility.df_to_h5(target_hhs_df, output_h5_file, 'Household')
        utility.df_to_h5(target_persons_df, output_h5_file, 'Person')
        logging.info(f'Estimated household and persons by TAZ file exported: {output_h5_file}')
        output_h5_file.close()

        logging.info('\nBacking up the scripts for step A...')
        os.makedirs(os.path.join(working_folder_synpop, self.backup_folder, version), exist_ok=True)
        utility.backupScripts(__file__, os.path.join(working_folder_synpop, self.backup_folder, version, os.path.basename(__file__)))
        logging.info(f'Scripts for step A backup exported: {os.path.join(working_folder_synpop, self.backup_folder, version, os.path.basename(__file__))}')

        logging.info('\nExecuting step A...done. Households and persons estimation by GEOID is completed.\n')

    def step_B_distribute_hh2parcel(self, debug=False):
        """
        Step B: prepare households for base or future year using KR oldTAZ COB parcel forecast.
        This program will decide how many households each parcel should have.
        It takes COB dwelling units forecast (cob_du_file), and Kirkland/Redmond's household forecast by trip model TAZ (hhs_control_total_by_TAZ) as local estimate
        to replace parcel data in (hhs_by_parcel) in relevant jurisdictions. 
        It will round number of household and persons from decimals to whole integer while keeping hhs and person intact by BKRCastTAZ level. 
        If no local estimate from Kirkland/Redmond is provided, set hhs_control_total_by_TAZ = ''.

        This program can be used in producing base year or future year household inputs for populatitonsim and parcelizationV2.py.

        # in ACS 2016 there is no hhs in Census block group 530619900020, but in PSRC's future hhs forecast there are. 
        # We need to relocate these households from parcels in this blockgroup to parcels in block group 530610521042 
        # while staying in the same BKRCastTAZ. 

        Number of hhs per parcel in whole number is exported to an external file. 
        This file is used as guidance to allocate synthetic popualtion to parcel using parcelizationV2.py.
        A control file for populationsim is generated as well. 
        """

        self.lookup_df = pd.read_csv(lookup_file, low_memory = False)
        self.hhs_by_parcel_df = pd.read_csv(hhs_by_parcel)
        self.cob_du_df = pd.read_csv(os.path.join(working_folder_lu, cob_du_file))

        # make a deep copy of hhs_by_parcel_df for number adjusting
        adjusted_hhs_by_parcel_df = self.hhs_by_parcel_df.copy(deep=True)
        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.rename(columns = {'total_hhs_by_parcel': 'adj_hhs_by_parcel', 'total_persons_by_parcel':'adj_persons_by_parcel'})

        if hhs_control_total_by_TAZ != '':
            logging.info(f'A household control file by TAZ is provided: {hhs_control_total_by_TAZ}')
            hhs_control_total_by_TAZ_df = pd.read_csv(os.path.join(working_folder_synpop, hhs_control_total_by_TAZ))
            juris_list = hhs_control_total_by_TAZ_df['Jurisdiction'].unique()
            logging.info(f'The following jurisdictions are included: {juris_list}')    

            hhs_control_total_by_TAZ_df['total_persons'] = 0
            hhs_control_total_by_TAZ_df['total_hhs'] = 0
            # calculate households and persons by the pre-defined occupancy rates
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Kirkland', 'sfhhs'] = hhs_control_total_by_TAZ_df['SFU'] * sf_occupancy_rate_Kirkland
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Kirkland', 'mfhhs'] = hhs_control_total_by_TAZ_df['MFU'] * mf_occupancy_rate_Kirkland
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Kirkland', 'total_hhs'] = hhs_control_total_by_TAZ_df['sfhhs'] + hhs_control_total_by_TAZ_df['mfhhs']
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Kirkland', 'total_persons'] = hhs_control_total_by_TAZ_df['sfhhs'] * avg_persons_per_sfhh_Kirkland + hhs_control_total_by_TAZ_df['mfhhs'] * avg_persons_per_mfhh_Kirkland
        
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Redmond', 'sfhhs'] = hhs_control_total_by_TAZ_df['SFU'] * sf_occupancy_rate_Redmond
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Redmond', 'mfhhs'] = hhs_control_total_by_TAZ_df['MFU'] * mf_occupancy_rate_Redmond
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Redmond', 'total_hhs'] = hhs_control_total_by_TAZ_df['sfhhs'] + hhs_control_total_by_TAZ_df['mfhhs']
            hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['Jurisdiction'] == 'Redmond', 'total_persons'] = hhs_control_total_by_TAZ_df['sfhhs'] * avg_persons_per_sfhh_Redmond + hhs_control_total_by_TAZ_df['mfhhs'] * avg_persons_per_mfhh_Redmond

            # get parcels within trip model Redmond and Kirkland TAZ (old taz system)
            parcels_in_trip_model_TAZ_df = pd.merge(self.hhs_by_parcel_df[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], self.lookup_df.loc[self.lookup_df['BKRTMTAZ'].notna(), ['PSRC_ID', 'Jurisdiction', 'BKRTMTAZ']], on = 'PSRC_ID', how = 'inner')
            parcels_in_trip_model_TAZ_df = parcels_in_trip_model_TAZ_df.merge(hhs_control_total_by_TAZ_df[['BKRTMTAZ']], on  = 'BKRTMTAZ', how = 'inner')

            hhs_by_TAZ_df = parcels_in_trip_model_TAZ_df[['BKRTMTAZ', 'total_hhs_by_parcel', 'total_persons_by_parcel']].groupby('BKRTMTAZ').sum()
            hhs_by_TAZ_df = pd.merge(hhs_by_TAZ_df, hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['total_hhs'] >= 0, ['BKRTMTAZ', 'total_hhs', 'total_persons']], on = 'BKRTMTAZ', how = 'outer')
            hhs_by_TAZ_df.fillna(value = {'total_hhs' : 0, 'total_persons' : 0}, inplace = True)
            hhs_by_TAZ_df.to_csv(os.path.join(working_folder_synpop, hhs_by_taz_comparison_file), index = False)

            adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(parcels_in_trip_model_TAZ_df[['PSRC_ID', 'BKRTMTAZ']], on = 'PSRC_ID', how = 'left')

            for city in juris_list:
                # reset hhs and persons to zero in Kirkland and Redmond parcels that are not included in local estimates. We will use their local forecast.
                adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['Jurisdiction'] == city.upper()) & adjusted_hhs_by_parcel_df['BKRTMTAZ'].isna(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

            # for a TAZ that have no hhs in PSRC erstimate but have hhs in local jurisdiction estimate, evenly distribute hhs to all parcels in that TAZ
            tazs_for_evenly_distri_df = hhs_by_TAZ_df.loc[hhs_by_TAZ_df['total_hhs_by_parcel'] == 0]
            logging.info('Evenly distribute hhs on parcels in the following trip model TAZs: ')
            for row in tazs_for_evenly_distri_df.itertuples():
                logging.info(row.BKRTMTAZ, row.total_hhs_by_parcel, row.total_hhs)
                # find parcels within this taz
                counts = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ].shape[0]
                if counts == 0 and row.total_hhs > 0:
                    logging.info(f'TAZ {row.BKRTMTAZ} is has no parcels but has {row.total_hhs} households.')
                    continue
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ, 'adj_hhs_by_parcel'] = row.total_hhs / counts
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRTMTAZ'] == row.BKRTMTAZ, 'adj_persons_by_parcel'] = row.total_persons / counts

            # for other parcels, scale up hhs to match local jurisdiction's forecast by applying factors calculated in TAZ level
            tazs_for_proportional_distri_df = hhs_by_TAZ_df.loc[hhs_by_TAZ_df['total_hhs_by_parcel'] > 0].copy()
            tazs_for_proportional_distri_df['ratio_hhs'] = tazs_for_proportional_distri_df['total_hhs'] / tazs_for_proportional_distri_df['total_hhs_by_parcel']
            tazs_for_proportional_distri_df['ratio_persons'] = tazs_for_proportional_distri_df['total_persons'] / tazs_for_proportional_distri_df['total_persons_by_parcel']

            adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(tazs_for_proportional_distri_df[['BKRTMTAZ', 'ratio_hhs', 'ratio_persons']], on = 'BKRTMTAZ', how = 'left')
            adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.fillna(value = {'ratio_hhs' : 1, 'ratio_persons' : 1})
            adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_hhs']
            adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_persons']
            adjusted_hhs_by_parcel_df.drop(columns = ['ratio_hhs', 'ratio_persons'], inplace = True)
        else:
            logging.info('No household estimate is provided by Redmond and Kirkland. ')

        # replace hhs estimate with COB's forecast
        # if some parcels are missing from the cob_du_df, export them for further investigation.
        cob_total_parcels_df = self.hhs_by_parcel_df.loc[self.hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE']
        cob_parcels_provided = self.cob_du_df.shape[0]
        if cob_total_parcels_df.shape[0] != cob_parcels_provided:
            missing_parcels = set()
            missing_fname = ''
            if cob_total_parcels_df.shape[0] > cob_parcels_provided:
                logging.info('cob_total_parcels_df output from SynPop step A has a larger number of parcels')
                cob_missing_parcels_df = cob_total_parcels_df.loc[~cob_total_parcels_df['PSRC_ID'].isin(self.cob_du_df['PSRC_ID'])]
                missing_fname = f'cob_missing_parcels_{modeller_initial}_{version}'
                cob_missing_parcels_df.to_csv(os.path.join(working_folder_synpop, f'{missing_fname}.csv'), index = False)
                logging.warning(f'{cob_missing_parcels_df.shape[0]} parcels are missing in {os.path.join(working_folder_lu, cob_du_file)}.')
                missing_parcels = list(set(cob_missing_parcels_df['PSRC_ID']))
            elif cob_total_parcels_df.shape[0] < cob_parcels_provided:
                logging.info('cob_parcels_provided output from LandUse step 1 has a larger number of parcels')
                lookup_missing_parcels_df = self.cob_du_df.loc[~self.cob_du_df['PSRC_ID'].isin(cob_total_parcels_df['PSRC_ID'])]
                missing_fname = f'lookup_missing_parcels_{modeller_initial}_{version}'
                lookup_missing_parcels_df.to_csv(os.path.join(working_folder_synpop, f'{missing_fname}.csv'), index = False)
                logging.warning(f'{lookup_missing_parcels_df.shape[0]} parcels are missing in {os.path.join(working_folder_lu, hhs_by_parcel)}.')
                missing_parcels = list(set(cob_missing_parcels_df['PSRC_ID']))
            with open(f'{os.path.join(working_folder_lu, missing_fname)}.exp', 'w') as f:
                missing = ''
                for miss in missing_parcels:
                    missing += f',{miss}'
                missing = missing.lstrip(',')
                f.write(f'PSRC_ID IN ({missing})')
            logging.info('The missing parcel ids into a csv file and a exp file exported for GIS investigation.\n')
            logging.warning(f'Missing parcels are exported in {os.path.join(working_folder_synpop, f"{missing_fname}.csv and .exp")}.')
            logging.warning('Please cehck the missing parcel files for further investigation.')

        self.cob_du_df['sfhhs'] = self.cob_du_df['SFUnits'] * sf_occupancy_rate 
        self.cob_du_df['mfhhs'] = self.cob_du_df['MFUnits'] * mf_occupancy_rate
        self.cob_du_df['sfpersons'] = self.cob_du_df['sfhhs'] * avg_persons_per_sfhh
        self.cob_du_df['mfpersons'] = self.cob_du_df['mfhhs'] * avg_persons_per_mfhh
        self.cob_du_df['cobflag'] = 'cob'

        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(self.cob_du_df[['PSRC_ID', 'cobflag', 'sfhhs', 'mfhhs', 'sfpersons', 'mfpersons']], on = 'PSRC_ID', how = 'left')
        # reset hhs and persons in all COB parcels to zero. Only use local forecast.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE', ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        # it is important to use cobflag rather than Jurisdiction, because (hhs and persons in) parcels flagged by cobflag are provided by COB staff.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['sfhhs'] + adjusted_hhs_by_parcel_df['mfhhs']
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['sfpersons'] + adjusted_hhs_by_parcel_df['mfpersons']

        ### Control Rounding
        ### hhs should not be fractions, so round the hhs to integer, controlled by BKRCastTAZ
        ### we will use the rounded hhs by parcel as guidance to allocate synthetic households. So controlled rounding is very important here, otherwise we will have more or less 
        ### total households due to rounding error, and we cannot allocate a fraction of a household.
        ### we rely on this rounded hhs by parcel to generate control file (in census block group level) for populationsim
        ### to get correct number of persons by block group, instead of doing controlled rounding, we simply summarize persons by block group before controlled rounding on hhs.
        adj_persons_by_GEOID10 = adjusted_hhs_by_parcel_df[['GEOID10', 'adj_persons_by_parcel']].groupby('GEOID10').sum()
        total_hhs_before_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()
        logging.info(f'Total number of households before rounding: {total_hhs_before_rounding:,.2f}\n')
        logging.info(f"Total number of households in COB before rounding: {adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_hhs_by_parcel'].sum():,.2f}\n")
        # debug for crosscheck the hh numbers and the population
        if debug:
            cob_before = adjusted_hhs_by_parcel_df[adjusted_hhs_by_parcel_df['cobflag']=='cob'].copy(deep=True)
            cob_before_ = cob_before[['BKRCastTAZ', 'adj_hhs_by_parcel']].groupby(by='BKRCastTAZ').sum().reset_index()

        logging.info('Rounding households to integer. Controlled by BKRCastTAZ subtotal....')
        # check if this 2016 file is 'acecon0403.csv'
        if popsim_control_file == 'acecon0403.csv':
            # in ACS 2016 there is no hhs in Census block group 530619900020, but in PSRC's future hhs forecast there are. 
            # We need to relocate these households from parcels in this blockgroup to  
            # parcels in block group 530610521042 while staying in the same BKRCastTAZ. 
            special_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530619900020) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 0)
            special_hhs_by_TAZ = adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['BKRCastTAZ', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
            # move all persons in 530619900020 to 530610521042
            adj_persons_by_GEOID10.loc[530610521042, 'adj_persons_by_parcel'] += adj_persons_by_GEOID10.loc[530619900020, 'adj_persons_by_parcel']

            # move hhs from parcels in 530619900020 to parcels in 530610521042 && same TAZ
            for row in special_hhs_by_TAZ.itertuples():
                mf_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530610521042) & (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == row.BKRCastTAZ) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 1)
                mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcels_flag].shape[0]
                if mf_parcels_count > row.adj_hhs_by_parcel:
                    selected_ids = adjusted_hhs_by_parcel_df.sample(n = int(row.adj_hhs_by_parcel))['PSRC_ID']
                    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
                else:
                    increase = math.floor(row.adj_hhs_by_parcel / mf_parcels_count)
                    adjusted_hhs_by_parcel_df.loc[mf_parcels_flag, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                    diff = row.adj_hhs_by_parcel - increase * mf_parcels_count
                    selected_ids = adjusted_hhs_by_parcel_df.sample(n = 1)['PSRC_ID']
                    adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + diff
                adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        adj_hhs_by_BKRCastTAZ = adjusted_hhs_by_parcel_df[['BKRCastTAZ', 'adj_hhs_by_parcel']].groupby('BKRCastTAZ').sum().round(0).astype(int)
        controlled_taz_hhs = adj_hhs_by_BKRCastTAZ.reset_index().to_dict('records')

        for record in controlled_taz_hhs:
            condition = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'])
            adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'].round(0)
            subtotal = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'], 'adj_hhs_by_parcel'].sum()
            diff = subtotal - record['adj_hhs_by_parcel']
            mf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] >= 2)
            sf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] == 1)
            mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].shape[0]
            sf_parcels_count = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].shape[0]
            if diff > 0: 
                # too many hhs in this TAZ after rounding. 
                # need to bring down subtotal start from mf parcels. 
                if mf_parcels_count > 0:
                    if mf_parcels_count < diff:
                        adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] - 1
                        diff = diff - mf_parcels_count
                    else: # number of mf parcels are more than diff, randomly pick diff number of mf parcels and reduce adj_hhs_by_parcel in each parcel by 1
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                        condition = (adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids))
                        adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] - 1
                        diff = 0
                # if rounding issue is not resolved yet, deal with it in sf parcel
                if (diff > 0) and (sf_parcels_count > 0):
                    if sf_parcels_count < diff: 
                        adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] - 1
                        diff = diff - sf_parcels_count
                    else: # number of sf parcels are more than diff, randomly pick diff number of sf parcels and reduce adj_hhs_by_parcel in each by 1 (set to zero)
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                        condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                        adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] - 1
                        diff = 0
                # last option, if rounding issue is still not resolved, 
                if diff > 0:
                    logging.info(f"TAZ {record['BKRCastTAZ']}: rounding issue is not resolved. Difference is {diff}")
            elif diff < 0:
                # too less hhs in this TAZ after rounding. need to increase subtotal
                if mf_parcels_count > 0:
                    # evenly distribute diff to all mf parcel, then the remaining to a ramdomly selected one
                    if mf_parcels_count < abs(diff):
                        increase = math.floor(abs(diff) / mf_parcels_count)
                        adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] + increase
                        diff = diff + increase * mf_parcels_count
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = 1)['PSRC_ID']
                        condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                        adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] + abs(diff)
                        diff = diff + abs(diff)
                    else:
                        # randomly select parcel to increase the number of households
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                        condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                        adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] + 1
                        diff = diff + abs(diff)
                        
                else: # if no mf parcel is available, add diff to sf parcels
                    if sf_parcels_count > 0:
                        if sf_parcels_count < abs(diff):
                            increase = math.floor(abs(diff) / sf_parcels_count)
                            adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] + increase
                            diff = diff + increase * sf_parcels_count
                            selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = 1)['PSRC_ID']
                            condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                            adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] + abs(diff)
                            diff = diff + abs(diff)
                        else:
                            selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                            condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                            adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] + 1
                            diff = diff + abs(diff)
                    else:  # last option, add diff to a ramdomly selected parcel
                        applicable_parcels_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'])
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[applicable_parcels_flags].sample(n = 1)['PSRC_ID']
                        condition = adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids)
                        adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df.loc[condition, 'adj_hhs_by_parcel'] + abs(diff)

        adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].astype(int)
        total_hhs_after_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()
        logging.info('Controlled rounding is complete. ')
        logging.info(f'\nTotal hhs before rounding: {total_hhs_before_rounding:,.2f}, after: {total_hhs_after_rounding:,.0f}\n')
        logging.info(f"Total number of households in COB after rounding: {adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob', 'adj_hhs_by_parcel'].sum():,.2f}\n")
        
        if debug:
            cob_tazs = list(adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['cobflag'] == 'cob']['BKRCastTAZ'])
            cob_after = adjusted_hhs_by_parcel_df[adjusted_hhs_by_parcel_df['BKRCastTAZ'].isin(cob_tazs)].copy(deep=True)
            cob_after = cob_after[['BKRCastTAZ', 'adj_hhs_by_parcel']].groupby(by='BKRCastTAZ').sum().reset_index()
            cob_after['controlled_hhs'] = 0
            for _, record in cob_before_.iterrows():
                if record['BKRCastTAZ'] in cob_tazs:
                    cob_after.loc[cob_after['BKRCastTAZ']==record['BKRCastTAZ'], 'controlled_hhs'] = record['adj_hhs_by_parcel']
            cob_after['cross_check'] = cob_after['adj_hhs_by_parcel'] - cob_after['controlled_hhs']
            logging.debug(f"COB before control rounding: {cob_before['adj_hhs_by_parcel'].sum()}")
            logging.debug(f"COB after control rounding: {cob_after['adj_hhs_by_parcel'].sum()}")
            logging.debug(f"total control rounding difference (after - before): {cob_after['cross_check'].sum()}")
            logging.debug(f"cross check table exported in: {os.path.join(working_folder_synpop, 'COB_population_before_after.csv')}")

        if  hhs_control_total_by_TAZ != '':
            # export adjusted hhs by parcel to file
            adjusted_hhs_by_parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ', 'BKRTMTAZ', 'adj_hhs_by_parcel']].rename(columns = {'adj_hhs_by_parcel':'total_hhs'}).to_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file), index = False)
        else:
            # export adjusted hhs by parcel to file
            adjusted_hhs_by_parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ', 'adj_hhs_by_parcel']].rename(columns = {'adj_hhs_by_parcel':'total_hhs'}).to_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file), index = False)
        logging.info(f'Rounded number of households by parcel file exported: {os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file)}')
                
        sum_hhs_by_jurisdiction = adjusted_hhs_by_parcel_df[['Jurisdiction', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby('Jurisdiction').sum()
        sum_hhs_by_jurisdiction.to_csv(os.path.join(working_folder_synpop,  summary_by_jurisdiction_filename))
        logging.info(f'Rounded number of households by jurisdiction file exported: {os.path.join(working_folder_synpop, summary_by_jurisdiction_filename)}\n')

        ### Create control file for PopulationSim
        popsim_control_df = pd.read_csv(os.path.join(working_folder_synpop, popsim_control_file), sep = ',')
        hhs_by_geoid10_df =  adjusted_hhs_by_parcel_df[['GEOID10', 'adj_hhs_by_parcel']].groupby('GEOID10').sum()
        hhs_by_geoid10_df = hhs_by_geoid10_df.merge(adj_persons_by_GEOID10, left_index = True, right_index = True, how = 'left')
        hhs_by_geoid10_df.fillna(0, inplace = True)
        popsim_control_df = popsim_control_df.merge(hhs_by_geoid10_df, left_on = 'block_group_id', right_on = 'GEOID10', how = 'left')
        error_blkgrps_df = popsim_control_df.loc[popsim_control_df.isna().any(axis = 1)]
        if error_blkgrps_df.shape[0] > 0:
            logging.warning(f'Some blockgroups are missing values.')
            logging.warning(f"Please check {os.path.join(working_folder_synpop, f'error_census_blockgroup_{modeller_initial}_{version}.csv')} for more details.")
            logging.warning('The missing values are all replaced with zeros.\n')
            error_blkgrps_df.to_csv(os.path.join(working_folder_synpop, f'error_census_blockgroup_{modeller_initial}_{version}.csv'), index = False)

        popsim_control_df.fillna(0, inplace = True)
        popsim_control_df['hh_bg_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['hh_tract_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_bg_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_tract_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df.drop(hhs_by_geoid10_df.columns, axis = 1, inplace = True)
        popsim_control_df.to_csv(os.path.join(working_folder_synpop, popsim_control_output_file), index = False)
        logging.info(f'PopulationSim control file exported: {os.path.join(working_folder_synpop, popsim_control_output_file)}.')

        total_hhs = popsim_control_df['hh_bg_weight'].sum()
        total_persons = popsim_control_df['pers_bg_weight'].sum()
        logging.info(f'{total_hhs:,.0f} households, {total_persons:,.0f} persons are exported in the control file.\n')

        ### generate other support files for parcelization
        bel_parcels_hhs_df = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == 'BELLEVUE', ['PSRC_ID', 'adj_hhs_by_parcel', 'sfhhs', 'mfhhs', 'adj_persons_by_parcel', 'Jurisdiction', 'GEOID10']]
        bel_parcels_hhs_df.rename(columns = {'adj_hhs_by_parcel':'total_hhs', 'adj_persons_by_parcel':'total_persons'}, inplace = True)
        bel_parcels_hhs_df.to_csv(os.path.join(working_folder_synpop, parcels_for_allocation_filename), index = False)

        logging.info('\nBacking up the scripts for step B...')
        os.makedirs(os.path.join(working_folder_synpop, self.backup_folder, version), exist_ok=True)
        utility.backupScripts(__file__, os.path.join(working_folder_synpop, self.backup_folder, version, os.path.basename(__file__)))
        logging.info(f'Scripts for step B backup exported: {os.path.join(working_folder_synpop, self.backup_folder, version, os.path.basename(__file__))}')

        logging.info('Executing step B...done. Preparing households for base or future year using KR oldTAZ COB parcel forecast is completed.\n')

    def step_C_parcelization(self):
        """
        Step C: parcelizationV3.py
        This program takes synthetic households and synthetic persons (from PopulationSim) as inputs,
        and allocates them to parcels, under the guidance of adjusted_hhs_by_parcel_file which is an output file from 
        Prepare_Hhs_for_future_using_KR_oldTAZ_COB_parcel_forecast.py or prepare_hhs_for_baseyear_using_ofm.py. 
        It also reformats household and person data columns to match BKRCast input requirement. 
        The output, h5_file_name, can be directly loaded into BKRCast.

        Number of households per parcel in synthetic population should be consistent with the parcel file. It can be done by calling 
        sync_population_parcel.py. 
        """
        # 3/9/2022
        # upgrade to python 3.7

        # 05/01/2025
        # move I/O paths to config file

        self.hhs_df = pd.read_csv(os.path.join(working_folder_synpop, synthetic_households_file_name))

        self.hhs_df['hhparcel'] = 0
        hhs_by_GEOID10_synpop = self.hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()

        adjusted_hhs_by_parcel_df = pd.read_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file))
        # remove any blockgroup ID is Nan.
        all_blcgrp_ids = self.hhs_df['block_group_id'].unique()
        mask = np.isnan(all_blcgrp_ids)
        all_blcgrp_ids = sorted(all_blcgrp_ids[~mask])

        if popsim_control_file == 'acecon0403.csv':
            # special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
            # we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
            adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['GEOID10'] == 530619900020) & (adjusted_hhs_by_parcel_df['total_hhs'] > 0), 'GEOID10'] = 530610521042

        hhs_by_blkgrp_adjusted = adjusted_hhs_by_parcel_df.groupby('GEOID10')[['total_hhs']].sum()
        final_hhs_df = pd.DataFrame()

        logging.info(f"Allocating households to parcels at each census block group...\nIt may take a while...\n")
        for blcgrpid in all_blcgrp_ids:
            assert hhs_by_GEOID10_synpop.loc[blcgrpid, 'hhexpfac'] == hhs_by_blkgrp_adjusted.loc[blcgrpid, 'total_hhs'], \
                   '# households in the synthetic population should equal # households in the adjusted parcel file. You need to fix this issue before moving forward.'
            num_parcels = 0
            num_hhs = 0
            parcels_in_adjusted_df = adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['GEOID10'] == blcgrpid) & 
                                                                  (adjusted_hhs_by_parcel_df['total_hhs'] > 0)]
            subtotal_parcels = parcels_in_adjusted_df.shape[0]
            total_control_hhs = parcels_in_adjusted_df['total_hhs'].sum()
            j_start_index = 0
            selected_hhs_synpop_df = self.hhs_df.loc[(self.hhs_df['block_group_id'] == blcgrpid) & (self.hhs_df['hhparcel'] == 0)].copy()
            numhhs_synpop = selected_hhs_synpop_df['hhexpfac'].sum()
            index_hhparcel = selected_hhs_synpop_df.columns.get_loc('hhparcel')
            for i in range(subtotal_parcels):
                numHhs = parcels_in_adjusted_df['total_hhs'].iat[i]
                parcelid = parcels_in_adjusted_df['PSRC_ID'].iat[i]
                for j in range(int(numHhs)):
                    if num_hhs < numhhs_synpop:
                        selected_hhs_synpop_df.iat[j + j_start_index, index_hhparcel] = parcelid 
                        num_hhs += 1          
                num_parcels += 1
                j_start_index += int(numHhs)

            ## take care the remaining unallocated hhs, very unlikely we'll have remaining unallocated hhs
            # numhhs_synpop is the number of households waiting for being assigned a parcel id;
            # total_control_hhs is the total households where each household has a parcel id
            unallocated_num = numhhs_synpop - total_control_hhs  # number of households that have not been assigned a parcel id yet
            if unallocated_num > 0:
                for j in range(int(unallocated_num)):
                    if (j + j_start_index) < selected_hhs_synpop_df.shape[0]:
                        # randomly pick the corresponding parcel ids to those households
                        parcels_for_allocation_this_cbg = adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['GEOID10'] == blcgrpid) & 
                                                                                        (adjusted_hhs_by_parcel_df['total_hhs'] > 0)]
                        assert len(parcels_for_allocation_this_cbg) >= unallocated_num, \
                            "Not enough households in the allocation file for allocating synthetic households.\n \
                            Is your synthetic household file is generated through PopulationSim based on the adjusted households from the previous step?"
                        random_picked_pids = parcels_for_allocation_this_cbg.sample(n = unallocated_num)['PSRC_ID'].to_numpy()
                        selected_hhs_synpop_df.iat[j + j_start_index, index_hhparcel] = random_picked_pids[j] 

            final_hhs_df = pd.concat([final_hhs_df, selected_hhs_synpop_df])

            logging.debug(f"Control: {total_control_hhs}, {hhs_by_GEOID10_synpop.loc[blcgrpid, 'hhexpfac']} (actual {num_hhs}) hhs allocated to GEOID10 {blcgrpid}, {num_parcels} parcels are processed")

        final_hhs_df = final_hhs_df.merge(adjusted_hhs_by_parcel_df[['PSRC_ID', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        final_hhs_df.rename(columns = {'BKRCastTAZ': 'hhtaz'}, inplace = True)
        final_hhs_df.drop(columns = ['PSRC_ID'], axis = 1, inplace = True)

        ### process other attributes to match required columns
        logging.info('Processing other column attributes...')
        pop_df = pd.read_csv(os.path.join(working_folder_synpop, synthetic_population_file_name)) 
        pop_df.rename(columns={'household_id':'hhno', 'SEX':'pgend'}, inplace = True)
        pop_df.sort_values(by = 'hhno', inplace = True)

        # -1 pdairy ppaidprk pspcl,pstaz ptpass,puwarrp,puwdepp,puwmode,pwpcl,pwtaz 
        # pstyp is covered by pptyp and pwtyp, misssing: puwmode -1 puwdepp -1 puwarrp -1 pwpcl -1 pwtaz -1 ptpass -1  pspcl,pstaz 
        # 1 psexpfac 
        morecols = pd.DataFrame({'pdairy': [-1]*pop_df.shape[0],'pno': [-1]*pop_df.shape[0],'ppaidprk': [-1]*pop_df.shape[0],
                                 'psexpfac': [1]*pop_df.shape[0],'pspcl': [-1]*pop_df.shape[0], 'pstaz': [-1]*pop_df.shape[0],
                                 'pptyp': [-1]*pop_df.shape[0],'ptpass': [-1]*pop_df.shape[0],'puwarrp': [-1]*pop_df.shape[0], 
                                 'puwdepp': [-1]*pop_df.shape[0],'puwmode': [-1]*pop_df.shape[0],'pwpcl': [-1]*pop_df.shape[0], 
                                 'pwtaz': [-1]*pop_df.shape[0]})
        pop_df = pop_df.join(morecols) #1493219

        ### here assign household size in household size and person numbers in person file
        hhsize_df = pop_df.groupby('hhno')[['psexpfac']].sum().reset_index()
        final_hhs_df.rename(columns = {'household_id':'hhno'}, inplace = True)
        final_hhs_df = final_hhs_df.merge(hhsize_df, how = 'inner', left_on = 'hhno', right_on = 'hhno')
        final_hhs_df['hhsize'] = final_hhs_df['psexpfac']
        final_hhs_df.drop(['psexpfac'], axis = 1, inplace = True)

        #=========================================
        # in pwtype (person worker type), 1 and 2 represent fullworkers; 3 to 6 represent part-time workers; -1 represents non-worker.
        fullworkers = [1, 2]
        partworkers = [3.0, 4.0, 5.0, 6.0]
        noworker = [-1]
        # in pstype (person student type), 
        # -1, 1 and 2 represent non-student; 3 to 16 represent full-time student; -1 represents non-worker.
        pstype = pop_df.pstyp.fillna(-1)
        pop_df.pstyp = pstype
        fullstudents = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]
        nostudents = [-1, 0, 1.0, 2.0]
        # pstype=15 or 16: university student (pptyp=5)
        # pstype=13 or 14: grade school student/child age 16+ (pptyp=6)
        # pstype=2 to 12: child age 5-15 (pptyp=7)
        # pstype=1: child age 0-4 (pptyp=8)
        pp5 = [15, 16]
        pp6 = [13.0, 14.0]
        pp7 = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]
        pp8 = [1]

        # pptyp: Person type (1=full time worker, 2=part time worker, 3=non-worker age 65+, 4=other non-working adult, 
        # 5=university student, 6=grade school student/child age 16+, 7=child age 5-15, 8=child age 0-4); 
        # this could be made optional and computed within DaySim for synthetic populations based on ACS PUMS; 
        # for other survey data, the coding and rules may be more variable and better done outside DaySim
        logging.info('Processing attributes...')
        # assign pno within each household
        pop_df['pno'] = pop_df.groupby('hhno').cumcount() + 1
        #####
        # Assign pwtyp category. Categories referred to https://github.com/psrc/soundcast/wiki/Inputs
        #####
        pop_df['WKW'].fillna(-1)
        # full-time worker
        pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pwtyp'] = 1
        pop_df.loc[pop_df['WKW'].isin(fullworkers), 'pptyp'] = 1
        # part-time worker
        pop_df.loc[pop_df['WKW'].isin(partworkers), 'pwtyp'] = 2
        pop_df.loc[pop_df['WKW'].isin(partworkers), 'pptyp'] = 2

        #####
        # Assign pstyp category. Categories referred to https://github.com/psrc/soundcast/wiki/Inputs
        #####
        pop_df.rename(columns={'pstyp': 'pstyp_'}, inplace=True)
        pop_df['pstyp_'] = pop_df['pstyp_'].fillna(-1)
        pop_df['pstyp'] = -1        
        # full-time student
        pop_df.loc[pop_df['pstyp_'].isin(fullstudents), 'pstyp'] = 1
        # part-time student
        pop_df.loc[(pop_df['WKW'].isin(partworkers)) &
                   (pop_df['pstyp_'].isin(fullstudents)), 'pstyp'] = 2
        # non-student
        pop_df.loc[pop_df['pstyp_'].isin(nostudents), 'pstyp'] = 0

        #####
        # Assign pptyp category. Categories referred to https://github.com/psrc/soundcast/wiki/Inputs
        #####
        # university student
        pop_df.loc[pop_df['pstyp_'].isin(pp5), 'pptyp'] = 5
        # grade school student/child age 16+
        pop_df.loc[pop_df['pstyp_'].isin(pp6), 'pptyp'] = 6
        # child age 5-15
        pop_df.loc[pop_df['pstyp_'].isin(pp7), 'pptyp'] = 7
        # child age 0-4
        pop_df.loc[pop_df['pstyp_'].isin(pp8), 'pptyp'] = 8
        # non-worker and over 65 years old
        pop_df.loc[(pop_df['WKW'].isin(noworker)) &
                   (pop_df['pagey']>=65), 'pptyp'] = 3
        # non-worker and over 15 years old
        pop_df.loc[(pop_df['WKW'].isin(noworker)) &
                   (pop_df['pagey']>15), 'pptyp'] = 4
        # non-worker and age between 5-15
        pop_df.loc[(pop_df['WKW'].isin(noworker)) &
                   (5<=pop_df['pagey']) * (pop_df['pagey']<=15), 'pptyp'] = 7
        # non-worker and age between 0-5
        pop_df.loc[(pop_df['WKW'].isin(noworker)) &
                   (0<=pop_df['pagey']) * (pop_df['pagey']<5), 'pptyp'] = 8

        pop_df.drop(['block_group_id', 'hh_id', 'PUMA', 'WKW'], axis = 1, inplace = True)

        morecols = pd.DataFrame({'hownrent': [-1]*final_hhs_df.shape[0]})
        final_hhs_df.drop(['hownrent'], axis = 1, inplace = True)
        final_hhs_df = final_hhs_df.join(morecols) 

        pop_df = pop_df.loc[pop_df['hhno'].isin(final_hhs_df['hhno'])]
        output_h5_file = h5py.File(os.path.join(working_folder_synpop, h5_file_name), 'w')
        utility.df_to_h5(final_hhs_df, output_h5_file, 'Household')
        utility.df_to_h5(pop_df, output_h5_file, 'Person')
        output_h5_file.close()
        logging.info(f'Parcelized household and person file is exported in hdf5 format: {os.path.join(working_folder_synpop, h5_file_name)}')

        pop_df.to_csv(os.path.join(working_folder_synpop, updated_persons_file_name), sep = ',')
        logging.info(f'Parcelized person file is also exported in csv format: {os.path.join(working_folder_synpop, updated_persons_file_name)}')
        
        final_hhs_df.to_csv(os.path.join(working_folder_synpop, updated_hhs_file_name), sep = ',')
        logging.info(f'Parcelized household file is also exported in csv format: {os.path.join(working_folder_synpop, h5_file_name)}')

        logging.info('Backing up the scripts for step C...')
        os.makedirs(os.path.join(working_folder_lu, self.backup_folder, version), exist_ok=True)
        utility.backupScripts(__file__, os.path.join(working_folder_synpop, self.backup_folder, version, os.path.basename(__file__)))
        logging.info(f'Scripts for step C backup exported: {os.path.join(working_folder_lu, self.backup_folder, version, os.path.basename(__file__))}')

        logging.info(f'Total census block groups: {len(all_blcgrp_ids):,.0f}')
        logging.info(f'Final number of households: {final_hhs_df.shape[0]:,.0f}')
        logging.info(f'Final number of persons: {pop_df.shape[0]:,.0f}')
        logging.info(f'Executing step C...done. Allocating the synthetic households and persons to control parcels is completed.\n')
