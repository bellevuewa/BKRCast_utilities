import pandas as pd
import h5py
import utility
from config import *

#####
# Check the outputs from step A is okay
#####
future_hdf_file = h5py.File(future_year_synpop_file, "r")
base_hdf_file = h5py.File(base_year_synpop_file, "r")

future_hh_df = utility.h5_to_df(future_hdf_file, 'Household')
base_hh_df = utility.h5_to_df(base_hdf_file, 'Household')

future_hh_df['future_total_persons'] = future_hh_df['hhexpfac'] * future_hh_df['hhsize']
future_hh_df['future_total_hhs'] = future_hh_df['hhexpfac']

base_hh_df['base_total_persons'] = base_hh_df['hhexpfac'] * base_hh_df['hhsize']
base_hh_df['base_total_hhs'] = base_hh_df['hhexpfac']

parcel_df = pd.read_csv(parcel_filename, low_memory=False)
future_hh_df = future_hh_df.merge(parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
future_hhs_by_geoid10 = future_hh_df.groupby('GEOID10')[['future_total_hhs', 'future_total_persons']].sum()
base_hh_df = base_hh_df.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
base_hhs_by_geoid10 = base_hh_df.groupby('GEOID10')[['base_total_hhs', 'base_total_persons']].sum()

print(f"Base total persons: {base_hh_df['base_total_persons'].sum()}")
print(f"Future total persons: {future_hh_df['future_total_persons'].sum()}")
print('-----')
print(f"Base total households: {base_hh_df['base_total_hhs'].sum()}")
print(f"Future total households: {future_hh_df['future_total_hhs'].sum()}")

# calculate interpolation by parcel
target_year = 2024
target_hhs_by_parcel_output = pd.read_csv(hhs_by_parcel_filename)
future_hhs_by_parcel = future_hh_df.groupby(by='PSRC_ID')[['future_total_hhs', 'future_total_persons']].sum().reset_index()
base_hhs_by_parcel = base_hh_df.groupby(by='PSRC_ID')[['base_total_hhs', 'base_total_persons']].sum().reset_index()
target_hhs_by_parcel = pd.merge(base_hhs_by_parcel, future_hhs_by_parcel, on = 'PSRC_ID', how = 'outer')
target_hhs_by_parcel.fillna(0, inplace = True)
# interpolate linearly hhs and persons by parcel id: hhs_target_per_parcel = hhs_base_per_parcel + delta_hhs_btw_base_and_future * ratio
target_hhs_by_parcel['total_hhs_by_parcel_manual'] = target_hhs_by_parcel['base_total_hhs'] + \
                            (target_hhs_by_parcel['future_total_hhs'] - target_hhs_by_parcel['base_total_hhs']) * (target_year-2014) / (2050-2014)
target_hhs_by_parcel['total_persons_by_parcel_manual'] = target_hhs_by_parcel['base_total_persons'] + \
                            (target_hhs_by_parcel['future_total_persons'] - target_hhs_by_parcel['base_total_persons']) * (target_year-2014) / (2050-2014)
target_hhs_by_parcel = target_hhs_by_parcel.merge(target_hhs_by_parcel_output[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], on='PSRC_ID', how='left')
# cross-check number of households
assert max(abs(target_hhs_by_parcel['total_hhs_by_parcel'] - target_hhs_by_parcel['total_hhs_by_parcel_manual'])) <= 1e-12, \
        "Household interpolation is not correct!!"
assert max(abs(target_hhs_by_parcel['total_persons_by_parcel'] - target_hhs_by_parcel['total_persons_by_parcel_manual'])) <= 1e-12, \
        "Person interpolation is not correct!!"

# calculate ofm interpolation
ofm_df = pd.read_csv(ofm_estimate_template_file)
ofm_df = ofm_df.merge(future_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
ofm_df = ofm_df.merge(base_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
ofm_df.rename(columns={'OFM_persons': 'OFM_persons_manual', 'OFM_hhs': 'OFM_hhs_manual'}, inplace=True)
ofm_df['OFM_hhs_manual'] = (ofm_df['base_total_hhs'] + \
                            (ofm_df['future_total_hhs'] - ofm_df['base_total_hhs']) * (target_year-2014) / (2050-2014)).round(0)
ofm_df['OFM_persons_manual'] = (ofm_df['base_total_persons'] + \
                                (ofm_df['future_total_persons'] - ofm_df['base_total_persons']) * (target_year-2014) / (2050-2014)).round(0)
ofm_df_output = pd.read_csv(interploated_ofm_estimate_by_GEOID)
ofm_df = ofm_df.merge(ofm_df_output[['GEOID10', 'OFM_hhs', 'OFM_persons']], on='GEOID10', how='left')

# cross-check number of households
assert (abs(ofm_df['OFM_hhs'] - ofm_df['OFM_hhs_manual'])).max() <= 1e-12, \
        "OFM household interpolation is not correct!!"
assert (abs(ofm_df['OFM_persons'] - ofm_df['OFM_persons_manual'])).max() <= 1e-12, \
        "OFM person interpolation is not correct!!"

print('Done')