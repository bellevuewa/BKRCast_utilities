import os
import h5py
import pandas as pd

import utility
from config import *

#####
# Check the outputs from step 5 is okay
#####
hdf_file = h5py.File(os.path.join(working_folder_synpop, h5_file_name), "r")
hh_df = utility.h5_to_df(hdf_file, 'Household')
parcel_taz_lookup = pd.read_csv(lookup_file)

# before rounding the number of households
hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
parcel_df = pd.read_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

# after rounding the number of households
parcel_df_after = pd.read_csv(os.path.join(working_folder_lu, output_parcel_file), sep = ' ')

print(f"before rounding the number of households at each parcel: {parcel_df['hhexpfac'].sum()}")
print(f"after rounding the number of households at each parcel: {parcel_df_after['HH_P'].sum()}")

# the updated number of houshols in each city should match with what output from step B
city_to_check = ['Bellevue', 'Kirkland']
# step B updated parcel hhs
adjusted_hhs_stepB = pd.read_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file))
for city in city_to_check:
    parcel_taz_city = parcel_taz_lookup[parcel_taz_lookup['Jurisdiction']==city.upper()]
    adjusted_hhs_city = adjusted_hhs_stepB[adjusted_hhs_stepB['PSRC_ID'].isin(parcel_taz_city['PSRC_ID'])]
    print(f"Adjusted hhs from step B in {city}: {adjusted_hhs_city['total_hhs'].sum()}")
    parcelized_hhs_city = hh_df[hh_df['hhparcel'].isin(parcel_taz_city['PSRC_ID'])]
    print(f"Parcelized hhs from step C in {city}: {len(parcelized_hhs_city)}")
    parcel_df_after_city = parcel_df_after[parcel_df_after['PARCELID'].isin(parcel_taz_city['PSRC_ID'])]
    print(f"Synchronized hhs from step 5 in {city}: {parcel_df_after_city['HH_P'].sum()}")
    # assertions
    assert adjusted_hhs_city['total_hhs'].sum() == len(parcelized_hhs_city), \
            "Go back to check parcelization (step C), it is not correct!"
    assert adjusted_hhs_city['total_hhs'].sum() == parcel_df_after_city['HH_P'].sum(), \
            "Synchronizng synthetic population file to parcel file (step 5) is not correct!"
    print('-----')

# summarize parcel urbansim file to TAZ, subarea and city level.
# the scripts below is revised from BKRCast_Tools-Python3/LandUse/parcel_file_summary.py
output_field = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

taz_subarea = pd.read_csv(os.path.join(working_folder_lu, subarea_file), sep = ',')

# parcel_df_after = parcel_df_after.merge(taz_subarea, how = 'left',  left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
parcel_df_after = parcel_df_after.merge(parcel_taz_lookup, how = 'left',  left_on = 'PARCELID', right_on = 'PSRC_ID')

summary_by_jurisdiction = parcel_df_after.groupby('Jurisdiction')[output_field].sum()
print("Exporting... \"summary_by_jurisdiction.csv\"")
summary_by_jurisdiction.to_csv(os.path.join(working_folder_lu, "summary_by_jurisdiction.csv"))
summary_by_taz = parcel_df_after.groupby('TAZ_P')[output_field].sum()
print("Exporting... \"summary_by_TAZ.csv\"")
summary_by_taz.to_csv(os.path.join(working_folder_lu, "summary_by_TAZ.csv")) 
print("Exporting... \"summary_by_subarea.csv\"")
parcel_df_after = parcel_df_after.merge(taz_subarea[['BKRCastTAZ', 'Subarea', 'SubareaName']], how = 'left',  left_on = 'TAZ_P', right_on = 'BKRCastTAZ')
summary_by_subarea = parcel_df_after[parcel_df_after['Subarea'] > 0].groupby('Subarea')[output_field].sum()
taz_subarea = parcel_df_after[['Subarea', 'SubareaName']].drop_duplicates()
taz_subarea.set_index('Subarea', inplace = True)
summary_by_subarea = summary_by_subarea.join(taz_subarea['SubareaName'])
summary_by_subarea.to_csv(os.path.join(working_folder_lu, "summary_by_subarea.csv"))

print('Done')