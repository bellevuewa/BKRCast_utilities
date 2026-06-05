import os
import pandas as pd
from config import *

#####
# Check the outputs from step 4 is okay
#####

lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'

updated_parcel_df = pd.read_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
job_columns = [i for i in columns_list if i != 'EMPTOT_P']

updated_parcel_df['EMPTOT_P_manual'] = updated_parcel_df[job_columns].sum(axis=1)

parcel_taz_lookup = pd.read_csv(lookup_file)
for city in subset_area:
    parcel_taz_city = parcel_taz_lookup[parcel_taz_lookup['Jurisdiction']==city.upper()]
    updated_parcel_df_city = updated_parcel_df[updated_parcel_df['PARCELID'].isin(parcel_taz_city['PSRC_ID'])]    
    print(f"Adjusted jobs in {city}: {updated_parcel_df_city['EMPTOT_P'].sum()}")
    print(f"Manual calculated jobs from step B in {city}: {updated_parcel_df_city['EMPTOT_P_manual'].sum()}")
    # check if the number of jobs by jurisdiction is correct
    assert updated_parcel_df_city['EMPTOT_P'].sum() - updated_parcel_df_city['EMPTOT_P_manual'].sum() < 10, \
            "Calculation of jobs is not correct!"
    print('-----')

# check if the sum of jobs is correct
assert sum(updated_parcel_df['EMPTOT_P_manual'] == updated_parcel_df['EMPTOT_P']) == len(updated_parcel_df), \
    "The sum of jobs is not correct!"

print('Done')