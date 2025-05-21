import os
import pandas as pd
from config import *

#####
# Check the outputs from step 3 is okay
#####
parcel_data_output = pd.read_csv(os.path.join(working_folder_lu, original_parcel_file_name), sep = ' ', low_memory = False)
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
kc_job = pd.read_csv(os.path.join(working_folder_lu, kc_job_file))

parcel_earlier_df = pd.read_csv(parcel_file_name_ealier, sep = ' ')
parcel_earlier_df.columns = [i.upper() for i in parcel_earlier_df.columns]
parcel_latter_df = pd.read_csv(parcel_file_name_latter, sep = ' ')
parcel_latter_df.columns = [i.upper() for i in parcel_latter_df.columns]

target_year = 2024
for city in ['BELLEVUE', 'KIRKLAND', 'REDMOND']:
    city_records = lookup_df[lookup_df['Jurisdiction']==city]
    
    city_parcel_earlier = parcel_earlier_df[parcel_earlier_df['PARCELID'].isin(city_records['PSRC_ID'])]
    print(f"2014 job total in {city}: {city_parcel_earlier['EMPTOT_P'].sum()}")
    
    city_parcel_output = parcel_data_output[parcel_data_output['PARCELID'].isin(city_records['PSRC_ID'])]
    print(f"Interpolated job total in {city}: {city_parcel_output['EMPTOT_P'].sum()}")
    print(f"COB job file's job total in {city}: {kc_job[kc_job['Jurisdiction']==city]['EMPTOT_P'].sum()}")
    
    city_parcel_latter = parcel_latter_df[parcel_latter_df['PARCELID'].isin(city_records['PSRC_ID'])]
    # manually calculate the job total
    city_parcel_manual = pd.merge(city_parcel_earlier[['EMPTOT_P', 'PARCELID']], city_parcel_latter[['EMPTOT_P', 'PARCELID']], 
                                  on='PARCELID', how='outer', suffixes=('_earlier', '_latter'))
    city_parcel_manual['EMPTOT_P_manual'] = city_parcel_manual['EMPTOT_P_earlier'] + \
        (city_parcel_manual['EMPTOT_P_latter'] - city_parcel_manual['EMPTOT_P_earlier']) * (target_year-2014) / (2050-2014)
    print(f"Manually interpolated job total in {city}: {city_parcel_manual['EMPTOT_P_manual'].sum()}")
    city_parcel_manual = city_parcel_manual.merge(city_parcel_output[['EMPTOT_P', 'PARCELID']], on='PARCELID', how='left')
    city_parcel_manual.rename(columns={'EMPTOT_P': 'EMPTOT_P_output'}, inplace=True)
    city_parcel_manual.merge(lookup_df[['PSRC_ID', 'Jurisdiction']], left_on='PARCELID', right_on='PSRC_ID', how='left')
    city_parcel_manual.to_csv(os.path.join(working_folder_lu, f"{city}_jobs_cross_check.csv"))
    
    city_parcel_latter = parcel_latter_df[parcel_latter_df['PARCELID'].isin(city_records['PSRC_ID'])]
    print(f"2050 job total in {city}: {city_parcel_latter['EMPTOT_P'].sum()}")

    print('-----')

print('Done')