import os
import pandas as pd
from config import *

#####
# Check the outputs from step 4 is okay
#####
updated_parcel_df = pd.read_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
job_columns = [i for i in columns_list if i != 'EMPTOT_P']

updated_parcel_df['EMPTOT_P_manual'] = updated_parcel_df[job_columns].sum(axis=1)

# check if the sum of jobs is correct
assert sum(updated_parcel_df['EMPTOT_P_manual'] == updated_parcel_df['EMPTOT_P']) == len(updated_parcel_df), \
    "The sum of jobs is not correct!"

print('Done')