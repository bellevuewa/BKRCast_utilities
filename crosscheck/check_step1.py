import os
import pandas as pd
from config import *

#####
# Check the outputs from step 1 is okay
#####
kc_job = pd.read_csv(os.path.join(working_folder_lu, kc_job_file))
# check the number of total jobs matches with that before summing up
assert kc_job['EMPTOT_P'].sum() == kc_job[job_cat_list].sum().sum(), \
        "The sum of the number of jobs is not correct!"

updated_sqft_kc = pd.read_csv(os.path.join(working_folder_lu, kc_SQFT_file))
# check the number of total squared footage matches with that before summing up
assert updated_sqft_kc['SQFT_TOT'].sum() == updated_sqft_kc[sqft_cat_list].sum().sum(), \
        "The sum of the squared footage is not correct!"

lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
kc_df = pd.read_csv(os.path.join(working_folder_lu, kingcsqft), sep = ',', low_memory = False)
kc_df = kc_df.merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
kc_df = kc_df[kc_df['Jurisdiction'].isin(subset_area)]
du_kc_output = pd.read_csv(os.path.join(working_folder_lu, kc_du_file))
# check after merging BKRCast TAZs and jurisdiction, the data remains the same
assert kc_df['TOTAL_UNITS_MF'].sum() == du_kc_output['MFUnits'].sum() and kc_df['TOTAL_UNITS_SF'].sum() == du_kc_output['SFUnits'].sum(), \
        "Step 1 process is not correct!"

print('Done')