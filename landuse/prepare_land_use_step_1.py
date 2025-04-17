import os, sys
sys.path.append(os.getcwd())
import pandas as pd
import utility

# 2/23/2021
# this script is used to join parcel data from Community Development to BKRCastTAZ and subarea
# # via PSRC_ID, and save jobs and sqft data to different data files. IF the parcels provided in the kingsqft file are not valid parcels in
# lookup_file, these invalid parcels will be exported to error_parcel_file for further investigation. 
# Sometimes BKRCastTAZ and subarea column in the data from CD are a little mismatched
# # so it is always good to remap parcel data to lookup file to ensure we always
# # summarize land use  on the same base data.

# 2/28/2022
# upgrade to Python 3.7

### input files
working_folder = r'Z:\Modeling Group\BKRCast\LandUse\2024baseyear'
kingcsqft = 'Base By PSRCID (12-31-2024).csv'
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"
###

### Output fiels
kc_job_file = '2024_BKR_Jobs_new_method.csv'
kc_SQFT_file = '2024_BKR_Sqft.csv'
error_parcel_file = 'parcels_not_in_2014_PSRC_parcels.csv'
kc_du_file = '2024_KC_housingunits.csv'
cob_du_file = '2024_COB_housingunits.csv'
###

##
##  subset_area can only be these values: 
# 'Rest of KC','External','BELLEVUE', 'KIRKLAND','REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe'
# if it is empty, means all parcels in kingcsqft file   
##
subset_area = ['BELLEVUE', 'KIRKLAND','REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe'] 
# subset_area = ['BELLEVUE']
SQFT_data_available = True
#subset_area = [] 

job_rename_dict = {'JOBS_EDU':'EMPEDU_P', 'JOBS_FOOD':'EMPFOO_P', 'JOBS_GOV':'EMPGOV_P', 'JOBS_IND':'EMPIND_P',
    'JOBS_MED':'EMPMED_P', 'JOBS_OFF':'EMPOFC_P', 'JOBS_RET':'EMPRET_P', 'JOBS_RSV':'EMPRSC_P', 'JOBS_SERV':'EMPSVC_P', 'JOBS_OTH':'EMPOTH_P',
    'JOBS_TOTAL':'EMPTOT_P'}
sqft_rename_dict = {'SQFT_EDU':'SQFT_EDU', 'SQFT_FOOD':'SQFT_FOO','SQFT_GOV':'SQFT_GOV','SQFT_IND':'SQFT_IND','SQFT_MED':'SQFT_MED', 'SQFT_OFF':'SQFT_OFC',
    'SQFT_RET':'SQFT_RET', 'SQFT_RSV':'SQFT_RSV', 'SQFT_SERV':'SQFT_SVC', 'SQFT_OTH': 'SQFT_OTH', 'SQFT_NONE':'SQFT_NON', 
    'SQFT_TOTAL':'SQFT_TOT'}
du_rename_dict = {'TOTAL_UNITS_SF':'SFUnits', 'TOTAL_UNITS_MF':'MFUnits'}

jobs_columns_List = ['PSRC_ID', 'EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P', 'EMPTOT_P']
sqft_columns_list = ['PSRC_ID', 'SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH', 'SQFT_TOT']
dwellingunits_list = ['PSRC_ID', 'SFUnits', 'MFUnits']

job_cat_list = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']
sqft_cat_list = ['SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH']

print('Loading....')
lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
kc_df = pd.read_csv(os.path.join(working_folder,kingcsqft), sep = ',', low_memory = False)
subarea_df = pd.read_csv(subarea_file, sep = ',')

## rename columns to fit modeling input format
kc_df.rename(columns = job_rename_dict, inplace = True)
kc_df.rename(columns = sqft_rename_dict, inplace = True)
kc_df.rename(columns = du_rename_dict, inplace = True)

print('Exporting job file...')
updated_jobs_kc = kc_df[jobs_columns_List].merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
updated_jobs_kc = updated_jobs_kc.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
if subset_area != []:
    updated_jobs_kc = updated_jobs_kc[updated_jobs_kc['Jurisdiction'].isin(subset_area)]
updated_jobs_kc['EMPTOT_P'] = updated_jobs_kc[job_cat_list].sum(axis = 1)    
updated_jobs_kc.to_csv(os.path.join(working_folder, kc_job_file), sep = ',', index = False)

if SQFT_data_available: 
    print('Exporting sqft file...')
    updated_sqft_kc = kc_df[sqft_columns_list].merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'left')
    updated_sqft_kc = updated_sqft_kc.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
    if subset_area != []:
        updated_sqft_kc = updated_sqft_kc[updated_sqft_kc['Jurisdiction'].isin(subset_area)]
    updated_sqft_kc['SQFT_TOT'] = updated_sqft_kc[sqft_cat_list].sum(axis = 1)       
    updated_sqft_kc.to_csv(os.path.join(working_folder, kc_SQFT_file), sep = ',', index = False)

print('Exporting King County dwelling units...')
du_kc = kc_df[dwellingunits_list].merge(lookup_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ']], left_on = 'PSRC_ID', right_on = 'PSRC_ID', how = 'inner')
du_kc = du_kc.merge(subarea_df[['BKRCastTAZ', 'Subarea', 'SubareaName']], left_on = 'BKRCastTAZ', right_on = 'BKRCastTAZ', how = 'left')
if subset_area != []:
    du_kc = du_kc[du_kc['Jurisdiction'].isin(subset_area)]
du_kc.to_csv(os.path.join(working_folder, kc_du_file), sep  = ',', index = False)

du_cob = du_kc[du_kc['Jurisdiction'] == 'BELLEVUE']
du_cob.to_csv(os.path.join(working_folder, cob_du_file), sep = ',', index = False)

print('Exporting error file...')
error_parcels = kc_df[~kc_df['PSRC_ID'].isin(lookup_df['PSRC_ID'])]
error_parcels.to_csv(os.path.join(working_folder, error_parcel_file), sep = ',', index = False)
if error_parcels.shape[0] > 0:
    print('Please check the error file first.')

utility.backupScripts(__file__, os.path.join(working_folder, os.path.basename(__file__)))
print('Done')



