import copy

from datetime import datetime

"""
Land use configuration and the synthetic population configuration
"""

#####
# Configuration for all
#####

modeller_initial = 'oa'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
version = 'v0.0'
step = 5  # step = 1, 2, 3, 4, 5, 'A', 'B', or 'C'. See main.py for more information

# parcel vs TAZ lookup file
lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
# year range and target year
base_year = 2014
target_year = 2024
future_year =2050

#####
# Land use configurations
#####
working_folder_lu = r'Z:\Modeling Group\BKRCast\LandUse\2024baseyear'
#=====
# Step 1: Prepare land use data
#=====

## input paths for step 1
kingcsqft = 'Base By PSRCID (12-31-2024).csv'
subarea_file = r"I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv"

## output paths for step 1
kc_job_file = f'2024_COB_Jobs_new_method_{modeller_initial}_{version}.csv'
kc_SQFT_file = f'2024_COB_Sqft_{modeller_initial}_{version}.csv'
error_parcel_file = f'parcels_not_in_2014_PSRC_parcels_{modeller_initial}_{version}.csv'
kc_du_file = f'2024_KC_housingunits_{modeller_initial}_{version}.csv'
cob_du_file = f'2024_COB_housingunits_{modeller_initial}_{version}.csv'

## other configurations for step 1
"""
subset_area can only be these values: 
'Rest of KC','External','BELLEVUE', 'KIRKLAND','REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe'
if it is empty, means all parcels in kingcsqft file   
"""
subset_area = ['BELLEVUE', 'KIRKLAND','REDMOND', 'BellevueFringe', 'KirklandFringe', 'RedmondFringe'] 
# subset_area = ['BELLEVUE']
SQFT_data_available = True
#subset_area = []
job_rename_dict = {'JOBS_EDU':'EMPEDU_P', 'JOBS_FOOD':'EMPFOO_P', 'JOBS_GOV':'EMPGOV_P', 'JOBS_IND':'EMPIND_P',
                   'JOBS_MED':'EMPMED_P', 'JOBS_OFF':'EMPOFC_P', 'JOBS_RET':'EMPRET_P', 'JOBS_RSV':'EMPRSC_P', 
                   'JOBS_SERV':'EMPSVC_P', 'JOBS_OTH':'EMPOTH_P', 'JOBS_TOTAL':'EMPTOT_P'}

sqft_rename_dict = {'SQFT_EDU':'SQFT_EDU', 'SQFT_FOOD':'SQFT_FOO', 'SQFT_GOV':'SQFT_GOV', 'SQFT_IND':'SQFT_IND',
                    'SQFT_MED':'SQFT_MED', 'SQFT_OFF':'SQFT_OFC', 'SQFT_RET':'SQFT_RET', 'SQFT_RSV':'SQFT_RSV', 
                    'SQFT_SERV':'SQFT_SVC', 'SQFT_OTH': 'SQFT_OTH', 'SQFT_NONE':'SQFT_NON', 'SQFT_TOTAL':'SQFT_TOT'}

du_rename_dict = {'TOTAL_UNITS_SF':'SFUnits', 'TOTAL_UNITS_MF':'MFUnits'}

jobs_columns_List = ['PSRC_ID', 'EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 
                     'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P', 'EMPTOT_P']

sqft_columns_list = ['PSRC_ID', 'SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 
                     'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH', 'SQFT_TOT']

dwellingunits_list = ['PSRC_ID', 'SFUnits', 'MFUnits']

job_cat_list = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P', 'EMPOTH_P']
sqft_cat_list = ['SQFT_EDU', 'SQFT_FOO', 'SQFT_GOV', 'SQFT_IND', 'SQFT_MED', 'SQFT_OFC', 'SQFT_RET', 'SQFT_RSV', 'SQFT_SVC', 'SQFT_OTH']
##

#=====
# Step 2: Validate input parcels
#=====
## input paths for step 2
parcel_data_file_name = copy.copy(kc_job_file)  #r'2024_BKR_Jobs_new_method.csv'
###

## step 2 doesn't require output paths

## other configurations for step 2
year_parcel = 2014  # the year of the lookup parcel data (e.g., 2014): scroll up to find lookup_file
"""
# Use Jurisdiction to set which area to look into. 
# Jurisdiction can be a list of the subset of ['Rest of KC', 'External', 'BELLEVUE', 'BellevueFringe', 'KIRKLAND', 'REDMOND', 'RedmondFridge', 'KirklandFringe']
# Set Jurisdiction to None if you want to look into all parcels in the lookup_df.
"""
Jurisdiction = None
##



#####
# Synthetic population configurations
#####

working_folder_synpop = r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2024baseyear'
#r'I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2023baseyear_with_RedmondData'

#=====
# Step A: Interpolate household and person data from bookends
#=====
## input paths for step A
base_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2014_psrc_hh_and_persons.h5"
future_year_synpop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\PSRC\2050_PSRC_hh_and_persons_bkr.h5"
parcel_filename = copy.copy(lookup_file)
ofm_estimate_template_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\OFM_estimate_template.csv"

## output paths for step A
interploated_ofm_estimate_by_GEOID = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2024baseyear\2024_ofm_estimate_from_PSRC_2014_2050" + f'_{modeller_initial}_{version}' + ".csv"
hhs_by_parcel_filename = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2024baseyear\2024_hhs_by_parcels_from_PSRC_2014_2050" + f'_{modeller_initial}_{version}' + ".csv"
final_output_pop_file = r"I:\Modeling and Analysis Group\01_BKRCast\BKRPopSim\PopulationSim_BaseData\2024baseyear\2024_interpolated_synthetic_population_from_SC" + f'_{modeller_initial}_{version}' + ".h5"

## step A doesn't require other configurations


#=====
# Step B: Distribute households to parcels
#=====

## inputs paths for step B
hhs_by_parcel = copy.copy(hhs_by_parcel_filename) # output file from interpolate_hhs_and_persons_by_GEOID_btw_two_horizon_years.py
popsim_control_file = 'acecon0403.csv'
"""
TAZ level control total (households) from Kirkland and Redmond. (can be any TAZ)
if there is no local estimate from Redmond/Kirkland, set it to ''. 
"""
hhs_control_total_by_TAZ = ''

## outputs paths for step B
hhs_by_taz_comparison_file = "2024_PSRC_hhs_and_forecast_from_kik_Red_by_trip_model_TAZ_comparison" + f'_{modeller_initial}_{version}' + ".csv"
adjusted_hhs_by_parcel_file = "2024_final_hhs_by_parcel" + f'_{modeller_initial}_{version}' + ".csv"
popsim_control_output_file = "ACS2016_controls_2024_Complan" + f'_{modeller_initial}_{version}' + ".csv"
parcels_for_allocation_filename = "2024_Complan_parcels_for_allocation_local_estimate" + f'_{modeller_initial}_{version}' + ".csv"
summary_by_jurisdiction_filename = "2024_summary_by_jurisdiction" + f'_{modeller_initial}_{version}' + ".csv"

## other configurations for step B
#==
"""
Occupancy rate for single family and multi family households
"""
# avg_person_per_hh_Redmond = 2.3146
# avg_person_per_hh_Kirkland = 2.2576

avg_persons_per_sfhh_Kirkland =  2.82 
avg_persons_per_mfhh_Kirkland =  2.03

avg_persons_per_sfhh_Redmond =  2.82 
avg_persons_per_mfhh_Redmond =  2.03

sf_occupancy_rate_Kirkland = 0.952
mf_occupancy_rate_Kirkland = 0.895

sf_occupancy_rate_Redmond = 0.952
mf_occupancy_rate_Redmond = 0.895

sf_occupancy_rate = 0.952  # from Gwen
mf_occupancy_rate = 0.895  # from Gwen

avg_persons_per_sfhh =  2.82 # from Gwen
avg_persons_per_mfhh =  2.03 # from Gwen
#==


#=====
# Step C: Parcelize households and persons
#=====

## input paths for step C
synthetic_households_file_name = '2024_baseyear_synthetic_households.csv'
synthetic_population_file_name = '2024_baseyear_synthetic_persons.csv'
# number of hhs per parcel
# Note: parcels_for_allocation_filename should be the output from step B: adjusted_hhs_by_parcel_file = r"2023_final_hhs_by_parcel.csv"

## output paths for step C
updated_hhs_file_name = 'updated_2024_baseyear_synthetic_households' + f'_{modeller_initial}_{version}' + ".csv"
updated_persons_file_name = 'updated_2024_baseyear_synthetic_persons' + f'_{modeller_initial}_{version}' + ".csv"
h5_file_name = '2024_baseyear_hh_and_persons' + f'_{modeller_initial}_{version}' + ".h5"

## step C doesn't require other configurations


#####
# Land use configurations: switch back
#####

#=====
# Step 3: Interpolate parcel files between what PSRC provided and the parcel data in the horizon year
#=====
## input paths for step 3
parcel_file_name_ealier = r'Z:\Modeling Group\BKRCast\CommonData\original_2014_parcels_urbansim.txt'
parcel_file_name_latter = r'Z:\Modeling Group\BKRCast\SoundCast\2050_Inputs\2050_SC_parcels_bkr.txt'
#parcel_file_name_latter = r'Z:\Modeling Group\BKRCast\Other ESD from PSRC\2020\2020_parcels_bkr.txt'


## output paths for step 3
new_parcel_file_name = 'interpolated_parcel_file_2024_from_PSRC_2014_2050.txt'

## step 3 doesn't require other configurations


#=====
# Step 4: Replace parcel columns with new tables
#=====
## input paths for step 4
new_bellevue_parcel_data_file_name = copy.copy(kc_job_file) # r"2023_COB_Jobs_new_method.csv"
original_parcel_file_name = copy.copy(new_parcel_file_name) # r"interpolated_parcel_file_2023_from_PSRC_2014_2050.txt"

## output paths for step 4
updated_parcel_file_name =  r"2024_baseyear_parcels_urbansim.txt"

## other configurations for step 4
set_Jobs_to_Zeros_All_Bel_Parcels_Not_in_New_Parcel_Data_File = True
columns_list = jobs_columns_List[1:]


#=====
# Step 5: Synchronize population to parcels
#=====
## step 5 doesn't require input paths

## output paths for step  5
output_parcel_file = 'updated_2024_baseyear_parcels_urbansim.txt'

## step 5 doesn't require other configurations