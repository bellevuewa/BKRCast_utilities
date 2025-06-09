# check the outputs from step C is okay
"""
This script is modified from hh_and_person_summary.py in BKRCast_Tools-Python3
"""
import os
import h5py
import pandas as pd

import utility
from config import *

TAZ_Subarea_File_Name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv'

def check_results():
    updated_hhs = pd.read_csv(os.path.join(working_folder_synpop, updated_hhs_file_name), low_memory = False)
    updated_ps = pd.read_csv(os.path.join(working_folder_synpop, updated_persons_file_name), low_memory = False)

    updated_h5 = h5py.File(os.path.join(working_folder_synpop, h5_file_name), "r")
    updated_h5_hh = utility.h5_to_df(updated_h5, 'Household')
    updated_h5_ps = utility.h5_to_df(updated_h5, 'Person')

    synthetic_hhs = pd.read_csv(os.path.join(working_folder_synpop, synthetic_households_file_name))
    synthetic_persons = pd.read_csv(os.path.join(working_folder_synpop, synthetic_population_file_name))

    #####
    # Check the city desired to check
    #####
    # step B updated parcel hhs
    adjusted_hhs_stepB = pd.read_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file))
    parcel_taz_lookup = pd.read_csv(lookup_file)
    city_to_check = ['Bellevue', 'Kirkland']
    for city in city_to_check:
        parcel_taz_city = parcel_taz_lookup[parcel_taz_lookup['Jurisdiction']==city.upper()]
        adjusted_hhs_city = adjusted_hhs_stepB[adjusted_hhs_stepB['PSRC_ID'].isin(parcel_taz_city['PSRC_ID'])]
        print(f"Adjusted hhs from step B in {city}: {adjusted_hhs_city['total_hhs'].sum()}")
        parcelized_hhs_city = updated_hhs[updated_hhs['hhparcel'].isin(parcel_taz_city['PSRC_ID'])]
        print(f"Parcelized hhs from step C in {city}: {len(parcelized_hhs_city)}")
        assert adjusted_hhs_city['total_hhs'].sum() == len(parcelized_hhs_city), \
                "Parcelization (step C) is not correct!"
        print('-----')

    # check the number of census block groups
    assert updated_h5_hh['block_group_id'].count() == synthetic_hhs['block_group_id'].count(), \
        "The numbers of census block groups in both the updated files and the synthetic households don't match!"

    # check the number of households
    assert len(updated_h5_hh) == len(synthetic_hhs), \
        "The numbers of households in both the updated files and the synthetic households don't match!"

    # check the number of persons
    assert len(synthetic_persons) == len(updated_ps) & len(updated_h5_ps) == len(updated_ps), \
        "The numbers of people in both the updated files and the synthetic households don't match!"
    
    # display the number of workers and the percentage of workers
    workers_df = updated_h5_ps[['hhno', 'pwtyp', 'psexpfac']].copy()
    workers_df['ft_w'] = 0
    workers_df['pt_w'] = 0
    workers_df.loc[workers_df['pwtyp'] == 1, 'ft_w'] = 1
    workers_df.loc[workers_df['pwtyp'] == 2, 'pt_w'] = 1
    workers_by_hhs_df = workers_df.groupby('hhno').sum().reset_index()
    hh_df = updated_h5_hh.merge(workers_by_hhs_df, on = 'hhno', how = 'left')
    taz_subarea = pd.read_csv(TAZ_Subarea_File_Name, sep = ",", index_col = "BKRCastTAZ")

    hh_taz = hh_df.join(taz_subarea, on = 'hhtaz')
    hh_taz['total_persons'] = hh_taz['hhexpfac'] * hh_taz['hhsize']
    hh_taz['total_hhs'] = hh_taz['hhexpfac']

    # summary by jurisdiction
    summary_by_jurisdiction = hh_taz.groupby('Jurisdiction')[['total_hhs', 'total_persons', 'ft_w', 'pt_w']].sum()
    summary_by_jurisdiction['worker_percentage'] = (summary_by_jurisdiction['ft_w'] + summary_by_jurisdiction['pt_w']) / summary_by_jurisdiction['total_persons'] * 100
    print('\n Summary by jurisdictions')
    print(summary_by_jurisdiction)
    print('Exporting summary by Jurisdiction ... ')
    summary_by_jurisdiction.to_csv(os.path.join(working_folder_synpop, "hh_summary_by_jurisdiction.csv"), header = True)

    # summary by subarea
    taz_subarea.reset_index()
    summary_by_mma = hh_taz.groupby('Subarea')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
    subarea_def = taz_subarea[['Subarea', 'SubareaName']]
    subarea_def = subarea_def.drop_duplicates(keep = 'first')
    subarea_def.set_index('Subarea', inplace = True)
    summary_by_mma = summary_by_mma.join(subarea_def)
    summary_by_mma['worker_percentage'] = (summary_by_mma['ft_w'] + summary_by_mma['pt_w']) / summary_by_mma['total_persons'] * 100
    print('Exporting summary by mma... ')
    summary_by_mma.to_csv(os.path.join(working_folder_synpop, "hh_summary_by_mma.csv"), header = True)

    # summary by taz
    summary_by_taz = hh_taz.groupby('hhtaz')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
    summary_by_taz['worker_percentage'] = (summary_by_taz['ft_w'] + summary_by_taz['pt_w']) / summary_by_taz['total_persons'] * 100
    print('Exporting summary by taz... ')
    summary_by_taz.to_csv(os.path.join(working_folder_synpop, "hh_summary_by_taz.csv"), header = True)

    # summary by parcel
    summary_by_parcels = hh_taz.groupby('hhparcel')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
    print('Exporting summary by parcels...')
    summary_by_parcels.to_csv(os.path.join(working_folder_synpop, 'hh_summary_by_parcel.csv'), header = True)

    # summary by block groups
    parcel_df = pd.read_csv(parcel_filename, low_memory=False) 
    hh_taz = hh_taz.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
    summary_by_geoid10 = hh_taz.groupby('GEOID10')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
    print('Exporting summary by block groups...')
    summary_by_geoid10.to_csv(os.path.join(working_folder_synpop, 'hh_summary_by_geoid10.csv'), header = True)

    print('Result checking ... Done.')


def check_rewrite_stepC_correctly():
    updated_h5 = h5py.File(os.path.join(working_folder_synpop, h5_file_name), "r")
    updated_h5_hh = utility.h5_to_df(updated_h5, 'Household')
    updated_h5_ps = utility.h5_to_df(updated_h5, 'Person')

    prev_h5 = h5py.File(os.path.join(working_folder_synpop, '_tmp_2024t_baseyear_hh_and_persons.h5'), "r")
    prev_h5_hh = utility.h5_to_df(prev_h5, 'Household')
    prev_h5_ps = utility.h5_to_df(prev_h5, 'Person')

    #####
    # Check the output households
    #####
    print(f"After updated, the numebr of households: {sum(updated_h5_hh['hhexpfac'])}")
    print(f"Before updated, the numebr of households: {sum(prev_h5_hh['hhexpfac'])}")
    assert len(updated_h5_hh) == len(prev_h5_hh), 'The numeber of households in the updated script (step C) is not the same as the previous script (parcelizationV3.py)!'

    # check everything in the updated dataframes is the same as the previous yielded dataframes
    checked_hh = updated_h5_hh == prev_h5_hh
    issues_hh = []
    for col in prev_h5_hh.columns:
        if sum(checked_hh[col]) != len(checked_hh[col]):
            print(f'Column {col} in updated households dataframe is not the same as the previous yielded!')
            issues_hh.append(col)
    if len(issues_hh) == 0:
        print('Every cell in both household dataframes is identical.\n')

    #####
    # Check the output persons
    #####
    print(f"After updated, the numebr of persons: {sum(updated_h5_ps['psexpfac'])}")
    print(f"Before updated, the numebr of persons: {sum(prev_h5_ps['psexpfac'])}")
    assert len(updated_h5_ps) == len(prev_h5_ps), 'The numeber of persons in the updated script (step C) is not the same as the previous script (parcelizationV3.py)!'

    # check everything in the updated dataframes is the same as the previous yielded dataframes
    checked_ps = updated_h5_ps == prev_h5_ps
    issues_ps = []
    for col in prev_h5_ps.columns:
        if sum(checked_ps[col]) != len(checked_ps[col]):
            print(f'Column {col} in updated persons dataframe is not the same as the previous yielded!')
            issues_ps.append(col)
    if len(issues_ps) == 0:
        print('Every cell in both person dataframes is identical.\n')

    print('Check the rewritten parcelization ... Done.')


if __name__ == '__main__':
    """
    Run the line below to check if the outputs from step C make sense.
    Here making sense refers to the number of households and persons in the updated files match
    the number of households and persons in the synthetic households and persons files.
    """
    check_results()

    """
    Because the previous version of step C 'parcelizationV3.py' is slow,
    to increase the speed of parcelization, we rewrite the person type assignment in the new version 'step C'.
    And we need to check if the new version of step C yeilds the same results as the previous version.
    Uncomment the following line to run the check for step C.
    """
    # check_rewrite_stepC_correctly()