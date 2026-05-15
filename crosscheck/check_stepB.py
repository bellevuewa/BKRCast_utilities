# check the outputs from step B is okay

# check the outputs from step C is okay
"""
This script is modified from hh_and_person_summary.py in BKRCast_Tools-Python3
"""
import os
import h5py
import pandas as pd

import utility
from config import *

lookup_file = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\parcel_TAZ_2014_lookup.csv'
TAZ_Subarea_File_Name = r'I:\Modeling and Analysis Group\07_ModelDevelopment&Upgrade\NextgenerationModel\BasicData\TAZ_subarea.csv'

def check_results():
    # read outputs from step B
    popsim_control = pd.read_csv(os.path.join(working_folder_synpop, popsim_control_output_file))
    parcel_output = pd.read_csv(os.path.join(working_folder_synpop, parcels_for_allocation_filename))

    # read King County housing units
    kc_du = pd.read_csv(os.path.join(working_folder_lu, kc_du_file))
    kc_du_jurisdiction = kc_du.groupby(by='Jurisdiction')[['SFUnits', 'MFUnits']].sum()
    # read Bellevue housing units
    cob_du = pd.read_csv(os.path.join(working_folder_lu, cob_du_file))
    cob_du_jurisdiction = cob_du.groupby(by='Jurisdiction')[['SFUnits', 'MFUnits']].sum()
    kc_du_jurisdiction.update(cob_du_jurisdiction)
    # read Kikrland and Redmond housing units
    kir_du = None
    red_du = None
    if hhs_control_total_by_TAZ_K:
        # TODO: add kirkland housing units checking
        NotImplemented
    if hhs_control_total_by_TAZ_R:
        red_du = pd.read_csv(os.path.join(working_folder_lu, hhs_control_total_by_TAZ_R))
        red_du.loc[red_du['Jurisdiction']=='Redmond', 'Jurisdiction'] = 'REDMOND'
        red_du_jurisdiction = red_du.groupby(by='Jurisdiction')[['SFUnits', 'MFUnits']].sum()
        kc_du_jurisdiction.update(red_du_jurisdiction)
    kc_du_jurisdiction['HHUnits'] = kc_du_jurisdiction['SFUnits'] + kc_du_jurisdiction['MFUnits']

    # calculate total households using the housing unit data
    kc_du_jurisdiction['TotalHH'] = kc_du_jurisdiction['SFUnits'] * sf_occupancy_rate + kc_du_jurisdiction['MFUnits'] * mf_occupancy_rate
    kc_du_jurisdiction.loc['KIRKLAND', 'TotalHH'] = kc_du_jurisdiction.loc['KIRKLAND', 'SFUnits'] * sf_occupancy_rate_Kirkland + \
                                                    kc_du_jurisdiction.loc['KIRKLAND', 'MFUnits'] * mf_occupancy_rate_Kirkland
    kc_du_jurisdiction.loc['REDMOND', 'TotalHH'] = kc_du_jurisdiction.loc['REDMOND', 'SFUnits'] * sf_occupancy_rate_Redmond + \
                                                   kc_du_jurisdiction.loc['REDMOND', 'MFUnits'] * mf_occupancy_rate_Redmond
    kc_du_jurisdiction['TotalHH'] = kc_du_jurisdiction['TotalHH'].round(0)
    
    # access the outcome from step B
    summary_jurisdiction = pd.read_csv(os.path.join(working_folder_synpop,  summary_by_jurisdiction_filename))
    summary_jurisdiction = summary_jurisdiction.set_index('Jurisdiction')
    kc_du_jurisdiction = kc_du_jurisdiction.merge(summary_jurisdiction, left_on=kc_du_jurisdiction.index, right_on=summary_jurisdiction.index, how='left')
    kc_du_jurisdiction['StepB-Manual'] = kc_du_jurisdiction['adj_hhs_by_parcel'] - kc_du_jurisdiction['TotalHH']
    
    # check the number of households
    assert all(kc_du_jurisdiction['StepB-Manual']<10), \
        "The numbers of households generated from step B and the manual calcualted don't match!"
    
    print('Result checking ... Done.')


if __name__ == '__main__':
    """
    Run the line below to check if the outputs from step B make sense.
    Here making sense refers to the output of total households categorized by single family and multi-family types
    match the manaul calculation (product of number of housing units with occupancy factors).
    """
    check_results()
    print('Please also use **debug=True** in step B to check before/after controlled rounding.')