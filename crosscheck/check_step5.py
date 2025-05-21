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

# before rounding the number of households
hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
parcel_df = pd.read_csv(os.path.join(working_folder_lu, updated_parcel_file_name), sep = ' ')
parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

# after rounding the number of households
parcel_df_after = pd.read_csv(os.path.join(working_folder_lu, output_parcel_file), sep = ' ')

print(f"before rounding the number of households at each parcel: {parcel_df['hhexpfac'].sum()}")
print(f"after rounding the number of households at each parcel: {parcel_df_after['HH_P'].sum()}")

print('Done')