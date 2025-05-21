# check the outputs from step C is okay
import os
import h5py
import pandas as pd

import utility
from config import *

updated_hhs = pd.read_csv(os.path.join(working_folder_synpop, updated_hhs_file_name), low_memory = False)
updated_ps = pd.read_csv(os.path.join(working_folder_synpop, updated_persons_file_name), low_memory = False)

updated_h5 = h5py.File(os.path.join(working_folder_synpop, h5_file_name), "r")
updated_h5_hh = utility.h5_to_df(updated_h5, 'Household')
updated_h5_ps = utility.h5_to_df(updated_h5, 'Person')

synthetic_hhs = pd.read_csv(os.path.join(working_folder_synpop, synthetic_households_file_name))
synthetic_persons = pd.read_csv(os.path.join(working_folder_synpop, synthetic_population_file_name))

# check the number of census block groups
assert updated_h5_hh['block_group_id'].count() == synthetic_hhs['block_group_id'].count(), \
    "The numbers of census block groups in both the updated files and the synthetic households don't match!"

# check the number of households
assert len(updated_h5_hh) == len(synthetic_hhs), \
    "The numbers of households in both the updated files and the synthetic households don't match!"

# check the number of p
assert len(updated_h5_ps) == len(updated_ps), \
    "The numbers of people in both the updated files and the synthetic households don't match!"

print('Done')