# check the outputs from step A is okay
import os
import numpy as np
import pandas as pd

from config import *

hhs_df = pd.read_csv(os.path.join(working_folder_synpop, synthetic_households_file_name))

hhs_by_GEOID10_synpop = hhs_df[['block_group_id', 'hhexpfac']].groupby('block_group_id').sum()

adjusted_hhs_by_parcel_df = pd.read_csv(os.path.join(working_folder_synpop, adjusted_hhs_by_parcel_file))
# remove any blockgroup ID is Nan.
all_blcgrp_ids = hhs_df['block_group_id'].unique()
mask = np.isnan(all_blcgrp_ids)
all_blcgrp_ids = sorted(all_blcgrp_ids[~mask])

if popsim_control_file == 'acecon0403.csv':
    # special treatment on GEOID10 530619900020. Since in 2016 ACS no hhs lived in this census blockgroup, when creating popsim control file
    # we move all hhs in this blockgroup to 530610521042. We need to do the same thing when we allocate hhs to parcels.
    adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['GEOID10'] == 530619900020) & (adjusted_hhs_by_parcel_df['total_hhs'] > 0), 'GEOID10'] = 530610521042

hhs_by_blkgrp_adjusted = adjusted_hhs_by_parcel_df.groupby('GEOID10')[['total_hhs']].sum()

for blcgrpid in all_blcgrp_ids:
    assert hhs_by_GEOID10_synpop.loc[blcgrpid, 'hhexpfac'] == hhs_by_blkgrp_adjusted.loc[blcgrpid, 'total_hhs'], \
            '# households in the synthetic population should equal # households in the adjusted parcel file. You need to fix this issue before moving forward.'

print('Done')