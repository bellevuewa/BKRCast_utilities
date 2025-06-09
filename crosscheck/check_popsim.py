# check the outputs from step A is okay
import os
import numpy as np
import pandas as pd

from config import *

hhs_df = pd.read_csv(os.path.join(working_folder_synpop, synthetic_households_file_name))
ppl_df = pd.read_csv(os.path.join(working_folder_synpop, synthetic_population_file_name))
acs_2016 = pd.read_csv(os.path.join(working_folder_synpop, popsim_control_output_file))
parcel_taz_lookup = pd.read_csv(lookup_file)

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

# check if every household has population
n_ppl_in_hh = ppl_df.groupby('household_id')['hh_id'].count().reset_index()
n_ppl_in_hh.rename(columns={'hh_id': 'population'}, inplace=True)
assert len(n_ppl_in_hh[n_ppl_in_hh['population']<1]) == 0, f"Households found no population: {n_ppl_in_hh[n_ppl_in_hh['population']<1]}."

#####
# Compare the number of households at the BKRCastTAZ level in the PopulationSim output matches the ACS data
#####
hhs_df['hh_weight'] = 1
ppl_df['pp_weight'] = 1
# BKRCast TAZ level comparison
tazs = parcel_taz_lookup[['BKRCastTAZ', 'Jurisdiction']].drop_duplicates()
print('Comparing households at the TAZ level...')
for taz, juri in zip(tazs['BKRCastTAZ'], tazs['Jurisdiction']):
    blcgrpid = parcel_taz_lookup[parcel_taz_lookup['BKRCastTAZ']==taz]['GEOID10']
    acs_2016_hh = acs_2016[acs_2016['block_group_id'].isin(blcgrpid)]['hh_bg_weight'].sum()
    hhs = hhs_df[hhs_df['block_group_id'].isin(blcgrpid)]['hh_weight'].sum()
    ppl = ppl_df[ppl_df['block_group_id'].isin(blcgrpid)]['pp_weight'].sum()
    assert acs_2016_hh == hhs, f"Number of households in TAZ {taz} doesn't match."

print('\n')
# BKRCast TMTAZ level comparison
print('Comparing households at the TMTAZ level...')
tmtazs = parcel_taz_lookup[['BKRTMTAZ', 'Jurisdiction']].drop_duplicates()
for taz, juri in zip(tmtazs['BKRTMTAZ'], tmtazs['Jurisdiction']):
    blcgrpid = parcel_taz_lookup[parcel_taz_lookup['BKRTMTAZ']==taz]['GEOID10']
    acs_2016_hh = acs_2016[acs_2016['block_group_id'].isin(blcgrpid)]['hh_bg_weight'].sum()
    hhs = hhs_df[hhs_df['block_group_id'].isin(blcgrpid)]['hh_weight'].sum()
    assert acs_2016_hh == hhs, f"Number of households in TMTAZ {taz} doesn't match."

# check the overall population
print('\n')
print(f"Total control population: {acs_2016['pers_bg_weight'].sum():,d}; total popsim population: {len(ppl_df):,d}.")
print(f"(popsim - control) is {(len(ppl_df) - acs_2016['pers_bg_weight'].sum()) / acs_2016['pers_bg_weight'].sum() * 100:.2f}%.")
print('Done')