import pandas as pd
import numpy as np, math
from utility import Data_Scale_Method, Job_Categories, Parcel_Data_Format, SynPop_Data_Scale_Method, dialog_level, IndentAdapter
import logging, os, sys
from synthetic_population import SyntheticPopulation
 
class SynPopDataOperations:
    def __init__(self, synpop: SyntheticPopulation, scen_name, output_dir: str, hhs_assumptions, indent):
        self.synpop = synpop # base synpop on which all the operations wwill be conducted.
        self.output_dir = output_dir
        self.indent = indent
        self.subarea_df = synpop.subarea_df
        self.lookup_df = synpop.lookup_df
        self.hhs_assumptions = hhs_assumptions
        self.scen_name = scen_name

        self.used_taz_attribute_names = set()

        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, indent)
        self.logger.info(f'Start synthetic population operations...')

        base_synpop_summary = self.synpop.summarize_synpop(self.output_dir, self.scen_name)
        self.updated_hhs_by_parcels_df = base_synpop_summary['summary_by_parcel'].copy()
        self.updated_hhs_by_parcels_df = self.updated_hhs_by_parcels_df.rename(columns = {'total_hhs_by_parcel': 'adj_hhs_by_parcel', 'total_persons_by_parcel':'adj_persons_by_parcel'})        

    def generate_total_hhs_data_for_jurisdiction(self, process_rule):
        self.logger.info(f"processing rule: {process_rule}")
        updated_parcel_dict = {}
        self.used_taz_attribute_names.clear()
        if process_rule['Data Format'] == Parcel_Data_Format.Processed_Parcel_Data.value:
            if process_rule['Scale Method'] == SynPop_Data_Scale_Method.Keep_the_Data_from_the_Partner_City.value:
                # only need to overwrite parcels in base_parcel_df with parcel data from process_rule['File]
                updated_parcel_dict = self.replace_hhs_data_using_local_jurisdiction_estimate(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == SynPop_Data_Scale_Method.Scale_by_Household.value:
                updated_parcel_dict = self.scale_by_job_category(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == SynPop_Data_Scale_Method.Scale_by_Total_Hhs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_hhs_by_TAZ(process_rule['Jurisdiction'], True, process_rule['File'], 'BKRTMTAZ')
            else:
                raise Exception(f'invalid scale method {process_rule["Scale Method"]}')
        elif process_rule['Data Format'] == Parcel_Data_Format.BKR_Trip_Model_TAZ_Format.value:
            if process_rule['Scale Method'] == SynPop_Data_Scale_Method.Scale_by_Total_Hhs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_hhs_by_TAZ(process_rule['Jurisdiction'], process_rule['File'], 'BKRTMTAZ')
        elif process_rule['Data Format'] == Parcel_Data_Format.BKRCastTAZ_Format.value:
            if process_rule['Scale Method'] == SynPop_Data_Scale_Method.Scale_by_Total_Hhs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_hhs_by_TAZ(process_rule['Jurisdiction'], process_rule['File'], 'BKRCastTAZ')

        self.logger.info(f'All rules are processed.')
        return updated_parcel_dict
    
    def replace_hhs_data_using_local_jurisdiction_estimate(self, jurisdiction, set_juris_base_hhs_to_zero, local_housing_unit_data_file) -> dict:
        # Replace hhs estimate with COB's forecast
        # if some parcels are missing from the cob_du_df, export them for further investigation.
        adjusted_hhs_by_parcel_df = self.updated_hhs_by_parcels_df.copy()

        local_du_df = pd.read_csv(os.path.join(self.output_dir, local_housing_unit_data_file))
        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == jurisdiction.upper()]
        local_parcels_provided = local_du_df.shape[0]
        if adjusted_hhs_by_parcel_df.shape[0] != local_parcels_provided:
            self.logger.info('COB forecast does not cover all parcels. Please cehck the missing parcel files for further investigation.')
            cob_missing_parcels_df = adjusted_hhs_by_parcel_df.loc[~adjusted_hhs_by_parcel_df['PSRC_ID'].isin(local_du_df['PSRC_ID'])]
            cob_missing_parcels_df.to_csv(os.path.join(self.output_dir, 'cob_missing_parcels.csv'), index = False)
            self.logger.info(f'{cob_missing_parcels_df.shape[0]} parcels are missing in {local_housing_unit_data_file}.')
        
        local_du_df['sfhhs'] = local_du_df['SFUnits'] * self.hhs_assumptions[jurisdiction]["sfhh_occ"] 
        local_du_df['mfhhs'] = local_du_df['MFUnits'] * self.hhs_assumptions[jurisdiction]["mfhh_occ"]
        local_du_df['sfpersons'] = local_du_df['sfhhs'] * self.hhs_assumptions[jurisdiction]["sfhhsize"]
        local_du_df['mfpersons'] = local_du_df['mfhhs'] * self.hhs_assumptions[jurisdiction]["mfhhsize"]
        local_du_df['source'] = 'local_parcel'

        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(local_du_df[['PSRC_ID', 'source', 'sfhhs', 'mfhhs', 'sfpersons', 'mfpersons']], on = 'PSRC_ID', how = 'left')
        # reset hhs and persons in all COB parcels to zero. Only use local forecast.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['Jurisdiction'] == jurisdiction.upper(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        # it is importand to use cobflag rather than Jurisdiction, because (hhs and persons in) parcels flagged by cobflag are provided by COB staff.
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['source'] == 'local_parcel', 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['sfhhs'] + adjusted_hhs_by_parcel_df['mfhhs']
        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['source'] == 'local_parcel', 'adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['sfpersons'] + adjusted_hhs_by_parcel_df['mfpersons']

        selection = self.updated_hhs_by_parcels_df['PSRC_ID'].isin(adjusted_hhs_by_parcel_df['PSRC_ID'])
        b4_change_df = self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].copy()
        self.logger.info(f'before synthetic population update, total hhs in {jurisdiction}: {b4_change_df.sum().to_dict()}')
        
        # must set index first before using update
        self.updated_hhs_by_parcels_df.set_index('PSRC_ID', inplace = True)
        self.updated_hhs_by_parcels_df.update(adjusted_hhs_by_parcel_df.set_index('PSRC_ID')[['adj_hhs_by_parcel','adj_persons_by_parcel']])
        
        self.updated_hhs_by_parcels_df.reset_index(inplace = True)       
        self.logger.info(f"after synthetic population update, total hhs in {jurisdiction}: {self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].sum()}")
        self.logger.info(f"Inputs from {jurisdiction}'s local input: {local_du_df[['SFUnits', 'MFUnits']].sum().to_dict()}")
        df_dict = {
            "data_frame": self.updated_hhs_by_parcels_df,
            'local_data': local_du_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        return df_dict
    
    def export_popsim_control_file(self, control_template_name, popsim_control_file):
        ### Create control file for PopulationSim
        popsim_control_df = pd.read_csv(os.path.join(self.output_dir, control_template_name), sep = ',')
        hhs_by_geoid10_df =  self.updated_hhs_by_parcels_df[['GEOID10', 'adj_hhs_by_parcel']].groupby('GEOID10').sum()
        adj_persons_by_GEOID10 = self.updated_hhs_by_parcels_df[['GEOID10', 'adj_persons_by_parcel']].groupby('GEOID10').sum()
        hhs_by_geoid10_df = hhs_by_geoid10_df.merge(adj_persons_by_GEOID10, left_index = True, right_index = True, how = 'left')
        hhs_by_geoid10_df.fillna(0, inplace = True)
        popsim_control_df = popsim_control_df.merge(hhs_by_geoid10_df, left_on = 'block_group_id', right_on = 'GEOID10', how = 'left')
        error_blkgrps_df = popsim_control_df.loc[popsim_control_df.isna().any(axis = 1)]
        if error_blkgrps_df.shape[0] > 0:
            self.logger.info('Some blockgroups are missing values. Please check the error_census_blockgroup.csv')
            self.logger.info('The missing values are all replaced with zeros.')
            error_blkgrps_df.to_csv(os.path.join(self.output_dir, 'error_census_blockgroup.csv'), index = False)

        popsim_control_df.fillna(0, inplace = True)
        popsim_control_df['hh_bg_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['hh_tract_weight'] = popsim_control_df['adj_hhs_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_bg_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df['pers_tract_weight'] = popsim_control_df['adj_persons_by_parcel'].round(0).astype(int)
        popsim_control_df.drop(hhs_by_geoid10_df.columns, axis = 1, inplace = True)
        popsim_control_df.to_csv(os.path.join(self.output_dir, popsim_control_file), index = False)
        self.logger.info(f'PopulationSim control file is saved to {popsim_control_file}')
        total_hhs = popsim_control_df['hh_bg_weight'].sum()
        total_persons = popsim_control_df['pers_bg_weight'].sum()
        self.logger.info(f'{total_hhs} households, {total_persons} persons are in the control file.')

    def scale_selected_base_data_by_total_hhs_by_TAZ(self, jurisdiction, local_TAZ_housing_data_file, taz_attr_name) -> dict:
        self.used_taz_attribute_names.add(taz_attr_name)

        hhs_control_total_by_TAZ_df = pd.read_csv(os.path.join(self.output_dir, local_TAZ_housing_data_file))
        juris_list = hhs_control_total_by_TAZ_df['Jurisdiction'].unique()
        self.logger.info(f'The following jurisdictions are included in {local_TAZ_housing_data_file}: {juris_list}')    

        hhs_control_total_by_TAZ_df['total_persons'] = 0
        hhs_control_total_by_TAZ_df['total_hhs'] = 0
        selection = hhs_control_total_by_TAZ_df['Jurisdiction'].str.upper() == jurisdiction.upper()
        hhs_control_total_by_TAZ_df.loc[selection, 'sfhhs'] = hhs_control_total_by_TAZ_df['SFUnits'] * self.hhs_assumptions[jurisdiction]["sfhh_occ"]
        hhs_control_total_by_TAZ_df.loc[selection, 'mfhhs'] = hhs_control_total_by_TAZ_df['MFUnits'] * self.hhs_assumptions[jurisdiction]["mfhh_occ"]
        hhs_control_total_by_TAZ_df.loc[selection, 'total_hhs'] = hhs_control_total_by_TAZ_df['sfhhs'] + hhs_control_total_by_TAZ_df["mfhhs"]
        hhs_control_total_by_TAZ_df.loc[selection, 'total_persons'] = hhs_control_total_by_TAZ_df['sfhhs'] * self.hhs_assumptions[jurisdiction]["sfhhsize"] + hhs_control_total_by_TAZ_df['mfhhs'] * self.hhs_assumptions[jurisdiction]["mfhhsize"]
            
        hhs_by_parcel_df = self.updated_hhs_by_parcels_df.loc[self.updated_hhs_by_parcels_df['Jurisdiction'].str.upper() == jurisdiction.upper()].copy()
        
        parcels_in_trip_model_TAZ_df = pd.merge(hhs_by_parcel_df[['PSRC_ID', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']], self.lookup_df.loc[self.lookup_df[taz_attr_name].notna(), ['PSRC_ID', 'Jurisdiction', taz_attr_name]], on = 'PSRC_ID', how = 'inner')
        parcels_in_trip_model_TAZ_df = parcels_in_trip_model_TAZ_df.merge(hhs_control_total_by_TAZ_df[[taz_attr_name]], on  = taz_attr_name, how = 'inner')

        hhs_by_TAZ_df = parcels_in_trip_model_TAZ_df[[taz_attr_name, 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby(taz_attr_name).sum().reset_index()
        hhs_by_TAZ_df = pd.merge(hhs_by_TAZ_df, hhs_control_total_by_TAZ_df.loc[hhs_control_total_by_TAZ_df['total_hhs'] >= 0, [taz_attr_name, 'total_hhs', 'total_persons']], on = taz_attr_name, how = 'outer')
        hhs_by_TAZ_df.fillna(value = {'total_hhs' : 0, 'total_persons' : 0}, inplace = True)
        hhs_by_taz_comparison_file = f'{self.scen_name}_by_taz_comparison.csv'
        hhs_by_TAZ_df.to_csv(os.path.join(self.output_dir, hhs_by_taz_comparison_file), index = False)

        adjusted_hhs_by_parcel_df = self.updated_hhs_by_parcels_df.loc[self.updated_hhs_by_parcels_df['Jurisdiction'].str.upper() == jurisdiction.upper()].copy()

        right_cols = ['PSRC_ID'] 
        if taz_attr_name not in adjusted_hhs_by_parcel_df.columns:
            right_cols.append(taz_attr_name)
        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(parcels_in_trip_model_TAZ_df[right_cols], on = 'PSRC_ID', how = 'left')

        for city in juris_list:
            # reset hhs and persons to zero in Kirkland and Redmond parcels that are not included in local estimates. We will use their local forecast.
            adjusted_hhs_by_parcel_df.loc[(adjusted_hhs_by_parcel_df['Jurisdiction'].str.upper() == city.upper()) & adjusted_hhs_by_parcel_df[taz_attr_name].isna(), ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        # for a TAZ that have no hhs in PSRC erstimate but have hhs in local jurisdiction estimate, evenly distribute hhs to all parcels in that TAZ
        tazs_for_evenly_distri_df = hhs_by_TAZ_df.loc[(hhs_by_TAZ_df['adj_hhs_by_parcel'] == 0) & (hhs_by_TAZ_df['total_hhs'] > 0) ]
        if tazs_for_evenly_distri_df.shape[0] > 0:
            self.logger.info(f'The following tazs do not have households in the base file, but do have some in the local land use forecasting ')
            self.logger.info(f'These households are evenly distributed among parcels in each TAZ')
            for row in tazs_for_evenly_distri_df.itertuples():
                taz_value = getattr(row, taz_attr_name)
                mask = adjusted_hhs_by_parcel_df[taz_attr_name] == taz_value
                count = mask.sum()
                # find parcels within this taz
                count = mask.sum()
                if count == 0 and row.total_hhs > 0:
                    self.logger.info(f"{taz_attr_name} {taz_value} in the base has zero households but has {row.total_hhs} households from {jurisdiction}'s data.")
                    continue
                adjusted_hhs_by_parcel_df.loc[mask, 'adj_hhs_by_parcel'] = row.total_hhs / count
                adjusted_hhs_by_parcel_df.loc[mask, 'adj_persons_by_parcel'] = row.total_persons / count
                self.logger.info(f'{taz_attr_name} {taz_value}: {row.total_hhs} households {count} parcels')

        # for other parcels, scale up hhs to match local jurisdiction's forecast by applying factors calculated in TAZ level
        tazs_for_proportional_distri_df = hhs_by_TAZ_df.loc[hhs_by_TAZ_df['adj_hhs_by_parcel'] > 0].copy()
        tazs_for_proportional_distri_df['ratio_hhs'] = tazs_for_proportional_distri_df['total_hhs'] / tazs_for_proportional_distri_df['adj_hhs_by_parcel']
        tazs_for_proportional_distri_df['ratio_persons'] = tazs_for_proportional_distri_df['total_persons'] / tazs_for_proportional_distri_df['adj_persons_by_parcel']

        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.merge(tazs_for_proportional_distri_df[[taz_attr_name, 'ratio_hhs', 'ratio_persons']], on = taz_attr_name, how = 'left')
        adjusted_hhs_by_parcel_df = adjusted_hhs_by_parcel_df.fillna(value = {'ratio_hhs' : 1, 'ratio_persons' : 1})
        adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_hhs']
        adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] = adjusted_hhs_by_parcel_df['adj_persons_by_parcel'] * adjusted_hhs_by_parcel_df['ratio_persons']
        adjusted_hhs_by_parcel_df.drop(columns = ['ratio_hhs', 'ratio_persons'], inplace = True)
          
        sum_by_taz = adjusted_hhs_by_parcel_df[[taz_attr_name, 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby(taz_attr_name).sum().reset_index()
        sum_by_taz = sum_by_taz.merge(hhs_control_total_by_TAZ_df[[taz_attr_name, 'SFUnits', 'MFUnits', 'sfhhs', 'mfhhs', 'total_hhs', 'total_persons']], on = taz_attr_name, how = 'outer')
        sum_fn = f'{self.scen_name}_{jurisdiction}_hhs_by_{taz_attr_name}_comparison_after_scaling.csv'
        sum_by_taz.to_csv(os.path.join(self.output_dir, sum_fn), index = False)
        self.logger.info(f'household comparison by {taz_attr_name} after scaling is saved to {sum_fn}')

        selection = self.updated_hhs_by_parcels_df['PSRC_ID'].isin(adjusted_hhs_by_parcel_df['PSRC_ID'])
        b4_change_df = self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].copy()
        self.logger.info(f'before synthetic population update, total hhs in {jurisdiction}: {b4_change_df.sum().to_dict()}')
        
        # must set index first before using update
        self.updated_hhs_by_parcels_df.set_index('PSRC_ID', inplace = True)
        self.updated_hhs_by_parcels_df.update(adjusted_hhs_by_parcel_df.set_index('PSRC_ID')[['adj_hhs_by_parcel','adj_persons_by_parcel']])
    
        self.updated_hhs_by_parcels_df.reset_index(inplace = True)       
        self.logger.info(f"after synthetic population update, total hhs in {jurisdiction}: {self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].sum().to_dict()}")
        self.logger.info(f"Inputs from {jurisdiction}'s local input: {hhs_control_total_by_TAZ_df[['SFUnits', 'MFUnits']].sum().to_dict()}")
  
        df_dict = {
            "data_frame": self.updated_hhs_by_parcels_df,
            'local_data': hhs_control_total_by_TAZ_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        return df_dict
    
    def treatment_for_special_GEOID10(self):
        adjusted_hhs_by_parcel_df = self.updated_hhs_by_parcels_df.copy()
        adj_persons_by_GEOID10 = adjusted_hhs_by_parcel_df[['GEOID10', 'adj_persons_by_parcel']].groupby('GEOID10').sum()

        # in ACS 2016 there is no hhs in Census block group 530619900020, but in PSRC's future hhs forecast there are. We need to relocate these households from parcels in this blockgroup to  
        # parcels in block group 530610521042 while staying in the same BKRCastTAZ. 
        special_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530619900020) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 0)
        special_hhs_by_TAZ = adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['BKRCastTAZ', 'adj_hhs_by_parcel', 'adj_persons_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
        # move all persons in 530619900020 to 530610521042
        adj_persons_by_GEOID10.loc[530610521042, 'adj_persons_by_parcel'] += adj_persons_by_GEOID10.loc[530619900020, 'adj_persons_by_parcel']

        # move hhs from parcels in 530619900020 to parcels in 530610521042 && same TAZ
        for row in special_hhs_by_TAZ.itertuples():
            mf_parcels_flag = (adjusted_hhs_by_parcel_df['GEOID10'] == 530610521042) & (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == row.BKRCastTAZ) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] > 1)
            mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcels_flag].shape[0]
            if mf_parcels_count > row.adj_hhs_by_parcel:
                selected_ids = adjusted_hhs_by_parcel_df.sample(n = int(row.adj_hhs_by_parcel))['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
            else:
                increase = math.floor(row.adj_hhs_by_parcel / mf_parcels_count)
                adjusted_hhs_by_parcel_df.loc[mf_parcels_flag, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                diff = row.adj_hhs_by_parcel - increase * mf_parcels_count
                selected_ids = adjusted_hhs_by_parcel_df.sample(n = 1)['PSRC_ID']
                adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + diff
            adjusted_hhs_by_parcel_df.loc[special_parcels_flag, ['adj_hhs_by_parcel', 'adj_persons_by_parcel']] = 0

        # must set index first before using update
        self.updated_hhs_by_parcels_df.set_index('PSRC_ID', inplace = True)
        self.updated_hhs_by_parcels_df.update(adjusted_hhs_by_parcel_df.set_index('PSRC_ID')[['adj_hhs_by_parcel','adj_persons_by_parcel']])
    
        self.updated_hhs_by_parcels_df.reset_index(inplace = True)       
                 
    def controlled_rounding(self):
        self.logger.info('Rounding households to integer. Controlled by BKRCastTAZ subtotal....')
 
        adjusted_hhs_by_parcel_df = self.updated_hhs_by_parcels_df.copy()
        adj_hhs_by_BKRCastTAZ = adjusted_hhs_by_parcel_df[['BKRCastTAZ', 'adj_hhs_by_parcel']].groupby('BKRCastTAZ').sum().round(0).astype(int)
        controlled_taz_hhs = adj_hhs_by_BKRCastTAZ.reset_index().to_dict('records')
        total_hhs_before_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()

        for record in controlled_taz_hhs:
            adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'], 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].round(0)
            subtotal = adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'], 'adj_hhs_by_parcel'].sum()
            diff = subtotal - record['adj_hhs_by_parcel']
            mf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] >= 2)
            sf_parcel_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ']) & (adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] == 1)
            mf_parcels_count = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].shape[0]
            sf_parcels_count = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].shape[0]
            if diff > 0: 
                # too many hhs in this TAZ after rounding. need to bring down subtotal 
                # start from mf parcels. 
                if mf_parcels_count > 0:
                    if mf_parcels_count < diff:
                        adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                        diff = diff - mf_parcels_count
                    else: # number of mf parcels are more than diff,  randomly pick diff number of mf parcels and reduce adj_hhs_by_parcel in each parcel  by 1
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                        diff = 0
                # if rounding issue is not resolved yet, deal with it in sf parcel
                if (diff > 0) and (sf_parcels_count > 0):
                    if sf_parcels_count < diff: 
                        adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                        diff = diff - sf_parcels_count
                    else: # number of sf parcels are more than diff, randomly pick diff number of sf parcels and reduce adj_hhs_by_parcel in each by 1 (set to zero)
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(diff))['PSRC_ID']
                        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] - 1
                        diff = 0
                # last option, if rounding issue is still not resolved, 
                if diff > 0:
                    self.logger.info(f"TAZ {record['BKRCastTAZ']}: rounding issue is not resolved. Difference is {diff}")
            elif diff < 0:
                # too less hhs in this TAZ after rounding. need to increase subtotal
                if mf_parcels_count > 0:
                    # evenly distribute diff to all mf parcel, then the remaining to a ramdomly selected one
                    if mf_parcels_count < abs(diff):
                        increase = math.floor(abs(diff) / mf_parcels_count)
                        adjusted_hhs_by_parcel_df.loc[mf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                        diff = diff + increase * mf_parcels_count
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = 1)['PSRC_ID']
                        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)
                        diff = diff + abs(diff)
                    else:
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[mf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
                        diff = diff + abs(diff)
                        
                else: # if no mf parcel is available, add diff to sf parcels
                    if sf_parcels_count > 0:
                        if sf_parcels_count < abs(diff):
                            increase = math.floor(abs(diff) / sf_parcels_count)
                            adjusted_hhs_by_parcel_df.loc[sf_parcel_flags, 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + increase
                            diff = diff + increase * sf_parcels_count
                            selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = 1)['PSRC_ID']
                            adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)
                            diff = diff + abs(diff)
                        else:
                            selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[sf_parcel_flags].sample(n = int(abs(diff)))['PSRC_ID']
                            adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + 1
                            diff = diff + abs(diff)
                    else:  # last option, add diff to a ramdomly selected parcel
                        applicable_parcels_flags = (adjusted_hhs_by_parcel_df['BKRCastTAZ'] == record['BKRCastTAZ'])
                        selected_parcel_ids = adjusted_hhs_by_parcel_df.loc[applicable_parcels_flags].sample(n = 1)['PSRC_ID']
                        adjusted_hhs_by_parcel_df.loc[adjusted_hhs_by_parcel_df['PSRC_ID'].isin(selected_parcel_ids), 'adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] + abs(diff)

        adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'] = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].astype(int)
        total_hhs_after_rounding = adjusted_hhs_by_parcel_df['adj_hhs_by_parcel'].sum()
        self.logger.info('Controlled rounding is complete. ')
        self.logger.info(f'Total hhs before rounding: {total_hhs_before_rounding}, after: {total_hhs_after_rounding}')


        selection = self.updated_hhs_by_parcels_df['PSRC_ID'].isin(adjusted_hhs_by_parcel_df['PSRC_ID'])
        b4_change_df = self.updated_hhs_by_parcels_df.loc[selection, ['adj_hhs_by_parcel','adj_persons_by_parcel']].copy()
       
        # must set index first before using update
        self.updated_hhs_by_parcels_df.set_index('PSRC_ID', inplace = True)
        self.updated_hhs_by_parcels_df.update(adjusted_hhs_by_parcel_df.set_index('PSRC_ID')[['adj_hhs_by_parcel','adj_persons_by_parcel']])
    
        self.updated_hhs_by_parcels_df.reset_index(inplace = True)       
  
        df_dict = {
            "data_frame": self.updated_hhs_by_parcels_df,
            'before_change': b4_change_df,
        }

        return df_dict

    def export_household_allocation_guide_file(self, fn):
        taz_attr_list = list(self.used_taz_attribute_names)
        cols = ['PSRC_ID', 'GEOID10', 'BKRCastTAZ', 'adj_hhs_by_parcel']

        self.updated_hhs_by_parcels_df[cols].rename(columns = {'adj_hhs_by_parcel':'total_hhs'}).to_csv(os.path.join(self.output_dir, fn), index = False)

