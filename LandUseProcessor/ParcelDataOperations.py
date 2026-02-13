import pandas as pd
import numpy as np
from utility import Data_Scale_Method, Job_Categories, Parcel_Data_Format, dialog_level, IndentAdapter
import logging, os, sys
from LandUseUtilities.Parcels import Parcels
 

class ParcelDataOperations:
    def __init__(self, base_parcels: Parcels, output_dir: str, output_filename: str, indent):
        self.base_parcel = base_parcels
        self.subarea_df = base_parcels.subarea_df.copy()
        self.lookup_df = base_parcels.lookup_df.copy()
        self.output_dir = output_dir
        self.output_filename = os.path.join(output_dir, output_filename)
        self.updated_parcels_df = base_parcels.original_parcels_df.copy()
        base_logger = logging.getLogger(__name__)
        self.indent = indent
        self.logger = IndentAdapter(base_logger, indent)


    def export_updated_parcels(self, export_name: str = None) -> Parcels: 
        if self.updated_parcels_df is None:
            self.logger.error("Updated parcel dataframe is not available for export.")
            raise ValueError("Updated parcel dataframe is not available.")

        if export_name is not None:
            fn = os.path.join(self.output_dir, export_name)
        else:
            fn = self.output_filename
        self.updated_parcels_df.to_csv(fn, sep = ' ', index=False)
        out = Parcels.from_dataframe(self.updated_parcels_df, filename=fn, data_year=self.base_parcel.data_year, subarea_df=self.base_parcel.subarea_df, lookup_df=self.base_parcel.lookup_df, log_indent = self.indent + 1)
        self.logger.info(f'Updated parcel data exported to: {fn}')
        return out
        
    def controlled_rounding(self, updated_data_df, attr_name, control_total, index_attr_name):
        # find residential parcels within taz     
        total_rows = updated_data_df.shape[0]
        if total_rows != 0:
            already_assigned = updated_data_df[attr_name].sum()
        else:
            already_assigned = 0
        
        # how many need to be assigned or removed to match the control total
        diff = int(control_total - already_assigned)
        if (diff == 0) | total_rows == 0:
            return self.updated_parcels_df

        if total_rows >= abs(diff):
            selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = False) 
        else:
            selected_indices = np.random.choice(updated_data_df.index, size = abs(diff), replace = True) 
        
        unique_indices, counts = np.unique(selected_indices, return_counts=True)
        sorted_zipped = sorted(zip(unique_indices, counts), key=lambda x: x[1], reverse=True)

        index_for_2nd_round = []
        if control_total >= already_assigned: # need to add to match the control total, 
            for index, count in sorted_zipped:
                updated_data_df.loc[index, attr_name] += count   
        else:  # need to remove to match the control total. more complicated.
            remaining = 0
            for index, count in sorted_zipped:
                count += remaining
                # need to ensure no negative values
                if updated_data_df.loc[index, attr_name] >= count:
                    updated_data_df.loc[index, attr_name] -= count
                    remaining = 0
                    index_for_2nd_round.append(index)
                else:
                    remaining = count - updated_data_df.loc[index, attr_name]
                    updated_data_df.loc[index, attr_name] = 0

            if (remaining > 0):
                for index in index_for_2nd_round:
                    curValue = updated_data_df.loc[index, attr_name]
                    if curValue >= remaining:
                        updated_data_df.loc[index, attr_name] = curValue - remaining
                        remaining = 0
                        break
                    else:
                        remaining -= updated_data_df.loc[index, attr_name]
                        updated_data_df.loc[index, attr_name] = 0
        
       
        return updated_data_df

    def generate_employment_data_for_jurisiction(self, process_rule) -> dict:
        '''
        produce parcel data based on the selected processing rules.
        
        :param process_rule: rule
        :return: a dict:{
            "data_frame": updated_parcels_df.reset_index(),
            'local_data': local_data_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }
        '''
        # process_rule: dict 'Jurisdiction', 'File', 'Data_Format', 'Scale_Method'
        self.logger.info(f"processing rule: {process_rule}")
        updated_parcel_dict = {}
        if process_rule['Data Format'] == Parcel_Data_Format.Processed_Parcel_Data.value:
            if process_rule['Scale Method'] == Data_Scale_Method.Keep_the_Data_from_the_Partner_City.value:
                # only need to overwrite parcels in base_parcel_df with parcel data from process_rule['File]
                updated_parcel_dict = self.replace_selected_base_data_from_with_local_jurisdiction(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Job_Category.value:
                updated_parcel_dict = self.scale_by_job_category(process_rule['Jurisdiction'], True, process_rule['File'])
            elif process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_jobs_by_TAZ(process_rule['Jurisdiction'], True, process_rule['File'])
            else:
                raise Exception(f'invalid scale method {process_rule["Scale Method"]}')
        elif process_rule['Data Format'] == Parcel_Data_Format.BKR_Trip_Model_TAZ_Format.value:
            if process_rule['Scale Method'] == Data_Scale_Method.Scale_by_Total_Jobs_by_TAZ.value:
                updated_parcel_dict = self.scale_selected_base_data_by_total_jobs_by_TAZ(process_rule['Jurisdiction'], process_rule['File'], 'BKRTMTAZ', 'ControlTotalJobs')
       
        return updated_parcel_dict

    def replace_selected_base_data_from_with_local_jurisdiction(self, jurisdiction, set_juris_base_jobs_to_zero, local_parcel_data_file) -> dict:
        jobs_cat = Job_Categories.copy()
        jobs_cat.append('EMPTOT_P')

        updated_parcels_df = self.updated_parcels_df.copy()
        local_data_df = pd.read_csv(local_parcel_data_file, low_memory=False)

        # keep only required columns
        required_cols = ['PSRC_ID'] + jobs_cat
        local_data_df = local_data_df[required_cols]

        full_juris_parcels_df = self.lookup_df.loc[self.lookup_df['Jurisdiction'] == jurisdiction.upper()]  #  a complete list of parcels in Jurisdiction
        actual_juris_parcels_df = local_data_df.loc[local_data_df['PSRC_ID'].isin(full_juris_parcels_df['PSRC_ID'])] # parcels included in local job file
        not_in_full_juris_parcels = actual_juris_parcels_df.loc[~actual_juris_parcels_df['PSRC_ID'].isin(full_juris_parcels_df['PSRC_ID'])] # parcels in local job file but not in the complete list
        missing_juris_parcels_df = updated_parcels_df.loc[updated_parcels_df['PARCELID'].isin(full_juris_parcels_df.loc[~full_juris_parcels_df['PSRC_ID'].isin(local_data_df['PSRC_ID']), 'PSRC_ID'])]
        
        if missing_juris_parcels_df.shape[0] > 0:
            missing_fn = f'missing_{jurisdiction}_parcels.csv'
            missing_juris_parcels_df.to_csv(os.path.join(self.output_dir, missing_fn), sep = ',', index = False)
            self.logger.info(f"missing parcels are saved in {missing_fn}")
            self.logger.info(f"jobs in {jurisdiction} missing parcels (base file): {missing_juris_parcels_df['EMPTOT_P'].sum()}")
        
        if not_in_full_juris_parcels.shape[0] > 0:
            invalid_parcels_fn = f'not_valid_{jurisdiction}_parcels.csv'
            not_in_full_juris_parcels.to_csv(os.path.join(self.output_dir, invalid_parcels_fn), sep = ',', index = False)
            self.logger.info(f"invalid parcels in {jurisdiction} are saved in {invalid_parcels_fn}")        

        self.logger.info(f"total jobs in {jurisdiction}-provided parcel data: {local_data_df['EMPTOT_P'].sum()}") 
        self.logger.info(f"set all jobs to zero for {jurisdiction} parcels in the base parcel file: {set_juris_base_jobs_to_zero}")
        
        if set_juris_base_jobs_to_zero:
            # find parcels in base parcel data that are not in local data, set jobs to zero
            jobs_to_be_zeroed_out = updated_parcels_df.loc[updated_parcels_df['PARCELID'].isin(missing_juris_parcels_df['PARCELID']), 'EMPTOT_P'].sum()
            updated_parcels_df.loc[updated_parcels_df['PARCELID'].isin(missing_juris_parcels_df['PARCELID']), Job_Categories] = 0
        # index by parcel id for alignment
        updated_parcels_df = updated_parcels_df.set_index('PARCELID')
        local_data_df = local_data_df.set_index('PSRC_ID')

        # only update rows that exist in both
        common_ids = updated_parcels_df.index.intersection(local_data_df.index)
        b4_change_df = updated_parcels_df.loc[common_ids, jobs_cat]
        self.logger.info(f"{len(common_ids)} parcels are found in both the base file and the parcel file provided by {jurisdiction}")
        self.logger.info(f"total jobs among these parcels before replacement: {b4_change_df['EMPTOT_P'].sum()} ")

        # replace values
        updated_parcels_df.loc[common_ids, jobs_cat] = local_data_df.loc[common_ids, jobs_cat]
        self.logger.info(f"total jobs among these parcels after replacement: {updated_parcels_df.loc[common_ids, 'EMPTOT_P'].sum()}")
        
        # calculate total jobs after change
        updated_parcels_df.fillna(0, inplace=True)
        updated_parcels_df['EMPTOT_P'] = updated_parcels_df[Job_Categories].sum(axis=1)

        df_dict = {
            "data_frame": updated_parcels_df.reset_index(),
            'local_data': local_data_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        self.updated_parcels_df = updated_parcels_df.reset_index().copy()
        # restore index
        return df_dict 
    
    def scale_selected_base_data_by_total_jobs_by_TAZ(self, jurisdiction, local_TAZ_data_file, taz_attr_name, total_job_attr_name) -> dict:
        '''
        scale up the employment data in the base file to match the control total provided by local jurisdiction.
        
        :param jurisdiction: local jurisdiction name
        :param local_TAZ_data_file: data (aggregated to TAZ) provided by jurisdiction
        :param taz_attr_name: TAZ attribute name
        :param total_job_attr_name: attribute name for control total (in local_TAZ_data_file)
        :return: a dict including scaled parcel data
                df_dict = {
                    "data_frame": updated_parcel_df.reset_index(),
                    'local_data': local_jobs_by_TAZ_df,
                    'before_change': b4_change_df,
                    "local_data_provider": jurisdiction
                }       
        :rtype: dict
        '''
        updated_parcel_df = self.updated_parcels_df.copy()

        local_jobs_by_TAZ_df = pd.read_csv(local_TAZ_data_file, low_memory=False)
        total_job_control = local_jobs_by_TAZ_df[total_job_attr_name].sum()
        target_parcels_df = self.lookup_df.loc[self.lookup_df[taz_attr_name].isin(local_jobs_by_TAZ_df[taz_attr_name])]
        target_parcels_df = updated_parcel_df.merge(target_parcels_df[['PSRC_ID', taz_attr_name]], left_on = 'PARCELID', right_on = 'PSRC_ID')
        b4_change_df = target_parcels_df.copy()
        job_cat = Job_Categories.copy()
        job_cat.append('EMPTOT_P')
        job_cat.append(taz_attr_name)

        total_jobs_b4_scaling = target_parcels_df['EMPTOT_P'].sum()
        self.logger.info(f'total jobs provided by {jurisdiction}: {total_job_control}')
        self.logger.info(f'total jobs in {jurisdiction} before scaling: {total_jobs_b4_scaling}')
        jobs_by_TAZ_b4_scaling_df = target_parcels_df[job_cat].groupby(taz_attr_name).sum()
        local_jobs_by_TAZ_df = local_jobs_by_TAZ_df.merge(jobs_by_TAZ_b4_scaling_df.reset_index(), on = taz_attr_name, how = 'left')
        local_jobs_by_TAZ_df.loc[local_jobs_by_TAZ_df['EMPTOT_P'] != 0, 'scale'] = local_jobs_by_TAZ_df[total_job_attr_name] / local_jobs_by_TAZ_df['EMPTOT_P']
        local_jobs_by_TAZ_df.loc[local_jobs_by_TAZ_df['EMPTOT_P'] == 0, 'scale'] = 1

        target_parcels_df = target_parcels_df.merge(local_jobs_by_TAZ_df[[taz_attr_name, 'scale']], on = taz_attr_name, how = 'left')
        target_parcels_df['EMPTOT_P'] = 0
 
        # scaling
        for col in Job_Categories:
            target_parcels_df[col] = target_parcels_df[col] * target_parcels_df['scale']
            target_parcels_df[col] = target_parcels_df[col].round(0).astype(int)
        self.logger.info(f'total scaled jobs in {jurisdiction} before controlled rounding: {target_parcels_df[Job_Categories].sum(axis=1).sum()}')
        
        # apply controlled rounding
        total_scaled = target_parcels_df[Job_Categories].sum(axis = 1).sum()
        diff = total_job_control - total_scaled
        for col in Job_Categories:
            assigned_by_job_cat = target_parcels_df[col].sum()
            job_cat_ctrl = int(round((assigned_by_job_cat / total_job_control) * diff + assigned_by_job_cat, 0))
            target_parcels_df = self.controlled_rounding(target_parcels_df, col, job_cat_ctrl, 'PARCELID')

        target_parcels_df['EMPTOT_P'] = target_parcels_df[Job_Categories].sum(axis=1)
        self.logger.info(f'total scaled jobs in {jurisdiction} after controlled rounding: {target_parcels_df["EMPTOT_P"].sum()}')

        target_parcels_df.to_csv(os.path.join(self.output_dir, f'{jurisdiction}_scaled_jobs_by_parcel.csv'), index = False)
        scaled_jobs_by_TAZ_df = target_parcels_df[job_cat].groupby(taz_attr_name).sum()
        scaled_jobs_by_TAZ_df = scaled_jobs_by_TAZ_df.merge(local_jobs_by_TAZ_df[[taz_attr_name, total_job_attr_name]], on = taz_attr_name)
        scaled_jobs_by_TAZ_df.to_csv(os.path.join(self.output_dir, f'{jurisdiction}_job_comparison_by_{taz_attr_name}.csv'), index = True)

        updated_parcel_df = updated_parcel_df.loc[~updated_parcel_df['PARCELID'].isin(target_parcels_df['PARCELID'])].copy()
        target_parcels_df.drop(columns = ['PSRC_ID', taz_attr_name, 'scale'], inplace = True)
        updated_parcel_df = pd.concat([updated_parcel_df, target_parcels_df], ignore_index=True)
        updated_parcel_df = updated_parcel_df.sort_values(by = ['PARCELID'], ascending = True)

        # calculate total jobs after change
        updated_parcel_df.fillna(0, inplace=True)
        updated_parcel_df['EMPTOT_P'] = updated_parcel_df[Job_Categories].sum(axis=1)
        new_jobs = updated_parcel_df.loc[updated_parcel_df['PARCELID'].isin(target_parcels_df['PARCELID']), 'EMPTOT_P'].sum()

        df_dict = {
            "data_frame": updated_parcel_df.reset_index(),
            'local_data': local_jobs_by_TAZ_df,
            'before_change': b4_change_df,
            "local_data_provider": jurisdiction
        }

        self.updated_parcels_df = updated_parcel_df.copy() 
        # restore index
        print('jobs before change: ' + str(total_jobs_b4_scaling))
        print('              after change: ' + str(new_jobs))
        print('jobs gained ' + str(new_jobs - total_jobs_b4_scaling))        
        
        return df_dict 
