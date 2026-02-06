import pandas as pd
import h5py, logging
import os, sys
from utility import *

sys.path.append(os.getcwd())

class SyntheticPopulation:
    def __init__(self, subarea_file, lookup_file, filename, data_year, log_indent = 0):
        self.data_year = data_year # year of the parcel data
        self.filename = filename # original parcel data filename

        self.lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
        self.subarea_df = pd.read_csv(subarea_file, sep = ',', low_memory = False)

        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, log_indent)
        self.indent = log_indent
        
        self.hhs_df, self.persons_df = self.load_synpop()
    
    @classmethod
    def from_dataframe(cls, subarea_df, lookup_df, filename, hhs_df, persons_df, data_year, log_indent = 0) -> "SyntheticPopulation":
        obj = cls.__new__(cls) # Create an uninitialized instance
        obj.data_year = data_year
        obj.filename = filename
        obj.lookup_df = lookup_df
        obj.subarea_df = subarea_df
        obj.hhs_df = hhs_df
        obj.persons_df = persons_df
        obj.indent = log_indent

        base_logger = logging.getLogger(__name__)
        obj.logger = IndentAdapter(base_logger, log_indent)

        return obj
        
    def load_synpop(self):
        hdf_file = h5py.File(self.filename, "r")
        hhs_df = h5_to_df(hdf_file, 'Household')
        persons_df = h5_to_df(hdf_file, 'Person')

        self.logger.info(f"Syntheic population {self.filename} loaded")
        return hhs_df, persons_df
    
    def validate_hhs(self) -> dict:
        validate_dict = validate_dataframe_file(self.hhs_df)
        return validate_dict
    
    def validate_persons(self) -> dict:
        validate_dict = validate_dataframe_file(self.persons_df)
        return validate_dict
    
    def validate_hhs_persons(self) -> dict:
        hhs_dict = self.validate_hhs()
        persons_dict = self.validate_persons()
        hhs_dict = {f"hhs_{k}": v for k, v in hhs_dict.items()}
        persons_dict = {f'persons_{k}': v for k, v in persons_dict.items()}
        out = hhs_dict | persons_dict
        return out

    def summarize_synpop(self, output_dir, output_fn_prefix = '', export_parcel_level_summary = False, export_parcel_level_dataset = False) -> dict:        
        workers_df = self.persons_df[['hhno', 'pwtyp', 'psexpfac']].copy()
        workers_df['ft_w'] = 0
        workers_df['pt_w'] = 0
        workers_df.loc[workers_df['pwtyp'] == 1, 'ft_w'] = 1
        workers_df.loc[workers_df['pwtyp'] == 2, 'pt_w'] = 1
        workers_by_hhs_df = workers_df.groupby('hhno').sum().reset_index()

        hh_df = self.hhs_df.merge(workers_by_hhs_df, on = 'hhno', how = 'left')
        taz_subarea = self.subarea_df.copy()
        taz_subarea.set_index('BKRCastTAZ', inplace = True)
        
        hh_taz = hh_df.join(taz_subarea, on = 'hhtaz')
        hh_taz['total_persons'] = hh_taz['hhexpfac'] * hh_taz['hhsize']
        hh_taz['total_hhs'] = hh_taz['hhexpfac']

        summary_by_jurisdiction = hh_taz.groupby('Jurisdiction')[['total_hhs', 'total_persons', 'ft_w', 'pt_w']].sum()   
        summary_by_mma = hh_taz.groupby('Subarea')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()

        subarea_def = taz_subarea[['Subarea', 'SubareaName']]
        subarea_def = subarea_def.drop_duplicates(keep = 'first')
        subarea_def.set_index('Subarea', inplace = True)
        summary_by_mma = summary_by_mma.join(subarea_def)
        summary_by_taz = hh_taz.groupby('hhtaz')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()

        self.logger.info('summarize...')
        if output_fn_prefix == '':
            fn_summary_by_juris = f'{self.data_year}_synpop_summary_by_jurisdiction.csv'
            fn_summary_by_taz = f'{self.data_year}_synpop_summary_by_taz.csv'
            fn_summary_by_subarea = f'{self.data_year}_synpop_summary_by_subarea.csv'
            fn_summary_by_geoid10 = f'{self.data_year}_synpop_summary_by_geoid10.csv'
            fn_summary_by_parcel = f'{self.data_year}_synpop_summary_by_parcel.csv'
            fn_hhs = f'{self.data_year}_synpop_households.csv'
            fn_persons = f'{self.data_year}_synpop_persons.csv'
        else:
            fn_summary_by_juris = f'{self.data_year}_{output_fn_prefix}_synpop_summary_by_jurisdiction.csv'
            fn_summary_by_taz = f'{self.data_year}_{output_fn_prefix}_synpop_summary_by_taz.csv'
            fn_summary_by_subarea = f'{self.data_year}_{output_fn_prefix}_synpop_summary_by_subarea.csv'
            fn_summary_by_geoid10 = f'{self.data_year}_{output_fn_prefix}_synpop_summary_by_geoid10.csv'
            fn_summary_by_parcel = f'{self.data_year}_{output_fn_prefix}_synpop_summary_by_parcel.csv'
            fn_hhs = f'{self.data_year}_{output_fn_prefix}_synpop_households.csv'
            fn_persons = f'{self.data_year}_{output_fn_prefix}_synpop_persons.csv'

        summary_by_jurisdiction.to_csv(os.path.join(output_dir, fn_summary_by_juris), header = True)
        summary_by_mma.to_csv(os.path.join(output_dir, fn_summary_by_subarea), header = True)
        summary_by_taz.to_csv(os.path.join(output_dir, fn_summary_by_taz), header = True)
        parcel_df = self.lookup_df
        hh_taz = hh_taz.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        summary_by_geoid10 = hh_taz.groupby('GEOID10')[['total_hhs', 'total_persons',  'ft_w', 'pt_w']].sum()
        summary_by_geoid10.to_csv(os.path.join(output_dir, fn_summary_by_geoid10), header = True)

        self.logger.info(f'Summary by jurisdiction is saved to {fn_summary_by_juris}')
        self.logger.info(f'Summary by taz is saved to {fn_summary_by_taz}')
        self.logger.info(f'Summary by subarea is saved to {fn_summary_by_subarea}')
        self.logger.info(f'Summary by geoid10 is saved to {fn_summary_by_geoid10}')

        agg_dict = {'total_hhs': 'sum', 'total_persons': 'sum'}
        summary_by_parcels = hh_taz.groupby('hhparcel').agg(agg_dict)
        summary_by_parcels = summary_by_parcels.merge(parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ', 'Jurisdiction']], how = 'right', left_on = 'hhparcel', right_on = 'PSRC_ID')
        summary_by_parcels.fillna(0, inplace = True)
        summary_by_parcels.rename(columns = {'total_hhs': 'total_hhs_by_parcel', 'total_persons': 'total_persons_by_parcel'}, inplace = True)
        
        if export_parcel_level_summary == True:
            summary_by_parcels.to_csv(os.path.join(output_dir, fn_summary_by_parcel), index = False, header = True)
            self.logger.info(f'Summaru by parcel is saved to {fn_summary_by_parcel}')

        if export_parcel_level_dataset == True:
            hh_df.to_csv(os.path.join(output_dir, fn_hhs), header = True)
            self.logger.info(f'Synthetic households exported to {fn_hhs}')
            self.persons_df.to_csv(os.path.join(output_dir, fn_persons), header = True)   
            self.logger.info(f'Synthetic persons exported to {fn_persons}')  

        summary_outputs = {
            'summary_by_jurisdiction': summary_by_jurisdiction.reset_index(),
            'summary_by_subarea': summary_by_mma.reset_index(),
            'summary_by_taz': summary_by_taz.reset_index(),
            'summary_by_geoid10': summary_by_geoid10.reset_index(),
            'summary_by_parcel': summary_by_parcels.reset_index()
        }   

        return summary_outputs
            
    def adjust_worker_status_for_WFH(self, wfh_rate_file, output_h5_file):
        '''
        create a new popsim h5 for WFH modeling in COB method, by converting an assumed % of workers to non-worker status
        
        :param wfh_rate_file: % WFH rate for each TAZ
        :param output_h5_file: output file name for the new popsim h5
        '''
        self.logger.info(f'COB WFH methodology. Convert workers from worker status to non-worker status.')
        self.logger.info(f'WFH rate: {wfh_rate_file}')

        output_dir = os.path.dirname(self.filename)

        person_df = self.persons_df.copy()
        hhs_df = self.hhs_df.copy()


        person_df = person_df.merge(hhs_df[['hhno', 'hhtaz', 'hhparcel']], on='hhno', how='left')
        rate_df = pd.read_csv(wfh_rate_file)
        rate_df = rate_df.rename(columns={'BKRCastTAZ': 'hhtaz', 'WorkerAdjFactor': 'rate'})

        # attach rate to every person
        person_df = person_df.merge(rate_df[['hhtaz', 'rate']], on='hhtaz', how='left')
        person_df['rate'] = person_df['rate'].fillna(0)

        # -------------------------------------------------
        # Vectorized selection
        # -------------------------------------------------
        rng = np.random.default_rng(1) # generate random number 0..1 for every person
        workers_mask = person_df['pwtyp'] > 0
        rand = rng.random(len(person_df))
        convert_mask = workers_mask & (rand < person_df['rate'])
        total_adjusted = convert_mask.sum()

        # adjust workers status based on selection
        person_df.loc[convert_mask, 'pwtyp'] = 0
        person_df.loc[convert_mask & person_df['pptyp'].isin([1, 2]),'pptyp'] = 0

        total_workers_before = (person_df['pwtyp'] > 0).sum() + total_adjusted
        total_workers_after = (person_df['pwtyp'] > 0).sum()

        self.logger.info(f'{total_workers_before} workers before the change.')
        self.logger.info(f'{total_workers_after} workers after the change.')
        self.logger.info(f'{total_adjusted} workers changed.')

        # Save converted list
        converted_file = 'converted_non_workers.csv'
        converted_df = person_df.loc[convert_mask,['hhno', 'pno', 'hhtaz', 'hhparcel']]
        converted_df.to_csv(os.path.join(output_dir, converted_file), index=False)
        self.logger.info(f'workers converted to non-worker status are saved in {converted_file}')

        person_df.drop(columns=['rate', 'hhtaz', 'hhparcel'], inplace=True)

        with h5py.File(os.path.join(output_dir, output_h5_file), 'w') as f:
            df_to_h5(hhs_df, f, 'Household')
            df_to_h5(person_df, f, 'Person')

        self.logger.info(f'updated synthetic population for WFH is saved in {output_h5_file} ')