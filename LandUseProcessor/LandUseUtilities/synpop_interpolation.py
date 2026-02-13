from abc import ABC, abstractmethod
import logging, copy, os
import pandas as pd
from LandUseUtilities.Parcels import Parcels
from utility import Job_Categories, IndentAdapter, backupScripts, dialog_level, h5_to_df, df_to_h5
import h5py
from LandUseUtilities.synthetic_population import SyntheticPopulation

class synpop_interpolation:
    def __init__(self, block_group_file : str, indent):
        self.interpolated_hhs_df: pd.DataFrame | None = None
        self.interpolated_persons_df: pd.DataFrame | None = None
        self.output_folder: str = ''
        self.block_group_file = block_group_file
        self.indent = indent
        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, self.indent)

    @abstractmethod
    def interpolate(self, left_synpop : SyntheticPopulation, right_synpop : SyntheticPopulation, horizon_year) -> SyntheticPopulation:
        pass

    def export_interpolated_synpop(self, export_name: str):
        if (self.interpolated_hhs_df is None) or (self.interpolated_persons_df is None) :
            self.logger.error("Interpolated synthetic population dataframe is not available for export.")
            raise ValueError("Interpolated synthetic population dataframe is not available.")

        fn = os.path.join(self.output_folder, export_name)
        with h5py.File(fn, 'w') as output_h5_file: 
            df_to_h5(self.interpolated_hhs_df, output_h5_file, 'Household')
            df_to_h5(self.interpolated_persons_df, output_h5_file, 'Person')

        self.logger.info(f'Interpolated synthetic population data exported to: {fn}')

class LinearSynPopInterpolator(synpop_interpolation):
    def __init__(self, output_folder: str, block_group_file: str, indent):
        super().__init__(block_group_file, indent)
        self.output_folder = output_folder

    def interpolate(self, left_synpop : SyntheticPopulation, right_synpop : SyntheticPopulation, horizon_year) -> SyntheticPopulation:
        base_hh_df = left_synpop.hhs_df.copy()
        future_hh_df = right_synpop.hhs_df.copy()
        
        future_hh_df['future_total_persons'] = future_hh_df['hhexpfac'] * future_hh_df['hhsize']
        future_hh_df['future_total_hhs'] = future_hh_df['hhexpfac']

        base_hh_df['base_total_persons'] = base_hh_df['hhexpfac'] * base_hh_df['hhsize']
        base_hh_df['base_total_hhs'] = base_hh_df['hhexpfac']

        parcel_df = left_synpop.lookup_df
        future_hh_df = future_hh_df.merge(parcel_df[['PSRC_ID', 'GEOID10', 'BKRCastTAZ']], how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        future_hhs_by_geoid10 = future_hh_df.groupby('GEOID10')[['future_total_hhs', 'future_total_persons']].sum()
        base_hh_df = base_hh_df.merge(parcel_df, how = 'left', left_on = 'hhparcel', right_on = 'PSRC_ID')
        base_hhs_by_geoid10 = base_hh_df.groupby('GEOID10')[['base_total_hhs', 'base_total_persons']].sum()

        self.logger.info('Future total hhs: ' + str(future_hh_df['future_total_hhs'].sum()))
        self.logger.info('Future total persons: '  + str(future_hh_df['future_total_persons'].sum()))
        self.logger.info('Base total hhs: ' + str(base_hh_df['base_total_hhs'].sum()))
        self.logger.info('Base total persons: ' + str(base_hh_df['base_total_persons'].sum()))

        ofm_df = pd.read_csv(self.block_group_file)
        ofm_df = ofm_df.merge(future_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)
        ofm_df = ofm_df.merge(base_hhs_by_geoid10, how = 'left', left_on = 'GEOID10', right_index = True)

        if horizon_year <= right_synpop.data_year and horizon_year >= left_synpop.data_year:
            # right between the bookends.
            self.logger.info('interpolating...')
        else:
            self.logger.info('extropolating...')
            
        ofm_df.fillna(0, inplace = True)
        ratio = (horizon_year - left_synpop.data_year) * 1.0 / (right_synpop.data_year - left_synpop.data_year)
        ofm_df['OFM_groupquarters'] = 0
        ofm_df['OFM_hhs'] = ((ofm_df['future_total_hhs'] - ofm_df['base_total_hhs']) * ratio + ofm_df['base_total_hhs']).round(0)
        ofm_df['OFM_persons'] = ((ofm_df['future_total_persons'] - ofm_df['base_total_persons']) * ratio + ofm_df['base_total_persons']).round(0)

        self.logger.info('Interpolated total hhs: ' + str(ofm_df['OFM_hhs'].sum()))
        self.logger.info('Interpolated total persons: ' + str(ofm_df['OFM_persons'].sum()))
        fn_total_by_geoid10 = f'{horizon_year}_interpolation_from_{left_synpop.data_year}_{right_synpop.data_year}_by_GEOID.csv'
        
        ofm_df[['GEOID10', 'OFM_groupquarters', 'OFM_hhs', 'OFM_persons']].to_csv(os.path.join(self.output_folder, fn_total_by_geoid10), index = False)
        self.logger.info(f'interpolation by geoid10 is saved in {fn_total_by_geoid10}')

        base_hhs_by_parcel = base_hh_df[['PSRC_ID', 'base_total_hhs', 'base_total_persons']].groupby('PSRC_ID').sum()
        future_hhs_by_parcel  = future_hh_df[['PSRC_ID', 'future_total_hhs', 'future_total_persons']].groupby('PSRC_ID').sum()
        target_hhs_by_parcel = pd.merge(base_hhs_by_parcel, future_hhs_by_parcel, on = 'PSRC_ID', how = 'outer')
        target_hhs_by_parcel.fillna(0, inplace = True)
        target_hhs_by_parcel['total_hhs_by_parcel'] = target_hhs_by_parcel['base_total_hhs'] + (target_hhs_by_parcel['future_total_hhs'] - target_hhs_by_parcel['base_total_hhs']) * ratio
        target_hhs_by_parcel['total_persons_by_parcel'] = target_hhs_by_parcel['base_total_persons'] + (target_hhs_by_parcel['future_total_persons'] - target_hhs_by_parcel['base_total_persons']) * ratio
        target_hhs_by_parcel.drop(['base_total_hhs', 'base_total_persons', 'future_total_hhs', 'future_total_persons'], axis = 1, inplace = True)
        target_hhs_by_parcel.reset_index(inplace = True)
        target_hhs_by_parcel = parcel_df[['PSRC_ID', 'Jurisdiction', 'BKRCastTAZ', 'GEOID10']].merge(target_hhs_by_parcel[['PSRC_ID', 'total_hhs_by_parcel', 'total_persons_by_parcel']], on = 'PSRC_ID', how = 'left')
        target_hhs_by_parcel.fillna(0, inplace= True)

        hhs_by_parcel_filename = f'{horizon_year}_hhs_by_parcels_from_{left_synpop.data_year}_{right_synpop.data_year}.csv'
        target_hhs_by_parcel.to_csv(os.path.join(self.output_folder, hhs_by_parcel_filename), index = False)
        self.logger.info(f'interpolation by parcel is saved in {hhs_by_parcel_filename}')

        target_hhs_by_taz = target_hhs_by_parcel[['BKRCastTAZ', 'total_hhs_by_parcel']].groupby('BKRCastTAZ').sum().reset_index()
        target_hhs_by_taz = target_hhs_by_taz.loc[target_hhs_by_taz['total_hhs_by_parcel'] > 0]
        future_hh_df.drop(['PSRC_ID', 'future_total_persons', 'future_total_hhs', 'GEOID10', 'BKRCastTAZ'], axis = 1, inplace=True)
        target_hhs_df = pd.DataFrame()

        for taz in target_hhs_by_taz['BKRCastTAZ'].tolist():
            hhs_in_taz = future_hh_df.loc[future_hh_df['hhtaz'] == taz]
            num_hhs_popsim = hhs_in_taz['hhexpfac'].sum()
            num_hhs = int(target_hhs_by_taz.loc[target_hhs_by_taz['BKRCastTAZ'] == taz, 'total_hhs_by_parcel'].tolist()[0])
            if num_hhs_popsim > num_hhs:
                target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs)])
            else:
                target_hhs_df = pd.concat([target_hhs_df, hhs_in_taz.sample(n = num_hhs_popsim)])
        self.logger.info(f"total hhs after interpolation: {target_hhs_df['hhexpfac'].sum()}")

        future_persons_df = right_synpop.persons_df
        target_persons_df = future_persons_df.loc[future_persons_df['hhno'].isin(target_hhs_df['hhno'])]
        self.logger.info(f"total persons after interpolation: {target_persons_df['psexpfac'].sum()}")

        avg_person_per_hhs_df = target_hhs_by_parcel[['Jurisdiction', 'total_hhs_by_parcel', 'total_persons_by_parcel']].groupby('Jurisdiction').sum()
        avg_person_per_hhs_df['avg_persons_per_hh'] = avg_person_per_hhs_df['total_persons_by_parcel'] / avg_person_per_hhs_df['total_hhs_by_parcel']

        fn_avg_hhsize = f'{horizon_year}_interpolation_average_hhsize_by_jurisdiction.csv'
        avg_person_per_hhs_df.to_csv(os.path.join(self.output_folder, fn_avg_hhsize))
        self.logger.info(f'Average household size is saved to {fn_avg_hhsize}')

        final_output_pop_file = f'{horizon_year}_interpolated_hhs_and_persons_from_{left_synpop.data_year}_{right_synpop.data_year}.h5'
        self.interpolated_hhs_df = target_hhs_df
        self.interpolated_persons_df = target_persons_df
        
        self.export_interpolated_synpop(final_output_pop_file)

        self.logger.info(f'interpolated synthetic population is saved in {final_output_pop_file}')

        out = SyntheticPopulation.from_dataframe(left_synpop.subarea_df, left_synpop.lookup_df, final_output_pop_file, self.interpolated_hhs_df, self.interpolated_persons_df, horizon_year, self.indent)
        
        return out