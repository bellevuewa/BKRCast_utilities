import os, sys
sys.path.append(os.getcwd())
import h5py
import logging
import pandas as pd
from utility import (IndentAdapter, Summary_Categories, validate_dataframe_file, h5_to_df
        )

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

class Parcels:
    def __init__(self, subarea_file, lookup_file, filename, data_year, log_indent = 0):
        """
        Initialize the class with data.
        """
        self.data_year = data_year # year of the parcel data
        self.filename = filename # original parcel data filename

        self.lookup_df = pd.read_csv(lookup_file, sep = ',', low_memory = False)
        self.subarea_df = pd.read_csv(subarea_file, sep = ',', low_memory = False)

        self.original_parcels_df = pd.read_csv(filename, sep = ' ', low_memory = False)

        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, log_indent)
        self.indent = log_indent

    def copy(self) -> "Parcels":
        """for deep copy"""
        return Parcels.from_dataframe(self.original_parcels_df, self.data_year, self.filename, self.subarea_df, self.indent)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, data_year: int, filename: str, subarea_df: pd.DataFrame, lookup_df: pd.DataFrame, log_indent = 0) -> "Parcels":
        """Initialize Parcels from a DataFrame. a python way to have multiple constructors."""
        obj = cls.__new__(cls) # Create an uninitialized instance
        obj.original_parcels_df = df.copy()
        obj.data_year = data_year
        obj.filename = filename
        obj.lookup_df = lookup_df.copy()
        obj.subarea_df = subarea_df.copy()
        base_logger = logging.getLogger(__name__)
        obj.logger = IndentAdapter(base_logger, log_indent)
        return obj


    def summarize_parcel_data(self, output_dir, output_fn_prefix = '') -> dict:
        '''
        summarize parcel data
        
        :param output_dir: output folder
        :param output_fn_prefix: additional prefix for output file name
        :return: a dict of summary by jurisdiction, TAZ, and subarea
            summary_dict = {
                "Jurisdiction": summary_jurisdictions,
                "Subarea": summary_subarea,
                "TAZ": summary_taz
            }
        '''
        parcel_df = self.original_parcels_df.merge(self.subarea_df[['BKRCastTAZ', 'Jurisdiction', 'Subarea']], left_on="TAZ_P", right_on = "BKRCastTAZ", how="left")
        summary_jurisdictions = parcel_df.groupby('Jurisdiction')[Summary_Categories].sum().reset_index()
        summary_taz = parcel_df.groupby('TAZ_P')[Summary_Categories].sum().reset_index()
        summary_subarea = parcel_df.groupby('Subarea')[Summary_Categories].sum().reset_index()
        summary_subarea = summary_subarea.merge(self.subarea_df[['Subarea', 'SubareaName']].drop_duplicates(), on='Subarea', how='left')

        if output_dir is None:
            output_dir = os.getcwd()

        juris_name = 'parel_summary_by_jurisdiction.csv'
        taz_name = 'parcel_summary_by_taz.csv'
        subarea_name = 'parcel_summary_by_subarea.csv'

        if output_fn_prefix != '': 
            juris_name = output_fn_prefix + '_' + juris_name
            taz_name = output_fn_prefix + '_' + taz_name
            subarea_name = output_fn_prefix + '_' + subarea_name
        # the exported files could overwrite other parcel summary files. need to think about better naming convention later.
        summary_jurisdictions.to_csv(os.path.join(output_dir, juris_name), index=False)
        summary_taz.to_csv(os.path.join(output_dir, taz_name), index=False)
        summary_subarea.to_csv(os.path.join(output_dir, subarea_name), index=False)

        self.logger.info(f'Parcel summary by jurisdiction exported to: {juris_name}')
        self.logger.info(f'Parcel summary by TAZ exported to: {taz_name}')
        self.logger.info(f'Parcel summary by subarea exported to: {subarea_name}')

        summary_dict = {
            "Jurisdiction": summary_jurisdictions,
            "Subarea": summary_subarea,
            "TAZ": summary_taz
        }

        return summary_dict

    def validate_parcel_file(self) -> dict:
        import debugpy
        debugpy.breakpoint()

        validation_dict = validate_dataframe_file(self.original_parcels_df)

        return validation_dict

    def sync_with_synthetic_population(self, popsim_filename) -> pd.DataFrame:
        '''
        pass the total households by parcel from the synthetic population file to the parcel file
        
        :param popsim_filename: synthetic population filename
        :return: a parcel dataframe with the updated number of households, consistent with the synthetic population.
        '''
        with h5py.File(popsim_filename, "r") as hdf_file:
            hh_df = h5_to_df(hdf_file, 'Household')

        output_dir = os.path.dirname(self.filename)
        name, ext = os.path.splitext(os.path.basename(self.filename))
        output_parcel_file = f'{name}_sync_with_synpop{ext}'

        hhs = hh_df.groupby('hhparcel')[['hhexpfac', 'hhsize']].sum().reset_index()
        parcel_df = self.original_parcels_df.copy()
        parcel_df = parcel_df.merge(hhs, how = 'left', left_on = 'PARCELID', right_on = 'hhparcel')

        parcel_df['HH_P']  = 0
        parcel_df['HH_P'] = parcel_df['hhexpfac']
        parcel_df.fillna(0, inplace = True)
        parcel_df.drop(['hhexpfac', 'hhsize', 'hhparcel'], axis = 1, inplace = True)
        parcel_df['HH_P'] = parcel_df['HH_P'].round(0).astype(int)

        parcel_df.to_csv(os.path.join(output_dir, output_parcel_file), sep = ' ', index = False) 
        self.logger.info(f'Synthetic population file {popsim_filename} synced with the parcel file {self.filename}')
        self.logger.info(f'The final parcel file is saved in {output_parcel_file}.')

        return parcel_df