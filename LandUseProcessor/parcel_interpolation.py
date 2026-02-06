from abc import ABC, abstractmethod
import logging, copy, os
import pandas as pd
from Parcels import Parcels
from utility import Job_Categories, IndentAdapter, backupScripts, dialog_level

class ParcelInterpolator(ABC):
    def __init__(self, indent):
        self.interpolated_df: pd.DataFrame | None = None
        self.output_folder: str = ''
        self.indent = indent + 1
        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger, self.indent)

    @abstractmethod
    def interpolate(self, left_Parcels, right_Parcels, horizon_year) -> Parcels:
        pass

    def export_interpolated_parcels(self, export_name: str):
        if self.interpolated_df is None:
            self.logger.error("Interpolated dataframe is not available for export.")
            raise ValueError("Interpolated dataframe is not available.")

        fn = os.path.join(self.output_folder, export_name)
        self.interpolated_df.to_csv(fn, sep = ' ', index=False)
        self.logger.info(f'Interpolated parcel data exported to: {fn}')


class LinearParcelInterpolator(ParcelInterpolator):
    def __init__(self, output_folder: str, indent):
        super().__init__(indent)
        self.output_folder = output_folder


    def interpolate(self, left_Parcels, right_Parcels, horizon_year) -> Parcels:
        """
        Step 3: Interpolate_parcel_files2.py
            Interpolate parcel files between what PSRC provided and the parcel data in the horizon year
            Very often PSRC will not have a parcel file consistent with our horizon year. Interpolation bewteen two different horizon years is unavoidable. We use
            interpolated parcel file for outside of King County or outside of BKR area. Inside BKR area, we always have our own local estimates of jobs.
            Create a new parcel file by interpolating employment bewteen two parcel files. The newly created parcel file has other non-job values
            from parcel_file_name_ealier.
        """
        self.logger.info('Linear interpolating...')
        self.logger.info(f"Left Parcel Year: {left_Parcels.data_year}, Right Parcel Year: {right_Parcels.data_year}, Horizon Year: {horizon_year}")
        self.logger.info(f"Left Parcel File: {left_Parcels.filename}")
        self.logger.info(f"Right Parcel File: {right_Parcels.filename}")

        columns = copy.copy(Job_Categories)
        columns.append('PARCELID')
        job_std = copy.copy(Job_Categories)
        job_std.extend(['STUGRD_P', 'STUHGH_P', 'STUUNI_P'])

        parcel_earlier_df = left_Parcels.original_parcels_df.copy()
        parcel_latter_df = right_Parcels.original_parcels_df.copy()
        parcel_latter_df.set_index('PARCELID', inplace = True)
        parcels_from_latter_df = parcel_latter_df.loc[:, job_std].copy(deep=True)
        parcels_from_latter_df.columns = [i + '_L' for i in parcels_from_latter_df.columns]
        parcels_from_latter_df['EMPTOT_L'] = 0
        for cat in Job_Categories:
            parcels_from_latter_df['EMPTOT_L'] = parcels_from_latter_df[cat + '_L'] + parcels_from_latter_df['EMPTOT_L']

        self.logger.info(f"Total jobs in year {right_Parcels.data_year} are {parcels_from_latter_df['EMPTOT_L'].sum():,.0f}")
        parcel_horizon_df = parcel_earlier_df.merge(parcels_from_latter_df.reset_index(), how = 'inner', left_on = 'PARCELID', right_on = 'PARCELID')

        parcel_horizon_df['EMPTOT_E'] = 0
        for cat in Job_Categories:
            parcel_horizon_df['EMPTOT_E'] = parcel_horizon_df['EMPTOT_E'] + parcel_horizon_df[cat]
        parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_E']
        self.logger.info(f"Total jobs in year {left_Parcels.data_year} are {parcel_horizon_df['EMPTOT_P'].sum():,.0f}")

        # interpolate number of jobs, and round to integer.
        for cat in job_std:
            parcel_horizon_df[cat] = parcel_horizon_df[cat] + ((horizon_year - left_Parcels.data_year) * 1.0 / (right_Parcels.data_year - left_Parcels.data_year) * (parcel_horizon_df[cat + '_L'] - parcel_horizon_df[cat])) 
            parcel_horizon_df[cat] = parcel_horizon_df[cat].round(0).astype(int)

        parcel_horizon_df['EMPTOT_P'] = 0
        for cat in Job_Categories:
            parcel_horizon_df['EMPTOT_P'] = parcel_horizon_df['EMPTOT_P'] + parcel_horizon_df[cat]

        parcel_horizon_df = parcel_horizon_df.drop([i + '_L' for i in job_std], axis = 1)
        parcel_horizon_df = parcel_horizon_df.drop(['EMPTOT_L', 'EMPTOT_E'], axis = 1)

        self.logger.info(f"After interpolation, total jobs are {parcel_horizon_df['EMPTOT_P'].sum():,.0f}")
        self.interpolated_df = parcel_horizon_df

        interpolated_fn = f'Interpolated_{horizon_year}_urbansim_parcels_from_{left_Parcels.data_year}_and {right_Parcels.data_year}_to_{horizon_year}.txt'
        self.export_interpolated_parcels(interpolated_fn)
        
        out = Parcels.from_dataframe(self.interpolated_df, horizon_year, interpolated_fn, left_Parcels.subarea_df, left_Parcels.lookup_df, self.indent + 1)
        
        return out

