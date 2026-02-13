import h5py
import sys, os
import numpy as np
import pandas as pd
from enum import Enum
import logging
from datetime import datetime
from PyQt6.QtCore import Qt, QThread, pyqtSignal


#2/3/2022
# upgrade to python 3.7

_LOGGING_CONFIGURED = False

SynPopAssumptions = {
    "Bellevue":  {"sfhhsize": 2.82, "mfhhsize": 2.03, "sfhh_occ": 0.952, "mfhh_occ": 0.895},
    "Kirkland":  {"sfhhsize": 2.82, "mfhhsize": 2.03, "sfhh_occ": 0.952, "mfhh_occ": 0.895},
    "Redmond":   {"sfhhsize": 2.82, "mfhhsize": 2.03, "sfhh_occ": 0.952, "mfhh_occ": 0.895},
}

Job_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPRSC_P', 'EMPSVC_P']
Summary_Categories = ['EMPEDU_P', 'EMPFOO_P', 'EMPGOV_P', 'EMPIND_P', 'EMPMED_P', 'EMPOFC_P', 'EMPOTH_P', 'EMPRET_P', 'EMPSVC_P', 'EMPTOT_P', 'STUGRD_P', 'STUHGH_P', 'STUUNI_P', 'HH_P']

job_rename_dict = {'JOBS_EDU':'EMPEDU_P', 'JOBS_FOOD':'EMPFOO_P', 'JOBS_GOV':'EMPGOV_P', 'JOBS_IND':'EMPIND_P',
    'JOBS_MED':'EMPMED_P', 'JOBS_OFF':'EMPOFC_P', 'JOBS_RET':'EMPRET_P', 'JOBS_RSV':'EMPRSC_P', 'JOBS_SERV':'EMPSVC_P', 'JOBS_OTH':'EMPOTH_P',
    'JOBS_TOTAL':'EMPTOT_P'}
sqft_rename_dict = {'SQFT_EDU':'SQFT_EDU', 'SQFT_FOOD':'SQFT_FOO','SQFT_GOV':'SQFT_GOV','SQFT_IND':'SQFT_IND','SQFT_MED':'SQFT_MED', 'SQFT_OFF':'SQFT_OFC',
    'SQFT_RET':'SQFT_RET', 'SQFT_RSV':'SQFT_RSV', 'SQFT_SERV':'SQFT_SVC', 'SQFT_OTH': 'SQFT_OTH', 'SQFT_NONE':'SQFT_NON', 
    'SQFT_TOTAL':'SQFT_TOT'}
du_rename_dict = {'UNITS_SF':'SFUnits', 'UNITS_MF':'MFUnits'}

class Parcel_Data_Format(Enum):
    Processed_Parcel_Data = "Processed_Parcel_Data"
    BKRCastTAZ_Format = "BKRCastTAZ_Format"
    BKR_Trip_Model_TAZ_Format = "BKR_Trip_Model_TAZ_Format"
class Data_Scale_Method(Enum):
    Keep_the_Data_from_the_Partner_City = "Keep_the_Data_from_the_Partner_City"
    Scale_by_Job_Category = "Scale_by_Job_Category"
    Scale_by_Total_Jobs_by_TAZ = "Scale_by_Total_Jobs_by_TAZ"
class SynPop_Data_Scale_Method(Enum):
    Keep_the_Data_from_the_Partner_City = "Keep_the_Data_from_the_Partner_City"
    Scale_by_Total_Hhs_by_TAZ = "Scale_by_Total_Hhs_by_TAZ"

def h5_to_df(h5_file, group_name):
    """
    Converts the arrays in a H5 store to a Pandas DataFrame. 
    """
    col_dict = {}
    h5_set = h5_file[group_name]
    for col in h5_set.keys():
        my_array = np.asarray(h5_set[col])
        col_dict[col] = my_array
    df = pd.DataFrame(col_dict)
    return df

def df_to_h5(df, h5_store, group_name):
    """
    Stores DataFrame series as indivdual to arrays in an h5 container. 
    """
    # delete store store if exists   
    if group_name in h5_store:
        del h5_store[group_name]
    
    my_group = h5_store.create_group(group_name)

    for col in df.columns:
        data = df[col].to_numpy()
        if np.issubdtype(data.dtype, np.integer):
            dtype = 'int32' if data.dtype.itemsize <= 4 else 'int64'
        elif np.issubdtype(data.dtype, np.floating):
            dtype = 'float32'
        else:
            dtype = None # let h5py decide

        my_group.create_dataset(col, data=data, dtype=dtype, compression = 'gzip')

def backupScripts(source, dest):
    import os
    import shutil
    shutil.copyfile(source, dest)

def setup_logger_file(output_dir, log_name = "parcel_processing.log") -> logging.Logger:
    global _LOGGING_CONFIGURED 
    if _LOGGING_CONFIGURED:
        base_logger = logging.getLogger(__name__)
        logger = IndentAdapter(base_logger, indent = 0)
        return logger

    log_filename = os.path.join(output_dir, log_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filename, mode = 'w')

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    _LOGGING_CONFIGURED = True

    base_logger = logging.getLogger(__name__)
    logger = IndentAdapter(base_logger)   
    logger.info(
        "Logging initialized at %s", log_filename
    )  
    return logger

class IndentAdapter(logging.LoggerAdapter):
    def __init__(self, logger, indent=0):
        super().__init__(logger, {})
        self.indent = indent

    def process(self, msg, kwargs):
        prefix = "   " * self.indent
        return f"{prefix}{msg}", kwargs

def get_logger() -> logging.Logger:
    if not _LOGGING_CONFIGURED:
        raise RuntimeError("Logger not configured. Please call setup_logger_file first.")
    base_logger = logging.getLogger(__name__)
    logger = IndentAdapter(base_logger)
    return logging.getLogger(__name__)

def dialog_level(widget):
    level = 0
    while widget.parentWidget():
        widget = widget.parentWidget()
        level += 1
    return level

def controlled_rounding(data_df, attr_name, control_total, index_attr_name):
    # find residential parcels within taz     
    updated_data_df = data_df.loc[data_df[attr_name] > 0].copy()
    total_rows = updated_data_df.shape[0]
    if total_rows != 0:
        already_assigned = updated_data_df[attr_name].sum()
    else:
        already_assigned = 0
    
    # how many need to be assigned or removed to match the control total
    diff = int(control_total - already_assigned)
    if (diff == 0) | total_rows == 0:
        return data_df

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
    
    new_data_df = data_df.copy()
    new_data_df = new_data_df.loc[~new_data_df[index_attr_name].isin(updated_data_df[index_attr_name])]
    new_data_df = pd.concat([new_data_df, updated_data_df])
    
    return new_data_df
 
def validate_dataframe_file(dataframe: pd.DataFrame) -> dict:
    import debugpy
    debugpy.breakpoint()

    validation_dict = {}  
    output_list = []
    header = ["Column", "Data Type", "Unique Values", "Missing Values", "Duplicated", "Min", "Max", "Mean"]

    for col in dataframe.columns:
        series = dataframe[col]
        unique_non_null = series.nunique(dropna = True)
        missing = series.isna().sum()
        duplicates = len(series) - unique_non_null - missing
        is_numeric = pd.api.types.is_numeric_dtype(series)
        min = series.min() if is_numeric else ""
        max = series.max() if is_numeric else ""
        mean = series.mean() if is_numeric else ""

        outputs = {
            "Column": col,
            "Data Type": str(series.dtype),
            "Unique Values": unique_non_null,
            "Missing Values": missing,
            "Duplicated": duplicates,
            "Min": min,
            "Max": max,
            "Mean": mean
        }

        output_list.append(outputs)

    # df: validation of data_df
    df = pd.DataFrame(output_list, columns = header)

    # df2: data_df shape
    df2 = pd.DataFrame([{"Rows": dataframe.shape[0], "Columns": dataframe.shape[1]}])

    df3 = dataframe.head(100)   

    validation_dict = {
        "Validation": df,
        "Summary": df2,
        "Raw Data Samples": df3
    }

    return validation_dict

class ThreadWrapper(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(object)
    status_update = pyqtSignal(str, str, str, str) #status bar section 1 ~ 4

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
        base_logger = logging.getLogger(__name__)
        self.logger = IndentAdapter(base_logger)

    def run(self):
        try:
            ret = None
            ret = self.func(*self.args, **self.kwargs)
        except Exception as e:
            self.error.emit(e)
            self.logger.error("Exception in thread: ", exc_info=True)
            return
         
        self.finished.emit(ret)