"""
Extract discharge paramaters from Confluence

This contains libraries to read from the integrator formatted
files. Adapted from functions in ReadQParams.py

"""
# Standard imports
from glob import glob
from pathlib import Path
import warnings
import os

# Third-party imports
from netCDF4 import Dataset
import numpy as np


def extract_alg(alg_dir, r_id, run_type):
    """Extracts and stores reach-level FLPE algorithm data in alg_dict.
    Parameters
    ----------
    alg_dir: Path
        path to FLPE algorithm directory
    r_id: int
        unique reach identifier
    run_type: str
        constrained or unconstrained data product indicator
    """

    non_run_type = "constrained" if run_type == "unconstrained" else "unconstrained"
    non_run_array = np.array([-9999], dtype=float)

    int_file = os.path.join(alg_dir,  f'{r_id}_integrator.nc')

    int_data = Dataset(int_file, 'r', format="NETCDF4")

    # note constrained/unconstrained not implemented yet
    alg_dict = {
        'unconstrained':{
            'MetroMan':{},
            'BAM':{},
            'HiVDI':{},
            'MOMMA':{},
            'SADS':{}
        },
        'constrained':{
            'MetroMan':{},
            'BAM':{},
            'HiVDI':{},
            'MOMMA':{},
            'SADS':{}
        }
    }

    # geobam - to be implemented
    alg_dict[run_type]['BAM'] = {
        "n" : non_run_array,
        "Abar" : non_run_array
    }
    alg_dict[non_run_type]['BAM'] = {
        "n" : non_run_array,
        "Abar" : non_run_array
    }

    # hivdi
    alg_dict[run_type]['HiVDI'] = {
        "alpha" : int_data["hivdi"]["alpha"][:].filled(np.nan),
        "beta" : int_data["hivdi"]["beta"][:].filled(np.nan),
        "Abar" : np.array(int_data["hivdi"]["Abar"][:].filled(np.nan))
    }
    alg_dict[non_run_type]['HiVDI'] = {
        "alpha" : non_run_array,
        "beta" : non_run_array,
        "Abar" : non_run_array
    }

    # momma
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        alg_dict[run_type]['MOMMA'] = {
            "B" : int_data["momma"]["B"][:].filled(np.nan),
            "H" : int_data["momma"]["H"][:].filled(np.nan),
            "Save" : np.nanmean(int_data["momma"]["Save"][:].filled(np.nan))
        }
        alg_dict[non_run_type]['MOMMA'] = {
            "B" : non_run_array,
            "H" : non_run_array,
            "Save" : non_run_array
        }

    # sad
    alg_dict[run_type]['SADS'] = {
        "n" : int_data["sad"]["n"][:].filled(np.nan),
        "Abar" : np.array(int_data["sad"]["a0"][:].filled(np.nan))
    }
    alg_dict[non_run_type]['SADS'] = {
        "n" : non_run_array,
        "Abar" : non_run_array
    }

    # metroman    
    alg_dict[run_type]['MetroMan'] = {
         "ninf" : int_data['metroman']["na"][:].filled(np.nan),
         "p" : int_data['metroman']["x1"][:].filled(np.nan),
         "Abar" : int_data['metroman']["Abar"][:].filled(np.nan)
    }
    alg_dict[non_run_type]['MetroMan'] = {
         "ninf" : non_run_array,
         "p" : non_run_array,
         "Abar" : non_run_array
    }

    int_data.close()

    return alg_dict


