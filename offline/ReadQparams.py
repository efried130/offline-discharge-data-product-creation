"""
Extract discharge paramaters from Confluence
"""
# Standard imports
from glob import glob
from pathlib import Path
import warnings

# Third-party imports
from netCDF4 import Dataset
import numpy as np


def extract_alg(alg_dir, r_id):
    """Extracts and stores reach-level FLPE algorithm data in alg_dict.
    TODO: 
    - Add in SAD results once available.
    """
    
    gb_file = Path(alg_dir + '/geobam/' + f'{r_id}_geobam.nc')
    hv_file = Path(alg_dir + '/hivdi/' + f'{r_id}_hivdi.nc')
    mo_file = Path(alg_dir + '/momma/' + f'{r_id}_momma.nc')
    sd_file = Path(alg_dir + '/sad/' + f'{r_id}_sad.nc')      ## TODO wait on SAD results
    mm_file = glob(str(alg_dir + '/metroman/' + f'*{r_id}*_metroman.nc'))    
    mm_file = Path(mm_file[0]) 

    if gb_file.exists() and hv_file.exists() and mm_file.exists() and mo_file.exists():    ## TODO add in SAD files

        param_dict = extract_valid(r_id, gb_file, hv_file, mo_file, sd_file, mm_file)
    else:
        param_dict = indicate_no_data(r_id)
        
    return param_dict


def extract_valid(r_id, gb_file, hv_file, mo_file, sd_file, mm_file):
    """ Extract valid data from the output of each reach-level FLPE alg.
    Parameters
    ----------
    r_id: str
        Unique reach identifier
    gb_file: Path
        Path to geoBAM results file
    hv_file: Path
        Path to HiVDI results file
    mo_file: Path
        Path to MOMMA results file
    sd_file: Path
        Path to SAD results file
    mm_file: Path
        Path to MetroMan results file
    """
   
    # note constrained/unconstrained not implemented yet
    alg_dict = {'discharge_models': 
                             {'unconstrained':{'MetroMan':{}, 
                                               'BAM':{}, 
                                               'HiVDI':{}, 
                                               'MOMMA':{},
                                               'SADS':{}}, 
                              'constrained':{'MetroMan':{}, 
                                             'BAM':{}, 
                                             'HiVDI':{}, 
                                             'MOMMA':{},
                                             'SADS':{}} }}

    # geobam
    gb = Dataset(gb_file, 'r', format="NETCDF4")
    alg_dict['discharge_models']['unconstrained']['BAM'] = {
        "n": np.nanmean(np.array(get_gb_data(gb, "logn_man", True))),
        "Abar": np.nanmean(np.array(get_gb_data(gb, "A0", False)))
    }
    alg_dict['discharge_models']['constrained']['BAM'] = {
        "n": np.nanmean(np.array(get_gb_data(gb, "logn_man", True))),
        "Abar": np.nanmean(np.array(get_gb_data(gb, "A0", False)))
    }
    gb.close()

    # hivdi
    hv = Dataset(hv_file, 'r', format="NETCDF4")
    alg_dict['discharge_models']['unconstrained']['HiVDI'] = {
        "alpha" : hv["reach"]["alpha"][:].filled(np.nan),  
        "beta" : hv["reach"]["beta"][:].filled(np.nan),  
        "Abar" : hv["reach"]["A0"][:].filled(np.nan)
    }
    alg_dict['discharge_models']['constrained']['HiVDI'] = {
        "alpha" : hv["reach"]["alpha"][:].filled(np.nan),  
        "beta" : hv["reach"]["beta"][:].filled(np.nan),  
        "Abar" : hv["reach"]["A0"][:].filled(np.nan)
    }
    hv.close()

    # momma
    mo = Dataset(mo_file, 'r', format="NETCDF4")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        alg_dict['discharge_models']['unconstrained']['MOMMA'] = {
            "B" : mo["zero_flow_stage"][:].filled(np.nan),
            "H" : mo["bankfull_stage"][:].filled(np.nan),                                  
            "Save" : np.nanmean(mo["slope"][:].filled(np.nan))
        }
        alg_dict['discharge_models']['constrained']['MOMMA'] = {
            "B" : mo["zero_flow_stage"][:].filled(np.nan),
            "H" : mo["bankfull_stage"][:].filled(np.nan),                                  
            "Save" : np.nanmean(mo["slope"][:].filled(np.nan))
        }
    mo.close()

    # sad ugly way to make discharge.py run
    # sd = Dataset(sd_file, 'r', format="NETCDF4")
    # alg_dict['discharge_models']['unconstrained']['SADS'] = {
    #     "n" : sd["n"][:].filled(np.nan),
    #     "Abar" : sd["A0"][:].filled(np.nan)
    # }
    # sd.close()
    alg_dict['discharge_models']['unconstrained']['SADS'] = {
        "n" : np.nan,
        "Abar" : np.nan
    }
    alg_dict['discharge_models']['constrained']['SADS'] = {
        "n" : np.nan,
        "Abar" : np.nan
    }
    
    
    # metroman    
    mm = Dataset(mm_file, 'r', format="NETCDF4")
    index = np.where(mm["reach_id"][:] == int(r_id))
    alg_dict['discharge_models']['unconstrained']['MetroMan'] = {
         "ninf" : mm["nahat"][index].filled(np.nan),
         "p" : mm["x1hat"][index].filled(np.nan),
         "Abar" : mm["A0hat"][index].filled(np.nan)
    }
    alg_dict['discharge_models']['constrained']['MetroMan'] = {
         "ninf" : mm["nahat"][index].filled(np.nan),
         "p" : mm["x1hat"][index].filled(np.nan),
         "Abar" : mm["A0hat"][index].filled(np.nan)
    }
    mm.close()
    
    return alg_dict
    
def indicate_no_data(r_id):
    """Indicate no data is available for the reach.
    TODO: Metroman results
    Parameters
    ----------
    r_id: str
        Unique reach identifier
    """
    alg_dict= {'discharge_models': 
                             {'unconstrained':{'MetroMan':{}, 
                                               'BAM':{}, 
                                               'HiVDI':{}, 
                                               'MOMMA':{},
                                               'SADS':{}}, 
                              'constrained':{'MetroMan':{}, 
                                             'BAM':{}, 
                                             'HiVDI':{}, 
                                             'MOMMA':{},
                                             'SADS':{}} }}
    
    # geobam
    alg_dict['discharge_models']['unconstrained']['BAM'] = {
        "n": np.nan,
        "Abar": np.nan
    }
    alg_dict['discharge_models']['constrained']['BAM'] = {
        "n": np.nan,
        "Abar": np.nan
    }

    # hivdi
    alg_dict['discharge_models']['unconstrained']['HiVDI'] = {
        "alpha" : np.nan,  
        "beta" : np.nan,  
        "Abar" : np.nan
    }
    alg_dict['discharge_models']['constrained']['HiVDI'] = {
        "alpha" : np.nan,  
        "beta" : np.nan,  
        "Abar" : np.nan
    }

    # momma
    alg_dict['discharge_models']['unconstrained']['MOMMA'] = {
        "B" : np.nan,
        "H" : np.nan,
        "Save" : np.nan
    }
    alg_dict['discharge_models']['constrained']['MOMMA'] = {
        "B" : np.nan,
        "H" : np.nan,
        "Save" : np.nan
    }

    # sad
    alg_dict['discharge_models']['unconstrained']['SADS'] = {
        "n" : np.nan,
        "Abar" : np.nan
    }
    alg_dict['discharge_models']['constrained']['SADS'] = {
        "n" : np.nan,
        "Abar" : np.nan
    }

    # MetroMan    
    alg_dict['discharge_models']['unconstrained']['MetroMan'] = {
         "ninf" : np.nan,
         "p" : np.nan,
         "Abar" : np.nan
    }
    alg_dict['discharge_models']['constrained']['MetroMan'] = {
         "ninf" : np.nan,
         "p" : np.nan,
         "Abar" : np.nan
    }
    
    return alg_dict 

def get_gb_data(gb, group, logged):
    """Return geoBAM data as a numpy array.

    Parameters
    ----------
    gb: netCDF4.Dataset
        NetCDF file dataset to extract discharge time series
    group: str
        string name of group to access chains
    logged: bool
        boolean indicating if result is logged
    """

    chain1 = gb[group]["mean_chain1"][:].filled(np.nan)
    chain2 = gb[group]["mean_chain2"][:].filled(np.nan)
    chain3 = gb[group]["mean_chain3"][:].filled(np.nan)
    chains = np.vstack((chain1, chain2, chain3))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        if logged:
            return np.exp(np.nanmean(chains, axis=0))
        else:
            return np.nanmean(chains, axis=0)
