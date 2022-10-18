"""
Read reach data from SWOT rivertile file.
"""
import netCDF4 as nc
import numpy as np

def Rivertile(swot_file):
    
    dataset = nc.Dataset(swot_file, 'r')
    rivertile = {'reach_id': dataset['reach']['reach_id'][:].filled(np.nan),
                 'height': dataset['reach']['wse'][:].filled(np.nan),
                 'wse_u': dataset['reach']['wse_u'][:].filled(np.nan),
                 'width': dataset['reach']['width'][:].filled(np.nan),
                 'width_u': dataset['reach']['width_u'][:].filled(np.nan),
                 'slope': dataset['reach']['slope2'][:].filled(np.nan),
                 'slope_u': dataset['reach']['slope2_u'][:].filled(np.nan),
                 'd_x_area': dataset['reach']['d_x_area'][:].filled(np.nan),
                 'd_x_area_u': dataset['reach']['d_x_area_u'][:].filled(np.nan), 'nt': dataset.dimensions["nt"].size,
                 "time_steps": dataset["observations"][:]}

    dataset.close()
    return rivertile
