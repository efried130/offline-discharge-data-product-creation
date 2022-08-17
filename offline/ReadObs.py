"""
Read reach data from SWOT rivertile file.
"""
import netCDF4 as nc
import numpy as np
import geopandas as gpd

def Rivertile(rivertile_path, input_type):
    """
    Read in swot observations, inputs can be in .nc or .shp 
    format
    """   
    
    # shapefile inputs, rivertile_path[-3:] == 'shp'
    if input_type == 'single_pass': 
        dataset = gpd.read_file(rivertile_path)
        rivertile = {}
        rivertile['reach_id'] = np.array(\
              dataset['reach_id'][:].replace(-9.999999999990000e+11, np.nan), dtype=float)
        rivertile['height'] = dataset['wse'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['wse_u'] = dataset['wse_u'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['width'] = dataset['width'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['width_u'] = dataset['width_u'].replace(-9.999999999990000e+11, np.nan)
        rivertile['slope'] = dataset['slope'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['slope_u'] = dataset['slope_u'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['d_x_area'] = dataset['d_x_area'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['d_x_area_u'] = dataset['d_x_area_u'][:].replace(-9.999999999990000e+11, np.nan)
#         rivertile['nt'] = 1
#         rivertile["time_steps"] = 1
    
    # timeseries inputs, rivertile_path[-2:] == 'nc'
    elif input_pass == 'timeseries':
        dataset = nc.Dataset(rivertile_path, 'r')
        rivertile = {}
        rivertile['reach_id'] = dataset['reach']['reach_id'][:].filled(np.nan)
        rivertile['height'] = dataset['reach']['wse'][:].filled(np.nan)
        rivertile['wse_u'] = dataset['reach']['wse_u'][:].filled(np.nan)
        rivertile['width'] = dataset['reach']['width'][:].filled(np.nan)
        rivertile['width_u'] = dataset['reach']['width_u'][:].filled(np.nan)
        rivertile['slope'] = dataset['reach']['slope'][:].filled(np.nan)
        rivertile['slope_u'] = dataset['reach']['slope_u'][:].filled(np.nan)
        rivertile['d_x_area'] = dataset['reach']['d_x_area'][:].filled(np.nan)
        rivertile['d_x_area_u'] = dataset['reach']['d_x_area_u'][:].filled(np.nan)
        rivertile['nt'] = dataset.dimensions["nt"].size
        rivertile["time_steps"] = dataset["observations"][:]
        dataset.close()
    else:
        raise NotImplementedError(
            'input format is not supported!')
        
    return rivertile