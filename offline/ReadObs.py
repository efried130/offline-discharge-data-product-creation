"""
Read reach data from SWOT rivertile file.
"""
import netCDF4 as nc
import numpy as np
import geopandas as gpd

def Rivertile(rivertile_path):
    """
    Read in swot observations, inputs can be in .nc or .shp 
    format
    """   
    
    if rivertile_path[-3:] == 'shp':
        dataset = gpd.read_file(rivertile_path)
        rivertile = {}
        rivertile['reach_id'] = np.array(dataset['reach_id'][:].replace(-9.999999999990000e+11, np.nan))
        rivertile['height'] = dataset['wse'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['wse_u'] = dataset['wse_u'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['width'] = dataset['width'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['width_u'] = dataset['width_u'].replace(-9.999999999990000e+11, np.nan)
        rivertile['slope'] = dataset['slope'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['slope_u'] = dataset['slope_u'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['d_x_area'] = dataset['d_x_area'][:].replace(-9.999999999990000e+11, np.nan)
        rivertile['d_x_area_u'] = dataset['d_x_area_u'][:].replace(-9.999999999990000e+11, np.nan)       
    
    elif rivertile_path[-3:] == '.nc':
        dataset = nc.Dataset(rivertile_path)
        rivertile = {}
        rivertile['reach_id'] = dataset['reaches']['reach_id'][:].filled(np.nan)
        rivertile['height'] = dataset['reaches']['wse'][:].filled(np.nan)
        rivertile['wse_u'] = dataset['reaches']['wse_u'][:].filled(np.nan)
        rivertile['width'] = dataset['reaches']['width'][:].filled(np.nan)
        rivertile['width_u'] = dataset['reaches']['width_u'][:].filled(np.nan)
        rivertile['slope'] = dataset['reaches']['slope'][:].filled(np.nan)
        rivertile['slope_u'] = dataset['reaches']['slope_u'][:].filled(np.nan)
        rivertile['d_x_area'] = dataset['reaches']['d_x_area'][:].filled(np.nan)
        rivertile['d_x_area_u'] = dataset['reaches']['d_x_area_u'][:].filled(np.nan)
        dataset.close()
        
    else:
        raise NotImplementedError(
            'input format is not supported!')
        
    return rivertile