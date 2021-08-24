"""
Read reach data from SWOT rivertile file.
"""
import netCDF4 as nc
import numpy as np

def Rivertile(rivertile_dir, rch):
    
    rivertile_path = rivertile_dir + str(rch) + '_SWOT.nc'
    dataset = nc.Dataset(rivertile_path, 'r')
    rivertile = {}
    rivertile['reach_id'] = np.array(dataset['reach']['reach_id'][:])
    rivertile['height'] = np.array(dataset['reach']['wse'][:])
    rivertile['width'] = np.array(dataset['reach']['width'][:])
    rivertile['slope'] = np.array(dataset['reach']['slope2'][:])
 
    dataset.close()
    return rivertile