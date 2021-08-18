"""
Read all of the reaches from SWORD overlapping with SWOT rivertile file.
"""
import netCDF4 as nc
import numpy as np

def Rivertile(rivertile_path):
        
    dataset = nc.Dataset(rivertile_path, 'r')
    rivertile = {}
    rivertile['reach_id'] = np.array(dataset['reaches']['reach_id'][:])
    rivertile['height'] = np.array(dataset['reaches']['wse'][:])
    rivertile['width'] = np.array(dataset['reaches']['width'][:])
    rivertile['slope'] = np.array(dataset['reaches']['slope'][:])
    dataset.close()
    return rivertile