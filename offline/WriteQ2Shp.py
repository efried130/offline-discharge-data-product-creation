"""
Read reach data from SWOT rivertile file.
"""
import numpy as np
import os
import geopandas as gpd

FILL_VALUE = -999999999999
DSCHG_KEYS = [
    'dschg' + a + b + c for a in ['_', '_g']
    for b in ['m', 'b', 'h', 'o', 's', 'i', 'c']
    for c in ['', '_u', '_q', 'sf']]
DSCHG_KEYS += ['dschg_q_b', 'dschg_gq_b', 'd_x_area', 'd_x_area_u']


def write_q2shp(input_file, output_dir, data_dict):
    """Write model discharge values to shapefile file.
    
    Parameters
    ----------
    data: dict
        Dictionary of data values to write to shapefile
    output_dir: Path
        Path to output directory
    """
    # Read the original Shapefile
    infile = gpd.read_file(input_file)
    for key in DSCHG_KEYS:
        infile[key] = data_dict[key]
    input_file_split = os.path.split(input_file)
    input_file_split[1]
    infile.to_file(str(output_dir) + '/' + input_file_split[1])
