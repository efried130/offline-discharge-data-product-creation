"""
Read reach data from SWOT rivertile file.
"""
import numpy as np
import os
import fiona
import geopandas as gpd

FILL_VALUE = -999999999999

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
    # d_x_area and its u
    infile['d_x_area'] = data_dict['d_x_area']
    if np.count_nonzero(~np.isnan(data_dict['d_x_area_u'])):
        infile['d_x_area_u']  = data_dict['d_x_area_u']
    # metroman, uncertainties need to be added later
    infile['dschg_m']  = data_dict['metro_q_uc']
    infile['dschg_gm']  = data_dict['metro_q_c']
    # geobam
    infile['dschg_b']  = data_dict['bam_q_uc']
    infile['dschg_gb']  = data_dict['bam_q_c']
    # hivdi
    infile['dschg_h']  = data_dict['hivdi_q_uc']
    infile['dschg_gh']  = data_dict['hivdi_q_c']
    # momma
    infile['dschg_o']  = data_dict['momma_q_uc']
    infile['dschg_go']  = data_dict['momma_q_c']
    # sads
    infile['dschg_s']  = data_dict['sads_q_uc']
    infile['dschg_gs']  = data_dict['sads_q_c']    
    # consensus
    infile['dschg_c']  = data_dict['consensus_q_uc']
    infile['dschg_gc']  = data_dict['consensus_q_c']
    input_file_split = os.path.split(input_file)
    input_file_split[1]
    infile.to_file(str(output_dir) + '/' + input_file_split[1])
