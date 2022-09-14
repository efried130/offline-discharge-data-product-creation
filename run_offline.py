# Standard imports
import json
import os
from pathlib import Path
import sys

# Third-party imports
import numpy as np

# Local imports
from offline.ReadOb import Rivertile
from offline.ReadObs import Rivertile 
from offline.ReadPRD import ReachDatabase
#from offline.ReadQparams import extract_alg
from offline.ReadQparamsIntegrator import extract_alg
from offline.discharge import compute, empty_q
from offline.WriteQ import write_q
from offline.WriteQ2Shp import write_q2shp

# Constants
#INPUT = Path("/mnt/data/input")
#FLPE_DIR = Path("/mnt/data/flpe")
#OUTPUT = Path("/mnt/data/output")

#SWORD =  the path to SWORD is hard-coded below, where it says'priors = ReachDatabase(input / "sword"...'
INPUT = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_inputs')  #must agree with input_type
FLPE_DIR = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_inputs/moi') 
OUTPUT = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_outputs')

def get_reach_data(reach_json,index_to_run):
    """Extract and return a dictionary of reach identifier, SoS and SWORD files.
    
    Parameters
    ----------
    reach_json : str
        Path to the file that contains the list of reaches to process
    """
   
    if index_to_run == -235:
        index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    else:
        index = index_to_run

    with open(reach_json) as json_file:
        data = json.load(json_file)
    return data[index]

def initialize_data_dict(nt, time_steps, reach_id):
    """Create an empty dictionary for holding model discharge values.
    
    Parameters
    ----------
    nt: int
        Number of time steps
    reach_id: int
        Unique reach identifier
    time_steps: np.ndarray
        Array of dates
    """

    return ({
        "d_x_area": np.repeat(np.nan, nt),
        "d_x_area_u": np.repeat(np.nan, nt),
        "metro_q_c" : np.repeat(np.nan, nt),
        "bam_q_c" : np.repeat(np.nan, nt),
        "hivdi_q_c" : np.repeat(np.nan, nt),
        "momma_q_c" : np.repeat(np.nan, nt),
        "sads_q_c" : np.repeat(np.nan, nt),
        "consensus_q_c" : np.repeat(np.nan, nt),
        "metro_q_uc" : np.repeat(np.nan, nt),
        "bam_q_uc" : np.repeat(np.nan, nt),
        "hivdi_q_uc" : np.repeat(np.nan, nt),
        "momma_q_uc" : np.repeat(np.nan, nt),
        "sads_q_uc" : np.repeat(np.nan, nt),
        "consensus_q_uc" : np.repeat(np.nan, nt),
        "nt": nt,
        "reach_id": reach_id,
        "time_steps": time_steps
    })

def initialize_data_dict_sp(rch_n):
    """Create an empty dictionary for holding model discharge values.
    Parameters
    ----------
    rch_n: int
        Number of reaches in shapefile
    """

    return ({
        "d_x_area": np.repeat(np.nan, rch_n),
        "d_x_area_u": np.repeat(np.nan, rch_n),
        "metro_q_c" : np.repeat(np.nan, rch_n),
        "bam_q_c" : np.repeat(np.nan, rch_n),
        "hivdi_q_c" : np.repeat(np.nan, rch_n),
        "momma_q_c" : np.repeat(np.nan, rch_n),
        "sads_q_c" : np.repeat(np.nan, rch_n),
        "consensus_q_c" : np.repeat(np.nan, rch_n),
        "metro_q_uc" : np.repeat(np.nan, rch_n),
        "bam_q_uc" : np.repeat(np.nan, rch_n),
        "hivdi_q_uc" : np.repeat(np.nan, rch_n),
        "momma_q_uc" : np.repeat(np.nan, rch_n),
        "sads_q_uc" : np.repeat(np.nan, rch_n),
        "consensus_q_uc" : np.repeat(np.nan, rch_n),
        "reach_id": np.repeat(np.nan, rch_n)
    })

def populate_data_array(data_dict, outputs, index):
    """Populate data_dict with outputs data.
    
    Parameters
    ----------
    data_dict: dict
        dictionary of model discharge values over time
    outputs: dict
        dictionary of model discharge values for one time
    index: int
        index value for data selection
    """

    # Insert data
    data_dict["d_x_area"][index] = outputs["d_x_area"]
    data_dict["d_x_area_u"][index] = outputs["d_x_area_u"] if "d_x_area_u" in outputs.keys() else None
    data_dict["metro_q_c"][index] = outputs["metro_q_c"][0] if type(outputs["metro_q_c"]) is np.ndarray else outputs["metro_q_c"]
    data_dict["bam_q_c"][index] = outputs["bam_q_c"][0] if type(outputs["bam_q_c"]) is np.ndarray else outputs["bam_q_c"]
    data_dict["hivdi_q_c"][index] = outputs["hivdi_q_c"][0] if type(outputs["hivdi_q_c"]) is np.ndarray else outputs["hivdi_q_c"]
    data_dict["momma_q_c"][index] = outputs["momma_q_c"][0] if type(outputs["momma_q_c"]) is np.ndarray else outputs["momma_q_c"]
    data_dict["sads_q_c"][index] = outputs["sads_q_c"][0] if type(outputs["sads_q_c"]) is np.ndarray else outputs["sads_q_c"]
    data_dict["consensus_q_c"][index] = outputs["consensus_q_c"][0] if type(outputs["consensus_q_c"]) is np.ndarray else outputs["consensus_q_c"]
    data_dict["metro_q_uc"][index] = outputs["metro_q_uc"][0] if type(outputs["metro_q_uc"]) is np.ndarray else outputs["metro_q_uc"]
    data_dict["bam_q_uc"][index] = outputs["bam_q_uc"][0] if type(outputs["bam_q_uc"]) is np.ndarray else outputs["bam_q_uc"]
    data_dict["hivdi_q_uc"][index] = outputs["hivdi_q_uc"][0] if type(outputs["hivdi_q_uc"]) is np.ndarray else outputs["hivdi_q_uc"]
    data_dict["momma_q_uc"][index] = outputs["momma_q_uc"][0] if type(outputs["momma_q_uc"]) is np.ndarray else outputs["momma_q_uc"]
    data_dict["sads_q_uc"][index] = outputs["sads_q_uc"][0] if type(outputs["sads_q_uc"]) is np.ndarray else outputs["sads_q_uc"]
    data_dict["consensus_q_uc"][index] = outputs["consensus_q_uc"][0] if type(outputs["consensus_q_uc"]) is np.ndarray else outputs["consensus_q_uc"]

    # Convert missing values to NaN values
    for k,v in data_dict.items(): 
        if k != "nt" and k != "reach_id" and k != "time_steps":
            v[np.isclose(v, -1.00000000e+12)] = np.nan

def main(input, output, index_to_run):
    """Main function to execute offline discharge product generation and storage.
    
    Command line arguments:
    run_type: either `unconstrained` or `constrained` (argument 1)
    reach_json: title of JSON file that contains reach data (argument 2)
    """

    # Command line arguments
    try:
        run_type = sys.argv[1]
        input_type = sys.argv[2]
        if input_type == 'timeseries':
            reach_json = input.joinpath(sys.argv[3])
    except IndexError:
        run_type = None
        if input_type == 'timeseries':
            reach_json = input.joinpath("reaches.json")

    flp_source='sword' # options are 'sword' or 'integrator'
    da_source='obs' # options are 'obs' or 'compute'

    #print(run_type)
    #print(input_type)
    #print(index_to_run)
    #print(reach_json)

    # Input data for timeseries data
    if input_type == 'timeseries':
        reach_data = get_reach_data(reach_json, index_to_run)
        obs = Rivertile(input / "swot" / reach_data["swot"], input_type)
        priors = ReachDatabase(input / "sword" / reach_data["sword"], 
            reach_data["reach_id"])

        #if run_type:
        if flp_source == 'integrator':
            priors["discharge_models"] = extract_alg(FLPE_DIR, reach_data["reach_id"], run_type)

        if  da_source == 'obs':
            del priors['area_fit']

        # Compute discharge
        data_dict = initialize_data_dict(obs["nt"], obs["time_steps"], reach_data["reach_id"])
        for i in range(obs["nt"]): 
#            if priors["area_fit"]["h_w_nobs"] != -9999:
                outputs = compute(priors, obs['height'][i], obs["width"][i], 
                                  obs["slope"][i], obs["d_x_area"][i], obs["wse_u"][i],
                                  obs["width_u"][i], obs["slope_u"][i], 
                                  obs["d_x_area_u"][i])
                populate_data_array(data_dict, outputs, i)

        # Output discharge model values
        write_q(output, data_dict)
    
    # find reaches in FLPE folder 
    FLPE_file_list = list(FLPE_DIR.glob('*integrator.nc'))
    FLPE_rch = []
    for i in range(len(FLPE_file_list)):
        FLPE_file = os.path.split(FLPE_file_list[i])
        FLPE_rch.append(int(FLPE_file[1].split('_')[0]))
    # input data for single pass data
    # list all of .shp files in INPUT directory and process one by one
    if input_type == 'single_pass':
        shapefiles = list(INPUT.glob('SWOT_L2_HR_RiverSP_reach*.shp'))
        FLPE_rch_list = list(FLPE_DIR.glob('*integrator.nc'))
        for i in range(len(shapefiles)):
            print('SHAPEFILE:        ', shapefiles[i])
            obs = Rivertile(shapefiles[i], input_type)
            data_dict = initialize_data_dict_sp(len(obs['reach_id']))
            for j in range(len(obs['reach_id'])):
                #priors = ReachDatabase(SWORD, obs['reach_id'][j]) # using params from FLPE, comment this out 
                print('reach_id: ', int(obs['reach_id'][j]))
                if run_type and obs['height'][j] != -999999999999 \
                and int(obs['reach_id'][j]) in FLPE_rch_list:
                    priors['discharge_models'] = extract_alg(FLPE_DIR, obs['reach_id'][j], run_type)
                    if priors["area_fit"]["h_w_nobs"] != -9999:
                        outputs = compute(priors, obs['height'][j], obs['width'][j],
                                          obs['slope'][j], obs['d_x_area'][j],
                                          obs['wse_u'][j], obs['width_u'][j],
                                          obs['slope_u'][j], obs['d_x_area_u'][j])
                else:
                    outputs = empty_q()
                populate_data_array(data_dict, outputs, j)
            # Output discharge model values
            # print('data_dict', data_dict)
            write_q2shp(shapefiles[i], output, data_dict)


if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()

    try:
        index_to_run=int(sys.argv[4]) #integer
    except IndexError:
        index_to_run=-235 #AWS

    #print('indx=',index_to_run)

    main(INPUT, OUTPUT, index_to_run)

    end = datetime.now()
    print(f"Exeuction time: {end - start}")
