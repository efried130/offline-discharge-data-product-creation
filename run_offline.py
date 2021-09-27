# Standard imports
import json
import os
from pathlib import Path
import sys

# Third-party imports
import numpy as np

# Local imports
from offline.ReadOb import Rivertile
from offline.ReadPRD import ReachDatabase
from offline.ReadQparams import extract_alg
from offline.discharge import compute
from offline.WriteQ import write_q

# Constants
INPUT = Path("/mnt/data/input")
FLPE_DIR = Path("/mnt/data/flpe")
OUTPUT = Path("/mnt/data/output")

def get_reach_data(reach_json):
    """Extract and return a dictionary of reach identifier, SoS and SWORD files.
    
    Parameters
    ----------
    reach_json : str
        Path to the file that contains the list of reaches to process
    """

    index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
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

def main(input, output):
    """Main function to execute offline discharge product generation and storage.
    
    Command line arguments:
    run_type: either `unconstrained` or `constrained` (argument 1)
    reach_json: title of JSON file that contains reach data (argument 2)
    """

    # Command line arguments
    try:
        run_type = sys.argv[1]
        reach_json = input.joinpath(sys.argv[2])
    except IndexError:
        run_type = None
        reach_json = input.joinpath("reaches.json") 

    # Input data
    reach_data = get_reach_data(reach_json)
    obs = Rivertile(input / "swot" / reach_data["swot"])

    priors = ReachDatabase(input / "sword" / reach_data["sword"], 
        reach_data["reach_id"])

    if run_type:
        priors["discharge_models"] = extract_alg(FLPE_DIR, reach_data["reach_id"], run_type)

    # Compute discharge
    data_dict = initialize_data_dict(obs["nt"], obs["time_steps"], reach_data["reach_id"])
    for i in range(obs["nt"]): 
        if priors["area_fit"]["h_w_nobs"] != -9999:
            outputs = compute(priors, obs['height'][i], obs["width"][i], obs["slope"][i], obs["d_x_area"][i])
            populate_data_array(data_dict, outputs, i)

    # Output discharge model values
    write_q(output, data_dict)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main(INPUT, OUTPUT)
    end = datetime.now()
    print(f"Exeuction time: {end - start}")