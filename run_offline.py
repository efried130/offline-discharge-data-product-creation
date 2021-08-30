# Standard imports
import json
import os
from pathlib import Path
import sys

# Third-party imports
import numpy as np

# Local imports
from src.ReadOb import Rivertile
from src.ReadPRD import ReachDatabase
from src.discharge import compute

INPUT = Path("/home/nikki/Documents/confluence/workspace/offline/data/input")
OUTPUT = Path("/home/nikki/Documents/confluence/workspace/offline/data/output")

def get_reach_data(reach_json):
    """Extract and return a dictionary of reach identifier, SoS and SWORD files.
    
    Parameters
    ----------
    reach_json : str
        Path to the file that contains the list of reaches to process
    """

    # index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    index = 13
    with open(reach_json) as json_file:
        data = json.load(json_file)
    return data[index]

def create_empty_outputs(nt):
    """Create an empty outputs dictionary for datasets with all missing data.
    
    Parameters
    ----------
    nt: int
        Number of time steps
    """

    return ({
        "d_x_area": np.repeat(np.nan, nt),
        "d_x_area_u": np.repeat(np.nan, nt),
        "metro_q_c" : np.repeat(np.nan, nt),
        "bam_q_c" : np.repeat(np.nan, nt),
        "hivdi_q_c" : np.repeat(np.nan, nt),
        "momma_q_c" : np.repeat(np.nan, nt),
        "sads_q_c" : np.repeat(np.nan, nt),
        "metro_q_uc" : np.repeat(np.nan, nt),
        "bam_q_uc" : np.repeat(np.nan, nt),
        "hivdi_q_uc" : np.repeat(np.nan, nt),
        "momma_q_uc" : np.repeat(np.nan, nt),
        "sads_q_uc" : np.repeat(np.nan, nt)
    })

def main():
    """Main function to execute offline discharge product generation and storage."""

    # Command line arguments
    try:
        reach_json = INPUT.joinpath(sys.argv[1])
    except IndexError:
        reach_json = INPUT.joinpath("reaches.json") 

    # Input data
    reach_data = get_reach_data(reach_json)
    obs = Rivertile(INPUT / "swot" / reach_data["swot"])
    priors = ReachDatabase(INPUT / "sword" / reach_data["sword"], 
        reach_data["reach_id"])

    # Compute discharge
    discharge_model_values = {}
    for i in range(len(obs['height'])): 
        if priors['area_fits']['h_w_nobs'] != -9999:
            discharge_model_values[i] = compute(priors, obs['height'][i], 
                obs['width'][i], obs['slope'][i])
        else:
            discharge_model_values[i] = create_empty_outputs(obs["nt"])
    
    # Output discharge
    print(discharge_model_values)

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Exeuction time: {end - start}")