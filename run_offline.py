# Standard imports
import json
import os
import pdb
from pathlib import Path
import sys
# Third-party imports
import numpy as np

# Local imports
from offline.ReadOb import Rivertile
from offline.ReadObs import Rivertile
from offline.ReadPRD import ReachDatabase
#from offline.ReadQparams import extract_alg # use with flpe dir
from offline.ReadQparamsIntegrator import extract_alg #  use with moi dir
from offline.discharge import compute, empty_q
from offline.WriteQ import write_q
from offline.WriteQ2Shp import write_q2shp

#Constants constrained
INPUT = Path("/Users/rwei/Documents/confluence/offline_data_mar/constrained/mnt/input")
FLPE_DIR = Path("/Users/rwei/Documents/confluence/offline_data_mar/constrained/mnt/constrained_moi_update")
#FLPE_DIR = Path("/Users/rwei/Documents/confluence/OneDrive_1_9-23-2022/offline_inputs/mnt/flpe")
OUTPUT = Path("/Users/rwei/Documents/confluence/offline_data_mar/constrained/mnt/constrained_output")
# SWORD dir for single_pass run
SWORD = Path("/Users/rwei/Documents/confluence/offline_data_mar/constrained/mnt/input/sword/na_sword_v11_moi.nc")

# Constants unconstrained
# INPUT = Path("/Users/rwei/Documents/confluence/offline_data_mar/unconstrained/mnt/input")
# #FLPE_DIR = Path("/Users/rwei/Documents/confluence/offline_data_mar/unconstrained/mnt/moi")
# FLPE_DIR = Path("/Users/rwei/Documents/confluence/offline_data_mar/unconstrained/mnt/unconstrained_moi_update")
# OUTPUT = Path("/Users/rwei/Documents/confluence/offline_data_mar/unconstrained/mnt/unconstrained_output")
# # SWORD dir for single_pass run
# SWORD = Path("/Users/rwei/Documents/confluence/offline_data_mar/unconstrained/mnt/input/sword/na_sword_v11_moi.nc")

# SWORD =  the path to SWORD is hard-coded below, where it says'priors = ReachDatabase(input / "sword"...'
# INPUT = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_inputs')  #must agree with input_type
# FLPE_DIR = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_inputs/moi')
# OUTPUT = Path('/Users/mtd/Analysis/SWOT/Discharge/Confluence/paper_debug/offline_outputs')

def get_reach_data(reach_json, index_to_run):
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
        "metro_q_c": np.repeat(np.nan, nt),
        "metro_q_uc": np.repeat(np.nan, nt),
        "metro_q_c_s_u": np.repeat(np.nan, nt),
        "metro_q_c_u": np.repeat(np.nan, nt),
        "metro_q_uc_s_u": np.repeat(np.nan, nt),
        "metro_q_uc_u": np.repeat(np.nan, nt),
        "bam_q_c": np.repeat(np.nan, nt),
        "bam_q_uc": np.repeat(np.nan, nt),
        "bam_q_c_s_u": np.repeat(np.nan, nt),
        "bam_q_c_u": np.repeat(np.nan, nt),
        "bam_q_uc_s_u": np.repeat(np.nan, nt),
        "bam_q_uc_u": np.repeat(np.nan, nt),
        "hivdi_q_c": np.repeat(np.nan, nt),
        "hivdi_q_uc": np.repeat(np.nan, nt),
        "hivdi_q_c_s_u": np.repeat(np.nan, nt),
        "hivdi_q_c_u": np.repeat(np.nan, nt),
        "hivdi_q_uc_s_u": np.repeat(np.nan, nt),
        "hivdi_q_uc_u": np.repeat(np.nan, nt),
        "momma_q_c": np.repeat(np.nan, nt),
        "momma_q_uc": np.repeat(np.nan, nt),
        "momma_q_c_s_u": np.repeat(np.nan, nt),
        "momma_q_c_u": np.repeat(np.nan, nt),
        "momma_q_uc_s_u": np.repeat(np.nan, nt),
        "momma_q_uc_u": np.repeat(np.nan, nt),
        "sads_q_c": np.repeat(np.nan, nt),
        "sads_q_uc": np.repeat(np.nan, nt),
        "sads_q_c_s_u": np.repeat(np.nan, nt),
        "sads_q_c_u": np.repeat(np.nan, nt),
        "sads_q_uc_s_u": np.repeat(np.nan, nt),
        "sads_q_uc_u": np.repeat(np.nan, nt),
        "sic4dvar_q_c": np.repeat(np.nan, nt),
        "sic4dvar_q_uc": np.repeat(np.nan, nt),
        "sic4dvar_q_c_s_u": np.repeat(np.nan, nt),
        "sic4dvar_q_c_u": np.repeat(np.nan, nt),
        "sic4dvar_q_uc_s_u": np.repeat(np.nan, nt),
        "sic4dvar_q_uc_u": np.repeat(np.nan, nt),
        "consensus_q_c": np.repeat(np.nan, nt),
        "consensus_q_uc": np.repeat(np.nan, nt),
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
        "metro_q_c": np.repeat(np.nan, rch_n),
        "metro_q_uc": np.repeat(np.nan, rch_n),
        "metro_q_c_s_u": np.repeat(np.nan, rch_n),
        "metro_q_c_u": np.repeat(np.nan, rch_n),
        "metro_q_uc_s_u": np.repeat(np.nan, rch_n),
        "metro_q_uc_u": np.repeat(np.nan, rch_n),
        "bam_q_c": np.repeat(np.nan, rch_n),
        "bam_q_uc": np.repeat(np.nan, rch_n),
        "bam_q_c_s_u": np.repeat(np.nan, rch_n),
        "bam_q_c_u": np.repeat(np.nan, rch_n),
        "bam_q_uc_s_u": np.repeat(np.nan, rch_n),
        "bam_q_uc_u": np.repeat(np.nan, rch_n),
        "hivdi_q_c": np.repeat(np.nan, rch_n),
        "hivdi_q_uc": np.repeat(np.nan, rch_n),
        "hivdi_q_c_s_u": np.repeat(np.nan, rch_n),
        "hivdi_q_c_u": np.repeat(np.nan, rch_n),
        "hivdi_q_uc_s_u": np.repeat(np.nan, rch_n),
        "hivdi_q_uc_u": np.repeat(np.nan, rch_n),
        "momma_q_c": np.repeat(np.nan, rch_n),
        "momma_q_uc": np.repeat(np.nan, rch_n),
        "momma_q_c_s_u": np.repeat(np.nan, rch_n),
        "momma_q_c_u": np.repeat(np.nan, rch_n),
        "momma_q_uc_s_u": np.repeat(np.nan, rch_n),
        "momma_q_uc_u": np.repeat(np.nan, rch_n),
        "sads_q_c": np.repeat(np.nan, rch_n),
        "sads_q_uc": np.repeat(np.nan, rch_n),
        "sads_q_c_s_u": np.repeat(np.nan, rch_n),
        "sads_q_c_u": np.repeat(np.nan, rch_n),
        "sads_q_uc_s_u": np.repeat(np.nan, rch_n),
        "sads_q_uc_u": np.repeat(np.nan, rch_n),
        "sic4dvar_q_c": np.repeat(np.nan, rch_n),
        "sic4dvar_q_uc": np.repeat(np.nan, rch_n),
        "sic4dvar_q_c_s_u": np.repeat(np.nan, rch_n),
        "sic4dvar_q_c_u": np.repeat(np.nan, rch_n),
        "sic4dvar_q_uc_s_u": np.repeat(np.nan, rch_n),
        "sic4dvar_q_uc_u": np.repeat(np.nan, rch_n),
        "consensus_q_c": np.repeat(np.nan, rch_n),
        "consensus_q_uc": np.repeat(np.nan, rch_n),
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
    # d_x_are
    data_dict["d_x_area"][index] = outputs["d_x_area"]
    data_dict["d_x_area_u"][index] = outputs[
        "d_x_area_u"] if "d_x_area_u" in outputs.keys() else None
    # metroman
    data_dict["metro_q_c"][index] = outputs["metro_q_c"][0] if type(
        outputs["metro_q_c"]) is np.ndarray else outputs["metro_q_c"]
    data_dict["metro_q_c_s_u"][index] = outputs["metro_q_c_s_u"].item() if type(
        outputs["metro_q_c_s_u"]) is np.ndarray else outputs["metro_q_c_s_u"]
    data_dict["metro_q_c_u"][index] = outputs["metro_q_c_u"][0] if type(
        outputs["metro_q_c_u"]) is np.ndarray else outputs["metro_q_c_u"]

    data_dict["metro_q_uc"][index] = outputs["metro_q_uc"][0] if type(
        outputs["metro_q_uc"]) is np.ndarray else outputs["metro_q_uc"]
    data_dict["metro_q_uc_s_u"][index] = outputs["metro_q_uc_s_u"][0] if type(
        outputs["metro_q_uc_s_u"]) is np.ndarray else outputs["metro_q_uc_s_u"]
    data_dict["metro_q_uc_u"][index] = outputs["metro_q_uc_u"][0] if type(
        outputs["metro_q_uc_u"]) is np.ndarray else outputs["metro_q_uc_u"]

    # bam
    data_dict["bam_q_c"][index] = outputs["bam_q_c"][0] if type(
        outputs["bam_q_c"]) is np.ndarray else outputs["bam_q_c"]
    data_dict["bam_q_c_s_u"][index] = outputs["bam_q_c_s_u"][0] if type(
        outputs["bam_q_c_s_u"]) is np.ndarray else outputs["bam_q_c_s_u"]
    data_dict["bam_q_c_u"][index] = outputs["bam_q_c_u"][0] if type(
        outputs["bam_q_c_u"]) is np.ndarray else outputs["bam_q_c_u"]

    data_dict["bam_q_uc"][index] = outputs["bam_q_uc"][0] if type(
        outputs["bam_q_uc"]) is np.ndarray else outputs["bam_q_uc"]
    data_dict["bam_q_uc_s_u"][index] = outputs["bam_q_uc_s_u"][0] if type(
        outputs["bam_q_uc_s_u"]) is np.ndarray else outputs["bam_q_uc_s_u"]
    data_dict["bam_q_uc_u"][index] = outputs["bam_q_uc_u"][0] if type(
        outputs["bam_q_uc_u"]) is np.ndarray else outputs["bam_q_uc_u"]

    # hivdi
    data_dict["hivdi_q_c"][index] = outputs["hivdi_q_c"][0] if type(
        outputs["hivdi_q_c"]) is np.ndarray else outputs["hivdi_q_c"]
    data_dict["hivdi_q_c_s_u"][index] = outputs["hivdi_q_c_s_u"][0] if type(
        outputs["hivdi_q_c_s_u"]) is np.ndarray else outputs["hivdi_q_c_s_u"]
    data_dict["hivdi_q_c_u"][index] = outputs["hivdi_q_c_u"][0] if type(
        outputs["hivdi_q_c_u"]) is np.ndarray else outputs["hivdi_q_c_u"]

    data_dict["hivdi_q_uc"][index] = outputs["hivdi_q_uc"][0] if type(
        outputs["hivdi_q_uc"]) is np.ndarray else outputs["hivdi_q_uc"]
    data_dict["hivdi_q_uc_s_u"][index] = outputs["hivdi_q_uc_s_u"][0] if type(
        outputs["hivdi_q_uc_s_u"]) is np.ndarray else outputs["hivdi_q_uc_s_u"]
    data_dict["hivdi_q_uc_u"][index] = outputs["hivdi_q_uc_u"][0] if type(
        outputs["hivdi_q_uc_u"]) is np.ndarray else outputs["hivdi_q_uc_u"]

    # momma
    data_dict["momma_q_c"][index] = outputs["momma_q_c"][0] if type(
        outputs["momma_q_c"]) is np.ndarray else outputs["momma_q_c"]
    data_dict["momma_q_c_s_u"][index] = outputs["momma_q_c_s_u"][0] if type(
        outputs["momma_q_c_s_u"]) is np.ndarray else outputs["momma_q_c_s_u"]
    data_dict["momma_q_c_u"][index] = outputs["momma_q_c_u"][0] if type(
        outputs["momma_q_c_u"]) is np.ndarray else outputs["momma_q_c_u"]

    data_dict["momma_q_uc"][index] = outputs["momma_q_uc"][0] if type(
        outputs["momma_q_uc"]) is np.ndarray else outputs["momma_q_uc"]
    data_dict["momma_q_uc_s_u"][index] = outputs["momma_q_uc_s_u"][0] if type(
        outputs["momma_q_uc_s_u"]) is np.ndarray else outputs["momma_q_uc_s_u"]
    data_dict["momma_q_uc_u"][index] = outputs["momma_q_uc_u"][0] if type(
        outputs["momma_q_uc_u"]) is np.ndarray else outputs["momma_q_uc_u"]

    # sads
    data_dict["sads_q_c"][index] = outputs["sads_q_c"][0] if type(
        outputs["sads_q_c"]) is np.ndarray else outputs["sads_q_c"]
    data_dict["sads_q_c_s_u"][index] = outputs["sads_q_c_s_u"][0] if type(
        outputs["sads_q_c_s_u"]) is np.ndarray else outputs["sads_q_c_s_u"]
    data_dict["sads_q_c_u"][index] = outputs["sads_q_c_u"][0] if type(
        outputs["sads_q_c_u"]) is np.ndarray else outputs["sads_q_c_u"]

    data_dict["sads_q_uc"][index] = outputs["sads_q_uc"][0] if type(
        outputs["sads_q_uc"]) is np.ndarray else outputs["sads_q_uc"]
    data_dict["sads_q_uc_s_u"][index] = outputs["sads_q_uc_s_u"][0] if type(
        outputs["sads_q_uc_s_u"]) is np.ndarray else outputs["sads_q_uc_s_u"]
    data_dict["sads_q_uc_u"][index] = outputs["sads_q_uc_u"][0] if type(
        outputs["sads_q_uc_u"]) is np.ndarray else outputs["sads_q_uc_u"]

    # sic4dvar
    data_dict["sic4dvar_q_c"][index] = outputs["sic4dvar_q_c"][0] if type(
        outputs["sic4dvar_q_c"]) is np.ndarray else outputs["sic4dvar_q_c"]
    data_dict["sic4dvar_q_c_s_u"][index] = outputs["sic4dvar_q_c_s_u"][0] if type(
        outputs["sic4dvar_q_c_s_u"]) is np.ndarray else outputs["sic4dvar_q_c_s_u"]
    data_dict["sic4dvar_q_c_u"][index] = outputs["sic4dvar_q_c_u"][0] if type(
        outputs["sic4dvar_q_c_u"]) is np.ndarray else outputs["sic4dvar_q_c_u"]

    data_dict["sic4dvar_q_uc"][index] = outputs["sic4dvar_q_uc"][0] if type(
        outputs["sic4dvar_q_uc"]) is np.ndarray else outputs["sic4dvar_q_uc"]
    data_dict["sic4dvar_q_uc_s_u"][index] = outputs["sic4dvar_q_uc_s_u"][0] if type(
        outputs["sic4dvar_q_uc_s_u"]) is np.ndarray else outputs["sic4dvar_q_uc_s_u"]
    data_dict["sic4dvar_q_uc_u"][index] = outputs["sic4dvar_q_uc_u"][0] if type(
        outputs["sic4dvar_q_uc_u"]) is np.ndarray else outputs["sic4dvar_q_uc_u"]


    # consensus
    data_dict["consensus_q_c"][index] = outputs["consensus_q_c"][0] if type(
        outputs["consensus_q_c"]) is np.ndarray else outputs["consensus_q_c"]


    data_dict["consensus_q_uc"][index] = outputs["consensus_q_uc"][0] if type(
        outputs["consensus_q_uc"]) is np.ndarray else outputs["consensus_q_uc"]

    # Convert missing values to NaN values
    for k, v in data_dict.items():
        if k != "nt" and k != "reach_id" and k != "time_steps":
            v[np.isclose(v, -1.00000000e+12)] = np.nan


def main(input, output, index_to_run):
    """Main function to execute offline discharge product generation and
    storage.
    Command line arguments:
    run_type: options are `unconstrained` or `constrained` (argument 1)
    input_type: options are 'timeseries' or 'single_pass' (argument 2)
                - timeseries: timer-series swot observations in one NetCDF file,
                              cross-sectional area change (d_x_area) should be
                              already computed and saved in time-series files.
                              discharge params from different discharge models
                              will be extracted from either moi-sword or integrator
                - single-pass: single-pass swot observation shapefiles, this is
                               what the swot data will look like. sword database
                               will be pulled d_x_area and discharge will be computed.
    flp_source: options are 'sword' or 'integrator' (argument 3)
    reach_json: title of JSON file that contains reach data (argument 4)

    Note: if input_type = single_pass, run_type, flp_source and reach_json will not be used.
          Set them to None or any other strings will work
    """
    # Command line arguments
    try:
        run_type = sys.argv[1]
        input_type = sys.argv[2]
        flp_source = sys.argv[3] # options are 'sword' or 'integrator
        if input_type == 'timeseries':
            reach_json = input.joinpath(sys.argv[4])
    except IndexError:
        run_type = None
        if input_type == 'timeseries':
            reach_json = input.joinpath("reaches.json")

    # Input timeseries data
    if input_type == 'timeseries':
        reach_data = get_reach_data(reach_json, index_to_run)
        obs = Rivertile(input / "swot" / reach_data["swot"], input_type)
        if flp_source == 'sword':
            # priors = ReachDatabase(input / "sword" / reach_data["sword"].
            #                        replace('.nc', '_moi.nc'),
            #                        reach_data["reach_id"])
            priors = ReachDatabase(input / "sword" / reach_data["sword"],
                                   reach_data["reach_id"])
            # if dA from timeseries, remove 'area_fit' params, so discharge module
            # won't compute dA again, don't need to set up dA source
            del priors['area_fit']
        elif flp_source == 'integrator':
            priors = {"discharge_models": extract_alg(FLPE_DIR,
                                                      reach_data["reach_id"],
                                                      run_type)}
        else:
            sys.exit('Warning: flp source not valid')

        # Compute discharge
        data_dict = initialize_data_dict(obs["nt"], obs["time_steps"],
                                         reach_data["reach_id"])
        for i in range(obs["nt"]):
            outputs = compute(priors, obs['height'][i], obs["width"][i],
                              obs["slope"][i], obs["d_x_area"][i],
                              obs["wse_u"][i],
                              obs["width_u"][i], obs["slope_u"][i],
                              obs["d_x_area_u"][i])
            populate_data_array(data_dict, outputs, i)

        # Output discharge model values
        write_q(output, data_dict)

    # List all the reach .shp files in INPUT directory and process one by one
    if input_type == 'single_pass':
        input_shapefile = input.joinpath('shapefile')
        shapefiles = list(input_shapefile.glob('SWOT_L2_HR_RiverSP_reach*.shp'))

        for shapefile in shapefiles:
            print('SHAPEFILE: ', shapefile)
            obs = Rivertile(shapefile, input_type)
            data_dict = initialize_data_dict_sp(len(obs['reach_id']))
            for j in range(len(obs['reach_id'])):
                priors = ReachDatabase(SWORD, obs['reach_id'][j])
                if obs['height'][j] != -999999999999 \
                        and priors["area_fit"]["h_variance"] != -9999:
                    # sword doesn't have the values below, so made up some values for testing
                    # remove after
                    priors["area_fit"]["w_variance"] = 5
                    priors["area_fit"]["hw_covariance"] = 3
                    priors["area_fit"]["h_break"] = [[[50], [100], [160]]]
                    priors["area_fit"]["w_break"] = [[[200], [400], [600]]]
                    priors["area_fit"]["h_err_stdev"] = 0.01
                    priors["area_fit"]["w_err_stdev"] = 2
                    priors["area_fit"]["h_w_nobs"] = 25
                    priors["area_fit"]["fit_coeffs"] = [[[2], [1], [2]]]
                    priors["area_fit"]["med_flow_area"] = 100
                    # remove above after testing
                    outputs = compute(priors, obs['height'][j],
                                      obs['width'][j], obs['slope'][j],
                                      obs['d_x_area'][j], obs['wse_u'][j],
                                      obs['width_u'][j], obs['slope_u'][j],
                                      obs['d_x_area_u'][j])
                else:
                    outputs = empty_q()
                populate_data_array(data_dict, outputs, j)
            # Output discharge model values
            write_q2shp(shapefile, output, data_dict)

if __name__ == "__main__":
    from datetime import datetime

    start = datetime.now()

    try:
        index_to_run = int(sys.argv[5])  # integer
    except IndexError:
        index_to_run = -235  # AWS

    # print('indx=',index_to_run)

    main(INPUT, OUTPUT, index_to_run)

    end = datetime.now()
    print(f"Exeuction time: {end - start}")
