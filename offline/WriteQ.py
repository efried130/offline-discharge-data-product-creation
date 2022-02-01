# Third-party imports
from netCDF4 import Dataset
import numpy as np

FILL_VALUE = -999999999999

def write_q(output_dir, data_dict):
    """Write model discharge values to NetCDF file.
    
    Parameters
    ----------
    data_dict: dict
        Dictionary of data values to write to NetCDF
    output_dir: Path
        Path to output directory
    """

    # NetCDF Dataset
    out_nc = Dataset(output_dir / f"{data_dict['reach_id']}_offline.nc", 'w', 
        format="NETCDF4")
    out_nc.reach_id = data_dict["reach_id"]

    # Dim and coord var
    out_nc.createDimension("nt", data_dict["nt"])
    nt = out_nc.createVariable("nt", "i4", ("nt",))
    nt.units = "time steps"
    nt[:] = list(range(0,data_dict["nt"]))

    d_x_area = out_nc.createVariable("d_x_area", "f8", ("nt",), fill_value=FILL_VALUE)
    d_x_area[:] = np.nan_to_num(data_dict["d_x_area"], copy=True, nan=FILL_VALUE)

    if np.count_nonzero(~np.isnan(data_dict["d_x_area_u"])):
        d_x_area_u  = out_nc.createVariable("d_x_area_u", "f8", ("nt",), fill_value=FILL_VALUE)
        d_x_area_u[:] = np.nan_to_num(data_dict["d_x_area_u"], copy=True, nan=FILL_VALUE)

    metro_q_c  = out_nc.createVariable("metro_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    metro_q_c[:] = np.nan_to_num(data_dict["metro_q_c"], copy=True, nan=FILL_VALUE)

    bam_q_c  = out_nc.createVariable("bam_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    bam_q_c[:] = np.nan_to_num(data_dict["bam_q_c"], copy=True, nan=FILL_VALUE)

    hivdi_q_c  = out_nc.createVariable("hivdi_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    hivdi_q_c[:] = np.nan_to_num(data_dict["hivdi_q_c"], copy=True, nan=FILL_VALUE)

    momma_q_c  = out_nc.createVariable("momma_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    momma_q_c[:] = np.nan_to_num(data_dict["momma_q_c"], copy=True, nan=FILL_VALUE)

    sads_q_c  = out_nc.createVariable("sads_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    sads_q_c[:] = np.nan_to_num(data_dict["sads_q_c"], copy=True, nan=FILL_VALUE)

    consensus_q_c  = out_nc.createVariable("consensus_q_c", "f8", ("nt",), fill_value=FILL_VALUE)
    consensus_q_c[:] = np.nan_to_num(data_dict["consensus_q_c"], copy=True, nan=FILL_VALUE)

    metro_q_uc  = out_nc.createVariable("metro_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    metro_q_uc[:] = np.nan_to_num(data_dict["metro_q_uc"], copy=True, nan=FILL_VALUE)

    bam_q_uc  = out_nc.createVariable("bam_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    bam_q_uc[:] = np.nan_to_num(data_dict["bam_q_uc"], copy=True, nan=FILL_VALUE)

    hivdi_q_uc  = out_nc.createVariable("hivdi_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    hivdi_q_uc[:] = np.nan_to_num(data_dict["hivdi_q_uc"], copy=True, nan=FILL_VALUE)

    momma_q_uc  = out_nc.createVariable("momma_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    momma_q_uc[:] = np.nan_to_num(data_dict["momma_q_uc"], copy=True, nan=FILL_VALUE)

    sads_q_uc  = out_nc.createVariable("sads_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    sads_q_uc[:] = np.nan_to_num(data_dict["sads_q_uc"], copy=True, nan=FILL_VALUE)

    consensus_q_uc  = out_nc.createVariable("consensus_q_uc", "f8", ("nt",), fill_value=FILL_VALUE)
    consensus_q_uc[:] = np.nan_to_num(data_dict["consensus_q_uc"], copy=True, nan=FILL_VALUE)

    out_nc.close()


