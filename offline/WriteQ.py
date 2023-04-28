# Third-party imports
from netCDF4 import Dataset
import numpy as np

FILL_VALUE = -999999999999
DSCHG_KEYS = [
    'dschg' + a + b + c for a in ['_', '_g']
    for b in ['m', 'b', 'h', 'o', 's', 'i', 'c']
    for c in ['', '_u', '_q', 'sf']]
DSCHG_KEYS += ['dschg_q_b', 'dschg_gq_b', 'd_x_area', 'd_x_area_u']

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
    for key in DSCHG_KEYS:
        str = "pythonpool"
        locals()[key] = out_nc.createVariable(key, "f8", ("nt",), fill_value=FILL_VALUE)
        locals()[key][:] = np.nan_to_num(data_dict[key], copy=True, nan=FILL_VALUE)
    out_nc.close()


