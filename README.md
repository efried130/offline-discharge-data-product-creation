# offline-discharge-data-product-creation
Offline Discharge Data Product Creation

**Example Usage**

```
InputDir=Path('./offline_inputs')
reaches_json=InputDir.joinpath('reaches.json')

with open(reaches_json) as json_file:
    reaches = json.load(json_file)
    
nR=len(reaches)

for reach in range(nR):
     %run /Users/mtd/GitHub/SWOT-confluence/offline-discharge-data-product-creation/run_offline.py 'unconstrained' 'timeseries' 'reaches.json'  $reach
     
```
     
So the command line arguments in order are [branch] [SWOT observation mode] [reaches file] [reach #]

where [branch] is either "constrained" or "unconstrained"
and [SWOT observation mode] is either "timeseries" or "single_pass". The "timeseries" option uses the .nc files created by the input module, and the "single_pass" uses the L2 single pass SWOT shapefiles.
