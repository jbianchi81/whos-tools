# python module whos_client
## Metadata and data retrieval from WHOS using timeseries API
### Includes convertion to CSV tables as required by FEWS

This folder contains:
- whos_client.py -> the python module
- help.txt -> the help document for the module
- examples.py -> some working examples of the module
- cuencas/cuencas.geojson -> basins map required to assign SUBBASIN parameter to stations

## Installation
- Install and/or upgrade python (3.9 or higher)
- Copy content of this folder into a local folder
- Install dependencies (e.g. in Linux):
```
    python -m pip install requests pandas isodate datetime lxml typing geopandas shapely
```
## Usage
### from python console

    from whos_client import Client
    client = Client()o    # get stations with bounding box and pagination 
    stations = client.getMonitoringPointsWithPagination(west=-60,south=-35,east=-55,north=-30,json_output="results/mp.json",fews_output="results/mp.csv")
    #get all stations
    stations = client.getMonitoringPointsWithPagination(json_output="results/mp.json",fews_output="results/mp.csv")
    # get timeseries by station ID
    timeseies = client.getTimeseriesWithPagination(monitoringPoint="0009BBB009E7F4067B498FC0073C2AA63D064D27",json_output="results/ts.json",fews_output="results/ts.csv")
    # To download and convert all stations and time series metadata from WHOS-Plata as required by FEWS
    metadata_for_fews = client.makeFewsTables(output_dir="results")

### or from the command line:
```
    # get stations by bounding box
    python whos_client.py monitoringPoints --bbox -60 -35 -55 -30 --json results/mp.json --fews results/mp.csv
    python whos_client.py monitoringPoints --json results/mp.json --fews results/mp.csv
    #get all stations
    
    # get timeseries by station ID 
    python whos_client.py timeseries --monitoringPoint 0009BBB009E7F4067B498FC0073C2AA63D064D27 --json results/ts.json --fews results/ts.csv
    # download and convert all stations and time series metadata as required by FEWS
    python whos_client.py all -O results
    # download stations and time series metadata for a selected country and observed property as required by FEWS
    python whos_client.py all -o 02B12CBDEF3984F7ADB9CFDFBF065FC1D3AEF13F -O results2 -c URY
```

## Contact
mail to: [jbianchi@ina.gob.ar](mailto:jbianchi@ina.gob.ar)

Instituto Nacional del Agua

Argentina
