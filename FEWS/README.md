# python module whos_client
## Metadata and data retrieval from WHOS using timeseries API
### Includes convertion to CSV tables as required by FEWS

This folder contains:
- whos_client.py -> the python module
- help.txt -> the help document for the module
- examples.py -> some working examples of the module

## Installation
- Install and/or upgrade python (3.9 or higher)
- Copy content of this folder into a local folder
- Install dependencies (e.g. in Linux):
```
    python -m pip install requests pandas isodate datetime lxml typing
```
## Usage
To download and convert all stations and time series metadata from WHOS-Plata, create a folder for outputs (e.g. "results") and run this in a python console:

    from whos_client import Client
    client = Client()
    metadata_for_fews = client.makeFewsTables(output_dir="results")

Or directly from the command line:
```
    python whos_client.py
```

## Contact
mail to: [jbianchi@ina.gob.ar](mailto:jbianchi@ina.gob.ar)
Instituto Nacional del Agua
Argentina
