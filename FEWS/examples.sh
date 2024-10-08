!#/bin/bash

# get stations by bounding box
python3 whos_client.py monitoringPoints --bbox -60 -35 -55 -30 --json results/mp.json --fews results/mp.csv
#get all stations
python3 whos_client.py monitoringPoints --json results/mp.json --fews results/mp.csv
# get timeseries by station ID 
python3 whos_client.py timeseries --monitoringPoint 0009BBB009E7F4067B498FC0073C2AA63D064D27 --json results/ts.json --fews results/ts.csv
# download and convert all stations and time series metadata as required by FEWS
python3 whos_client.py all -O results
# download and convert stations within bounding box and time series metadata as required by FEWS
mytoken="my_token"
python3 whos_client.py all -O results/ina --bbox -70 -40 -40 -10 -t $mytoken -P argentina-ina -B "2024-03-01"
python3 whos_client.py all -O results/inmet --bbox -70 -40 -40 -10 -t $mytoken -P brazil-inmet -B "2024-03-01"
python3 whos_client.py all -O results/ana --bbox -70 -40 -40 -10 -t $mytoken -P brazil-ana -B "2024-03-01"
python3 whos_client.py all -O results/dinagua --bbox -70 -40 -40 -10 -t $mytoken -P uruguay-dinagua -B "2024-03-01"