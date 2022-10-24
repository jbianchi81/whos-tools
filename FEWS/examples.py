from whos_client import Client
client = Client()

# get monitoring points
monitoringPoints = client.getMonitoringPoints(offset=1,limit=100,output="results/monitoringPoints.json")

# get monitoring points with bounding box
monitoringPoints = client.getMonitoringPoints(east=-57,west=-60,north=-32,south=-33,limit=10000)
north = -34.79618225195784
south = -34.92367330004518 
east = -58.37745447175688
west = -58.693466852831456
monitoringPoints = client.getMonitoringPoints(east=east,west=west,north=north,south=south,limit=1000,output="mp.json")

# convert to FEWS stations CSV
mp_df = client.monitoringPointsToFEWS(monitoringPoints,output="results/gauges.csv")

# get variable mapping from WHOS
var_map = client.getVariableMapping(output="results/var_map.csv",output_xml="results/getVariablesResponse.xml")

#get timeseries metadata
timeseries = client.getTimeseries(limit=1000,output="results/timeseries.json")
timeseries_fews = client.timeseriesToFEWS(timeseries,output="results/timeseries.csv")

#group timeseries by variableName
ts_grouped_df = client.groupTimeseriesByVar(timeseries_fews,var_map,output_dir="results")

#group timeseries by variableName using FEWS variable names 
ts_fews = client.groupTimeseriesByVar(timeseries_fews,var_map,output_dir="results",fews=True)

# get timeseries metadata of selected monitoring point
site_code = "6640DE74329FFE939F71D366F7662275BFD62E5F"
timeseries = client.getTimeseries(monitoringPoint=site_code,output="results/timeseries.json")

# get timeseries values
site_code = "2F12488193F66939384E07C2FD757FDAF2781D52"
variable_code = "15A8E68572CD0E538D6520C8ABD34F5680CFF81A"
beginPosition = "2000-01-01"
endPosition = "2016-01-01"
timeseries = client.getTimeseries(monitoringPoint=site_code, observedProperty=variable_code, beginPosition=beginPosition, endPosition=endPosition,output="results/timeseries_with_data.json")

# create FEWS tables for WHOS-Plata (all stations and variables).
metadata_for_fews = client.makeFewsTables(output_dir="results")

# timeseries filtered by array of monitoringPoints
ts = client.getTimeseriesMulti(monitoringPoint=["FFF27D6286244D98FA3C13DCA589F70C225C41BD","FEB977ECB3A3623027BACC3A609126DBBE8418E0"])

# timeseries filtered by array of monitoringPoints with pagination
ts = client.getTimeseriesWithPagination(monitoringPoint=["FFF27D6286244D98FA3C13DCA589F70C225C41BD","FEB977ECB3A3623027BACC3A609126DBBE8418E0"])

# timeseries filtered by array of monitoringPoints with pagination, save csv fews format
ts = client.getTimeseriesWithPagination(monitoringPoint=["FFF27D6286244D98FA3C13DCA589F70C225C41BD","FEB977ECB3A3623027BACC3A609126DBBE8418E0"], fews="results/stations.csv")

# timeseries filtered by array of observedProperties with pagination
ts = client.getTimeseriesWithPagination(observedProperty=['B838A449A5FBC64CBB8A204A5CD614519EB0844A', '4E47D870E717581F520F6C4EBE8E23962A880107']) 
ts = client.getTimeseriesWithPagination(observedProperty=["02B12CBDEF3984F7ADB9CFDFBF065FC1D3AEF13F",
"AF6C35E61AC362E0151B6458DADCB032043B67EA",
"D2DB8BC2930F82D1E5EFA6B529F0262EB0FFE994",
"5C34E7D199E2643B1AA949D920942C576A406AB4",
"80B052462E277E0F7D3002CA6C67E481CD953CDC",
"AEAF735EBFC73611C8A50B6D0319BD4258B45918",
"54DF3A7E4C8ED47459CDC1E3C81DED8A48797642",
"4E47D870E717581F520F6C4EBE8E23962A880107",
"B838A449A5FBC64CBB8A204A5CD614519EB0844A",
"CEE1F03F85E1231A57C7E756527ED41B9C43820B",
"F7B4831EA151DB33AA187F9C4248506E0CE6690C",
"E02E2A436A24C1DB98D819A4705B8089856A9579",
"4E47D870E717581F520F6C4EBE8E23962A880107",
"472D1E733D426DC0A514D0BD6A2AAA7541CCEC3A"])
ts = client.getTimeseriesWithPagination(observedProperty=["0B62463A8F8DE9DBB777F970195DC02E26CC4629","02B12CBDEF3984F7ADB9CFDFBF065FC1D3AEF13F"])
len(ts["features"])
observedProperties = set([f["properties"]["timeseries"]["observedProperty"]["href"] for f in ts["features"]])
len(list(observedProperties))
monitoringPoints = set([f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"] for f in ts["features"]])
len(list(monitoringPoints))
ts_df = client.timeseriesToFEWS(ts)

# get all stations with pagination
stations = client.getMonitoringPointsWithPagination(json_output="results/stations_all.json")
stations_fews = client.monitoringPointsToFEWS(stations,output="results/stations_all.csv")

# get all timeseries with pagination
timeseries = client.getTimeseriesWithPagination(json_output="results/timeseries_all.json")

# extract organisationNames from timeseries
station_organization = client.getOrganization(timeseries)
set(station_organization["organisationName"])
merged = stations_fews.merge(station_organization,how='left', on='STATION_ID')
merged["ORGANIZATION"] = merged["ORGANIZATION_y"]

# read timeseries from file
f = open("results/timeseries_all.json","r")
import json
timeseries = json.load(f)
f.close()
len(timeseries["features"])

# read stations fews from file
import pandas
stations_fews = pandas.read_csv("results/gauges.csv")

# get organization
station_organization = client.getOrganization(timeseries,stations_fews)
stations_fews["ORGANIZATION"]
f = open("results/stations_all_org.csv","w")
f.write(stations_fews.to_csv(index=False))
f.close()

# get subbasin
subbasin = client.getSubBasin([-58,-35])

# get stations with bounding box and pagination 
stations = client.getMonitoringPointsWithPagination(east=-57,west=-60,north=-32,south=-33)
stations_fews = client.monitoringPointsToFEWS(stations)

# make fews csv stepwise
mp = client.getMonitoringPointsWithPagination(json_output="results2/mp_all.json")
stations_fews = client.monitoringPointsToFEWS(mp)
var_map = client.getVariableMapping()
    # get all WHOS-Plata timeseries metadata (using pagination)
timeseries = client.getTimeseriesWithPagination(observedProperty=client.fews_observed_properties, json_output = "results2/timeseries.json", has_data = True)
station_organization = client.getOrganization(timeseries,stations_fews)
timeseries_fews = client.timeseriesToFEWS(timeseries, stations=stations_fews)
timeseries_fews_with_timestep = client.deleteSeriesWithoutTimestep(timeseries_fews)
# filter out stations with no timeseries
stations_fews = client.deleteStationsWithNoTimeseries(stations_fews,timeseries_fews_with_timestep)
stations_fews = client.setOriginalStationId(stations_fews)
# get organization name from timeseries metadata
# save stations to csv
f = open("results2/locations.csv","w")
f.write(stations_fews.to_csv())
f.close()
timeseries_fews_with_timestep = client.setOriginalStationId(timeseries_fews_with_timestep)
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
timeseries_fews_grouped = client.groupTimeseriesByVar(timeseries_fews,var_map,output_dir="results2",fews= True) # False)