from whos_client import Client
client = Client()

# get monitoring points
monitoringPoints = client.getMonitoringPoints(offset=1,limit=1000,output="results/monitoringPoints.json")

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
ts_df = client.timeseriesToFEWS(timeseries,output="results/timeseries.csv")

#group timeseries by variableName
ts_grouped_df = client.groupTimeseriesByVar(ts_df,var_map,output_dir="results")

#group timeseries by variableName using FEWS variable names 
ts_fews = client.groupTimeseriesByVar(ts_df,var_map,output_dir="results",fews=True)

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

