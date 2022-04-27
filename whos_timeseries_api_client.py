from subprocess import STD_OUTPUT_HANDLE
import requests
import json
import pandas
import isodate
from datetime import timedelta, datetime
from lxml import etree
from warnings import warn

config = {
    "url": "https://whos.geodab.eu",
    "token": "YOUR_TOKEN_HERE",
    "monitoring_points_max": 6000,
    "monitoring_points_per_page": 1000,
    "timeseries_max": 50000,
    "timeseries_per_page": 1000
}

def getMonitoringPoints(view="whos-plata",east=None, west=None, north=None, south=None, offset=1, limit=10, output=None): # returns geoJSON
    params = locals()
    del params["view"]
    del params["output"]
    url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/monitoring-points" % (config["url"], config["token"], view)
    try:
        response = requests.get(url, params)
    except:
        raise("request failed")
    if(response.status_code >= 400):
        raise("request failed, status code: %s" % response.status_code)
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise("Couldn't open file %s for writing" % output)
        f.write(json.dumps(response.json(), indent=2, ensure_ascii=False))
        f.close()
    return response.json()

def getTimeseries(view="whos-plata", monitoringPoint=None, observedProperty=None, beginPosition=None, endPosition=None, offset=1, limit=10, output=None): # returns geoJSON
    params = locals()
    del params["view"]
    del params["output"]
    url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/timeseries" % (config["url"], config["token"], view)
    try:
        response = requests.get(url, params)
    except:
        raise("request failed")
    if(response.status_code >= 400):
        raise("request failed, status code: %s" % response.status_code)
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise("Couldn't open file %s for writing" % output)
        f.write(json.dumps(response.json(), indent=2, ensure_ascii=False))
        f.close()
    return response.json()

def monitoringPointsToFEWS(monitoringPoints,output=None):
    rows = []
    for item in monitoringPoints["features"]:
        monitoring_point_parameters = {}
        for i in item["properties"]["monitoring-point"]["parameters"]:
            monitoring_point_parameters[i["name"]] = i["value"]
        row = {
            "STATION_ID": item["properties"]["monitoring-point"]["sampledFeature"]["href"],
            "STATION_NAME": item["properties"]["monitoring-point"]["sampledFeature"]["title"],
            "STATION_SHORTNAME": item["properties"]["monitoring-point"]["sampledFeature"]["title"].replace(" ","")[0:12],
            "TOOLTIP": item["properties"]["monitoring-point"]["sampledFeature"]["title"],
            "LATITUDE": item["geometry"]["coordinates"][1],
            "LONGITUDE": item["geometry"]["coordinates"][0],
            "ALTITUDE": item["geometry"]["coordinates"][2] if len(item["geometry"]["coordinates"]) > 2 else None,
            "COUNTRY": monitoring_point_parameters["country"] if "country" in monitoring_point_parameters.keys() else None,
            "ORGANIZATION": "WHOS",
            "SUBBASIN": getSubBasin(item["geometry"]["coordinates"])
        }
        rows.append(row)
    data_frame = pandas.DataFrame(rows)
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise("Couldn't open file %s for writing" % output)
        f.write(data_frame.to_csv(index=False))
        f.close()
    return data_frame

def getSubBasin(coordinates):
    # TODO
    return None

def timeseriesToFEWS(timeseries, output=None):
    rows = []
    for item in timeseries["features"]:
        row = {
            "STATION_ID": item["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
            "EXTERNAL_LOCATION_ID": item["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
            "EXTERNAL_PARAMETER_ID": item["properties"]["timeseries"]["observedProperty"]["href"],
            "TIMESTEP_HOUR": isoDurationToHours(item["properties"]["timeseries"]["result"]["defaultPointMetadata"]["aggregationDuration"]) if "aggregationDuration" in item["properties"]["timeseries"]["result"]["defaultPointMetadata"] else None,
            "UNIT": item["properties"]["timeseries"]["result"]["defaultPointMetadata"]["uom"] if "uom" in item["properties"]["timeseries"]["result"]["defaultPointMetadata"] else None,
            "IMPORT_SOURCE": "WHOS"
            # THRESHOLD_1   THRESHOLD_2	THRESHOLD_3	THRESHOLD_4 -> not present in WHOS
        }
        rows.append(row)
    data_frame = pandas.DataFrame(rows)
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise("Couldn't open file %s for writing" % output)
        f.write(data_frame.to_csv(index=False))
        f.close()
    return data_frame

def isoDurationToHours(aggregationDuration):
    # ACcording to: ISO 8601 (https://tc39.es/proposal-temporal/docs/duration.html)
    duration = isodate.parse_duration(aggregationDuration)
    if isinstance(duration,isodate.Duration):
        return duration.totimedelta(datetime(1970,1,1)).total_seconds() / 3600
    else:
        return duration.total_seconds() / 3600

def getVariableMapping(view="whos-plata",output=None,output_xml=None):
    url = "%s/gs-service/services/essi/token/%s/view/%s/cuahsi_1_1.asmx" % (config["url"], config["token"], view)
    params = {
        "request": "GetVariables"
    }
    try:
        response = requests.get(url, params)
    except:
        raise("request failed")
    if(response.status_code >= 400):
        raise("request failed, status code: %s" % response.status_code)
    # if output is not None:
    #     try: 
    #         f = open(output,"w")
    #     except:
    #         raise("Couldn't open file %s for writing" % output)
    #     f.write(response.text)
    #     f.close()
    xml_text = response.text.replace("&lt;","<").replace("&gt;",">")
    exml = etree.fromstring(xml_text.encode())
    namespaces = exml.nsmap
    namespaces["his"] = "http://www.cuahsi.org/his/1.1/ws/"
    namespaces["wml"] = "http://www.cuahsi.org/waterML/1.1/"
    variables = exml.xpath("./soap:Body/his:GetVariablesResponse/his:GetVariablesResult/wml:variablesResponse/wml:variables/wml:variable",namespaces=namespaces)
    var_map = []
    for v in variables:
        variableCode = v.find("./wml:variableCode",namespaces=namespaces).text
        variableName = v.find("./wml:variableName",namespaces=namespaces).text
        unitName = v.find("./wml:unit/wml:unitName",namespaces=namespaces).text
        var_map.append({
            "variableCode": variableCode,
            "variableName": variableName,
            "unitName": unitName
        })
    data_frame = pandas.DataFrame(var_map)
    if output_xml is not None:
        f = open(output_xml,"w")
        f.write(xml_text)
        f.close()
    if output is not None:
        f = open(output,"w")
        f.write(data_frame.to_csv(index=False))
        f.close()
    return data_frame

def groupTimeseriesByVar(input_ts,var_map,output_dir=None,fews=False):
    timeseries = input_ts.copy()
    fews_var_map = {
        "Precipitation": "P",
        "Level": "H",
        "Flux, discharge": "Q"
    }
    var_dict = {}
    for i in var_map.index:
        var_dict[var_map["variableCode"][i]] = {
            "variableName": var_map["variableName"][i],
            "unitName": var_map["unitName"][i]
        }
    timeseries["variableName"] = [var_dict[variableCode]["variableName"] for variableCode in timeseries["EXTERNAL_PARAMETER_ID"]]
    timeseries["UNIT"] = [var_dict[variableCode]["unitName"] for variableCode in timeseries["EXTERNAL_PARAMETER_ID"]]
    if fews:
        timeseries["variableName"] = [fews_var_map[variableName] if variableName in fews_var_map else None for variableName in timeseries["variableName"]]
        timeseries = timeseries[timeseries["variableName"].notnull()]
    if output_dir is not None:
        variableNames = set(timeseries["variableName"])
        for variableName in variableNames:
            group = timeseries[timeseries["variableName"]==variableName]
            del group["variableName"]
            f = open("%s/%s.csv" % (output_dir, variableName),"w")
            f.write(group.to_csv(index=False))
    return timeseries

def makeFewsTables(output_dir=""):
    # get all WHOS-Plata stations using pagination
    stations = pandas.DataFrame(columns= set(["STATION_ID", "STATION_NAME", "STATION_SHORTNAME", "TOOLTIP", "LATITUDE", "LONGITUDE", "ALTITUDE", "COUNTRY", "ORGANIZATION", "SUBBASIN"]))
    for i in range(1,config["monitoring_points_max"],config["monitoring_points_per_page"]):
        print("getMonitoringPoints offset: %i" % i)
        monitoringPoints = getMonitoringPoints(offset=i,limit=config["monitoring_points_per_page"],output="%s/monitoringPointsResponse_%i.json" % (output_dir,i))
        # convert to FEWS stations CSV, output as gauges.csv
        if "features" not in monitoringPoints:
            print("no monitoring points found")
            continue
        stations_i = monitoringPointsToFEWS(monitoringPoints)
        stations= pandas.concat([stations,stations_i])
    f = open("%s/gauges.csv" % output_dir,"w")
    f.write(stations.to_csv(index=False))
    f.close()
    # get WHOS-Plata variable mapping table
    var_map = getVariableMapping()
    # get all WHOS-Plata timeseries metadata (using pagination)
    timeseries_fews = pandas.DataFrame(columns= set(["STATION_ID", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE"]))
    for i in range(1,config["timeseries_max"],config["timeseries_per_page"]):
        print("getTimeseries offset: %i" % i)
        timeseries = getTimeseries(offset=i,limit=config["timeseries_per_page"],output="%s/timeseriesResponse_%i.json" % (output_dir,i))
        if "features" not in timeseries:
            print("No timeseries found")
            continue
        timeseries_fews = pandas.concat([timeseries_fews,timeseriesToFEWS(timeseries)])
    #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
    timeseries_fews_grouped = groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir,fews=True)
    return

### EXAMPLES
# make FEWS tables for WHOS-Plata (all stations and variables). Save into FEWS_MD folder
makeFewsTables(output_dir="FEWS_MD")

# # get monitoring points
monitoringPoints = getMonitoringPoints(offset=1,limit=1000,output="monitoringPoints.json")
# get monitoring points with bounding box
monitoringPoints = getMonitoringPoints(east=-57,west=-60,north=-32,south=-33,limit=10000)
north = -34.79618225195784
south = -34.92367330004518 
east = -58.37745447175688
west = -58.693466852831456
monitoringPoints = getMonitoringPoints(east=east,west=west,north=north,south=south,limit=10000,output="mp.json")
# convert to FEWS stations CSV
mp_df = monitoringPointsToFEWS(monitoringPoints,output="FEWS_MD/gauges.csv")
# get variable mapping
var_map = getVariableMapping(output="var_map.csv",output_xml="MD/getVariablesResponse.xml")
#get timeseries metadata
timeseries = getTimeseries(limit=1000,output="MD/timeseries.json")
ts_df = timeseriesToFEWS(timeseries,output="MD/timeseries.csv")
#group timeseries by variableName
ts_grouped_df = groupTimeseriesByVar(ts_df,var_map,output_dir="MD")
#group timeseries by variableName using FEWS variable names 
ts_fews = groupTimeseriesByVar(ts_df,var_map,output_dir="FEWS_MD",fews=True)
# get timeseries metadata of selected monitoring point
site_code = "2F12488193F66939384E07C2FD757FDAF2781D52"
site_code = "6640DE74329FFE939F71D366F7662275BFD62E5F"
timeseries = getTimeseries(monitoringPoint=site_code,output="timeseries.json")
item = timeseries["features"][0]
# get timeseries data
site_code = "2F12488193F66939384E07C2FD757FDAF2781D52"
variable_code = "15A8E68572CD0E538D6520C8ABD34F5680CFF81A"
beginPosition = "2000-01-01"
endPosition = "2016-01-01"
timeseries = getTimeseries(monitoringPoint=site_code, observedProperty=variable_code, beginPosition=beginPosition, endPosition=endPosition,output="timeseries_with_data.json")
