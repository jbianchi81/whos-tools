from subprocess import STD_OUTPUT_HANDLE
import requests
import json
import pandas
import isodate
from datetime import timedelta, datetime
from lxml import etree

config = {
    "url": "https://whos.geodab.eu",
    "token": "YOUR_TOKEN_HERE"
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
        f.write(data_frame.to_csv())
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
        f.write(data_frame.to_csv())
        f.close()
    return data_frame

def isoDurationToHours(aggregationDuration):
    # ACcording to: ISO 8601 (https://tc39.es/proposal-temporal/docs/duration.html)
    duration = isodate.parse_duration(aggregationDuration)
    if isinstance(duration,isodate.Duration):
        return duration.totimedelta(datetime(1970,1,1)).total_seconds() / 3600
    else:
        return duration.total_seconds() / 3600

def getVariableMapping(view="whos-plata",output=None):
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
        var_map.append({
            "variableCode": variableCode,
            "variableName": variableName
        })
    data_frame = pandas.DataFrame(var_map)
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise("Couldn't open file %s for writing" % output)
        f.write(data_frame.to_csv())
        f.close()
    return data_frame

# STATION_ID	EXTERNAL_LOCATION_ID	EXTERNAL_PARAMETER_ID	TIMESTEP_HOUR	UNIT	IMPORT_SOURCE	THRESHOLD_1	THRESHOLD_2	THRESHOLD_3	THRESHOLD_4



### EXAMPLES
# get monitoring points
monitoringPoints = getMonitoringPoints(offset=1,limit=1000,output="monitoringPoints.json")
# get monitoring points with bounding box
monitoringPoints = getMonitoringPoints(east=-57,west=-60,north=-32,south=-33,limit=10000)
north = -34.79618225195784
south = -34.92367330004518 
east = -58.37745447175688
west = -58.693466852831456
monitoringPoints = getMonitoringPoints(east=east,west=west,north=north,south=south,limit=10000,output="mp.json")
# convert to FEWS stations CSV
mp_df = monitoringPointsToFEWS(monitoringPoints,output="gauges.csv")
#get timeseries metadata
timeseries = getTimeseries(limit=1000,output="timeseries.json")
ts_df = timeseriesToFEWS(timeseries,output="timeseries.csv")
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
# get variable mapping
var_map = getVariableMapping(output="var_map.csv")