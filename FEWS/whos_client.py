import requests
import json
import pandas
import isodate
from datetime import timedelta, datetime
from lxml import etree
from warnings import warn
from typing import Union

class Client:
    """Functions for metadata retrieval from WHOS using timeseries API
    plus functions to convert from native format (geoJSON) to FEWS csv format
    
    Methods
    -------
    getMonitoringPoints(view: str = "whos-plata",east: float = None, west: float = None, north: float = None, south: float = None, offset: int = None, limit: int = None, output: str = None)
        Retrieves monitoring points as a geoJSON document from WHOS timeseries API
    getTimeseries(view: str = "whos-plata", monitoringPoint: str = None, observedProperty: str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None))
        Retrieves timeseries as a geoJSON document from WHOS timeseries API
    getVariableMapping(view='whos-plata', output=None, output_xml=None)
        Retrieves variable mapping from WHOS CUAHSI API
    monitoringPointsToFEWS(monitoringPoints, output=None)
        Converts monitoringPoints geoJSON to FEWS table 
    timeseriesToFEWS(timeseries, output=None)
        Converts timeseries geoJSON to FEWS table
    groupTimeseriesByVar(input_ts, var_map, output_dir=None, fews=False)
        Groups timeseries by observedVariable, optionally using FEWS convention
    makeFewsTables(output_dir='', save_geojson=False)
        Retrieves WHOS metadata and writes out FEWS tables
    """
    
    default_config = {
        "url": "https://whos.geodab.eu",
        "token": "YOUR_TOKEN_HERE",
        "monitoring_points_max": 6000,
        "monitoring_points_per_page": 1000,
        "timeseries_max": 48000,
        "timeseries_per_page": 1000
    }
    
    fews_var_map = {
        "Precipitation": "P",
        "Level": "H",
        "Flux, discharge": "Q"
    }
    
    def __init__(self,config: dict = None):
        """
        Parameters
        ----------
        config : dict
            Configuration parameters: url, token, monitoring_points_max, monitoring_points_per_page, timeseries_max, timeseries_per_page 
        """

        if config is not None:
            self.config = config
        else:
            self.config = self.default_config
    
    def getMonitoringPoints(self, view: str = "whos-plata",east: float = None, west: float = None, north: float = None, south: float = None, offset: int = None, limit: int = None, output: str = None) -> dict:
        """Retrieves monitoring points as a geoJSON document from the timeseries API
        
        Parameters
        ----------
        view : str
            WHOS view identifier. Default whos-plata
        east : float
            Bounding box eastern longitude coordinate
        west : float
            Bounding box western longitude coordinate
        north : float
            Bounding box northern latitude coordinate
        south : float
            Bounding box southern latitude coordinate
        offset : int
            Start position of matched records
        limit : int
            Maximum number of matched records
        output: string
            Write JSON output into this file
        
        Returns
        -------
        dict
            A dict containing geoJSON data
        """
        
        params = locals()
        del params["view"]
        del params["output"]
        url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/monitoring-points" % (self.config["url"], self.config["token"], view)
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
    
    def getTimeseries(self, view: str = "whos-plata", monitoringPoint: str = None, observedProperty: str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None):
        """Retrieves timeseries as a geoJSON document from the timeseries API

        Parameters
        ----------
        view : str
            WHOS view identifier
        monitoringPoint : str
            Identifier of the monitoring point
        observedProperty : str
            Identifier of the observed property
        beginPosition : str
            Temporal interval begin position (ISO8601 date)
        endPosition : str
            Temporal interval end position (ISO8601 date)
        offset : int
            Start position of matched records
        limit : int
            Maximum number of matched records
        output: string
            Write JSON output into this file
        
        Returns
        -------
        dict
            A dict containing geoJSON data
        """
        
        params = locals()
        del params["view"]
        del params["output"]
        url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/timeseries" % (self.config["url"], self.config["token"], view)
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
    
    def monitoringPointsToFEWS(self,monitoringPoints : Union[str, dict],output=None): 
        """Converts monitoringPoints geoJSON to FEWS table
        
        Parameters
        ----------
        monitoringPoints : dict or str
            Result of getMonitoringPoints
            If str: JSON file to read from
            If dict: getMonitoringPoints return value  
        output: string
            Write CSV output into this file
        
        Returns
        -------
        DataFrame
            A data frame of the stations in FEWS format
        """
        if isinstance(monitoringPoints,str):
            with open(monitoringPoints,"r") as f: 
                monitoringPoints = json.load(f)
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
                "SUBBASIN": self.getSubBasin(item["geometry"]["coordinates"])
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
    
    def getSubBasin(self,coordinates):
        # TODO
        return None
    
    def timeseriesToFEWS(self,timeseries : Union[str,dict], output=None):
        """Converts timeseries geoJSON to FEWS table
        
        Parameters
        ----------
        timeseries : dict or str
            Result of getTimeseries
            If str: JSON file to read from
            If dict: getTimeseries return value  
        output: string
            Write CSV output into this file
        
        Returns
        -------
        DataFrame
            A data frame of the time series in FEWS format
        """

         # timeseries: str => path to timeseries geojson file or dict => same already parsed into dict 
        if isinstance(timeseries,str):
            with open(timeseries,"r") as f: 
                timeseries = json.load(f)
        rows = []
        for item in timeseries["features"]:
            row = {
                "STATION_ID": item["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
                "EXTERNAL_LOCATION_ID": item["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
                "EXTERNAL_PARAMETER_ID": item["properties"]["timeseries"]["observedProperty"]["href"],
                "TIMESTEP_HOUR": self.isoDurationToHours(item["properties"]["timeseries"]["result"]["defaultPointMetadata"]["aggregationDuration"]) if "aggregationDuration" in item["properties"]["timeseries"]["result"]["defaultPointMetadata"] else None,
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
    
    def isoDurationToHours(self,aggregationDuration):
        # ACcording to: ISO 8601 (https://tc39.es/proposal-temporal/docs/duration.html)
        duration = isodate.parse_duration(aggregationDuration)
        if isinstance(duration,isodate.Duration):
            return duration.totimedelta(datetime(1970,1,1)).total_seconds() / 3600
        else:
            return duration.total_seconds() / 3600
    
    def getVariableMapping(self,view="whos-plata",output=None,output_xml=None):
        """Retrieves variable mapping from WHOS CUAHSI API (waterML 1.1)
        
        Parameters
        ----------
        view : str
            WHOS view idenfifier
        output : string
            Write CSV output into this file
        output_xml : string
            Write XML output into this file
        
        Returns
        -------
        DataFrame
            A data frame of the mapped observed variables
        """
        url = "%s/gs-service/services/essi/token/%s/view/%s/cuahsi_1_1.asmx" % (self.config["url"], self.config["token"], view)
        params = {
            "request": "GetVariables"
        }
        try:
            response = requests.get(url, params)
        except:
            raise("request failed")
        if(response.status_code >= 400):
            raise("request failed, status code: %s" % response.status_code)
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
    
    def groupTimeseriesByVar(self,input_ts,var_map,output_dir=None,fews=False): 
        """Groups timeseries by observedVariable, optionally using FEWS convention
        
        Parameters
        ----------
        input_ts : DataFrame
            Return value of timeseriesToFEWS()
        var_map : DataFrame
            Return value of getVariableMapping()
        output_dir : str
            Write output to this directory
        fews : bool
            Use FEWS variables naming convention
        
        Returns
        -------
        DataFrame
            A data frame of time series grouped by variable name
        """
        timeseries = input_ts.copy()
        var_dict = {}
        for i in var_map.index:
            var_dict[var_map["variableCode"][i]] = {
                "variableName": var_map["variableName"][i],
                "unitName": var_map["unitName"][i]
            }
        timeseries["variableName"] = [var_dict[variableCode]["variableName"] for variableCode in timeseries["EXTERNAL_PARAMETER_ID"]]
        timeseries["UNIT"] = [var_dict[variableCode]["unitName"] for variableCode in timeseries["EXTERNAL_PARAMETER_ID"]]
        if fews:
            timeseries["variableName"] = [self.fews_var_map[variableName] if variableName in self.fews_var_map else None for variableName in timeseries["variableName"]]
            timeseries = timeseries[timeseries["variableName"].notnull()]
        if output_dir is not None:
            variableNames = set(timeseries["variableName"])
            for variableName in variableNames:
                group = timeseries[timeseries["variableName"]==variableName]
                del group["variableName"]
                f = open("%s/%s.csv" % (output_dir, variableName),"w")
                f.write(group.to_csv(index=False))
        return timeseries

    def makeFewsTables(self,output_dir="",save_geojson=False):
        """Retrieves WHOS metadata and writes out FEWS tables
        
        Parameters
        ----------
        output_dir : str
            Write outputs in this directory. Defaults to current working directory
        save_geojson : bool
            Also writes out raw API responses (geoJSON files)
        
        Returns
        -------
        dict
            dict containing retrieved stations and timeseries in FEWS format
        """
        stations = pandas.DataFrame(columns= ["STATION_ID", "STATION_NAME", "STATION_SHORTNAME", "TOOLTIP", "LATITUDE", "LONGITUDE", "ALTITUDE", "COUNTRY", "ORGANIZATION", "SUBBASIN"])
        for i in range(1,self.config["monitoring_points_max"],self.config["monitoring_points_per_page"]):
            print("getMonitoringPoints offset: %i" % i)
            output = "%s/monitoringPointsResponse_%i.json" % (output_dir,i) if save_geojson else None
            monitoringPoints = self.getMonitoringPoints(offset=i,limit=self.config["monitoring_points_per_page"],output=output)
            # convert to FEWS stations CSV, output as gauges.csv
            if "features" not in monitoringPoints:
                print("no monitoring points found")
                continue
            stations_i = self.monitoringPointsToFEWS(monitoringPoints)
            stations= pandas.concat([stations,stations_i])
        f = open("%s/gauges.csv" % output_dir,"w")
        f.write(stations.to_csv(index=False))
        f.close()
        # get WHOS-Plata variable mapping table
        var_map = self.getVariableMapping()
        # get all WHOS-Plata timeseries metadata (using pagination)
        timeseries_fews = pandas.DataFrame(columns= ["STATION_ID", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE"])
        for i in range(1,self.config["timeseries_max"],self.config["timeseries_per_page"]):
            print("getTimeseries offset: %i" % i)
            output = "%s/timeseriesResponse_%i.json" % (output_dir,i) if save_geojson else None
            timeseries = self.getTimeseries(offset=i,limit=self.config["timeseries_per_page"],output=output)
            if "features" not in timeseries:
                print("No timeseries found")
                continue
            timeseries_fews = pandas.concat([timeseries_fews,self.timeseriesToFEWS(timeseries)])
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
        timeseries_fews_grouped = self.groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir,fews=True)
        return {"stations": stations, "timeseries": timeseries_fews_grouped}

if __name__ == "__main__":
    # make FEWS tables for WHOS-Plata (all stations and variables). Save into FEWS_MD folder
    client = Client()
    client.makeFewsTables(output_dir="results")

