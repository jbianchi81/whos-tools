import requests
import json
import pandas
import isodate
from datetime import timedelta, datetime
from lxml import etree
from warnings import warn
from typing import Union
from pathlib import Path
from geopandas import read_file as gpd_read_file
from shapely.geometry import Point

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
        "timeseries_per_page": 1000,
        "view": "whos-plata",
        "basins_geojson_file": "cuencas/cuencas.geojson"
    }
    
    fews_var_map = {
        "Precipitation": "P",
        "Level": "H",
        "Flux, discharge": "Q"
    }

    fews_observed_properties = ["02B12CBDEF3984F7ADB9CFDFBF065FC1D3AEF13F",
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
    "472D1E733D426DC0A514D0BD6A2AAA7541CCEC3A"]
    
    def __init__(self,config: dict = None):
        """
        Parameters
        ----------
        config : dict
            Configuration parameters: url, token, monitoring_points_max, monitoring_points_per_page, timeseries_max, timeseries_per_page 
        """
        
        self.config = self.default_config
        if config is not None:
            for key in config:
                self.config[key] = config[key]
        self.basins = gpd_read_file(self.config["basins_geojson_file"])
    
    def getMonitoringPoints(self, view: str = default_config["view"],east: float = None, west: float = None, north: float = None, south: float = None, offset: int = None, limit: int = None, output: str = None) -> dict:
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
            raise Exception("request failed")
        if(response.status_code >= 400):
            raise Exception("request failed, status code: %s" % response.status_code)
        if output is not None:
            try: 
                f = open(output,"w")
            except:
                raise Exception("Couldn't open file %s for writing" % output)
            f.write(json.dumps(response.json(), indent=2, ensure_ascii=False))
            f.close()
        return response.json()
    
    def getTimeseries(self, view: str = default_config["view"], monitoringPoint: str = None, observedProperty: str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None):
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
            raise Exception("request failed")
        if(response.status_code >= 400):
            raise Exception("request failed, status code: %s" % response.status_code)
        if output is not None:
            try: 
                f = open(output,"w")
            except:
                raise Exception("Couldn't open file %s for writing" % output)
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
                raise Exception("Couldn't open file %s for writing" % output)
            f.write(data_frame.to_csv(index=False))
            f.close()
        return data_frame
    
    def getSubBasin(self,coordinates):
        # def get_region(lon,lat,getNotation=False):
        st0 = Point(coordinates[0],coordinates[1])
        subbasin = None
        for i in range(0,len(self.basins.geometry)):
            if st0.within(self.basins.geometry[i]):
                # print("is within subbasin %s" % self.basins.nombre_2[i])
                subbasin = self.basins.nombre_2[i]
        return subbasin
    
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
                raise Exception("Couldn't open file %s for writing" % output)
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
    
    def getVariableMapping(self,view=default_config["view"],output=None,output_xml=None):
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
            raise Exception("request failed")
        if(response.status_code >= 400):
            raise Exception("request failed, status code: %s" % response.status_code)
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
            output_dir = Path(output_dir)
            variableNames = set(timeseries["variableName"])
            for variableName in variableNames:
                group = timeseries[timeseries["variableName"]==variableName]
                del group["variableName"]
                f = open(output_dir / ("%s.csv" % variableName),"w")
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
        output_dir = Path(output_dir)
        monitoringPoints = self.getMonitoringPointsWithPagination(json_output = output_dir / "monitoringPoints.json" if save_geojson else None)
        stations_fews = self.monitoringPointsToFEWS(monitoringPoints)
        # get WHOS-Plata variable mapping table
        var_map = self.getVariableMapping()
        # get all WHOS-Plata timeseries metadata (using pagination)
        timeseries = self.getTimeseriesWithPagination(observedProperty=self.fews_observed_properties, json_output = output_dir / "timeseries.json" if save_geojson else None)
        timeseries_fews = self.timeseriesToFEWS(timeseries)
        # filter out stations with no timeseries
        stations_fews = self.deleteStationsWithNoTimeseries(stations_fews,timeseries_fews)
        # get organization name from timeseries metadata
        station_organization = self.getOrganization(timeseries,stations_fews)
        # save stations to csv
        f = open(output_dir / "gauges.csv","w")
        f.write(stations_fews.to_csv())
        f.close()
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
        timeseries_fews_grouped = self.groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir,fews=False) # True)
        return {"stations": stations_fews, "timeseries": timeseries_fews_grouped}
    
    def getMonitoringPointsWithPagination(self, view: str = default_config["view"],east: float = None, west: float = None, north: float = None, south: float = None, json_output: str = None, fews_output: str = None, save_geojson : bool = False, output_dir : str = "") -> dict:
        output_dir = Path(output_dir)
        stations = pandas.DataFrame(columns= ["STATION_ID", "STATION_NAME", "STATION_SHORTNAME", "TOOLTIP", "LATITUDE", "LONGITUDE", "ALTITUDE", "COUNTRY", "ORGANIZATION", "SUBBASIN"])
        features = []
        for i in range(1,self.config["monitoring_points_max"],self.config["monitoring_points_per_page"]):
            print("getMonitoringPoints offset: %i" % i)
            output = output_dir / ("monitoringPointsResponse_%i.json" % i) if save_geojson else None
            monitoringPoints = self.getMonitoringPoints(offset=i,limit=self.config["monitoring_points_per_page"],west = west, south = south, east = east, north = north, output=output)
            # convert to FEWS stations CSV, output as gauges.csv
            if "features" not in monitoringPoints:
                print("no monitoring points found")
                break
            features.extend(monitoringPoints["features"])
            if fews_output:
                stations_i = self.monitoringPointsToFEWS(monitoringPoints)
                stations= pandas.concat([stations,stations_i])
            if len(monitoringPoints["features"]) < self.config["monitoring_points_per_page"]:
                break
        result = {
            "type": "featureCollection",
            "features": features
        }
        if json_output:
            f = open(json_output,"w")
            f.write(json.dumps(result, indent=2, ensure_ascii=False))
            f.close()
        if fews_output:
            f = open(fews_output,"w")
            f.write(stations.to_csv())
            f.close()
            return stations
        else:
            return result

    def getTimeseriesMulti(self, view: str = default_config["view"], monitoringPoint: list or str = None, observedProperty: list or str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None):
        if monitoringPoint is not None:
            if type(monitoringPoint) == str:
                monitoringPoint = [monitoringPoint]
        else:
            monitoringPoint = []
        if observedProperty is not None:
            if type(observedProperty) == str:
                observedProperty = [observedProperty]
        else:
            observedProperty = []
        if len(monitoringPoint) == 0:
            if len(observedProperty) == 0:
                return self.getTimeseries( view = view, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output)
            else:
                features = []
                for op in observedProperty:
                    print("observedProperty: %s" % op)
                    timeseries = self.getTimeseries(view, observedProperty = op, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output)
                    if "features" in timeseries:
                        features.extend(timeseries["features"])
                return {
                    "type": "featureCollection",
                    "features": features
                }
        else:
            features = []
            for mp in monitoringPoint:
                print("monitoringPoint: %s" % mp)
                if len(observedProperty) == 0:
                    timeseries = self.getTimeseries( view = view, monitoringPoint = mp, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output)
                    if "features" in timeseries:
                        features.extend(timeseries["features"])
                else:
                    for op in observedProperty:
                        print("observedProperty: %s" % op)
                        timeseries = self.getTimeseries(view, monitoringPoint = mp, observedProperty = op, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output)
                        if "features" in timeseries:
                            features.extend(timeseries["features"])
            return {
                "type": "featureCollection",
                "features": features
            }

    def getTimeseriesWithPagination(self, view: str = default_config["view"], monitoringPoint: list or str = None, observedProperty: list or str = None, beginPosition: str = None, endPosition: str = None, json_output: str = None, fews_output: str = None, save_geojson : bool = False, output_dir : str = "", grouped : bool = False) -> dict:
        output_dir = Path(output_dir)
        features = []
        var_map = self.getVariableMapping()
        timeseries_fews = pandas.DataFrame(columns= ["STATION_ID", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE"])
        for i in range(1,self.config["timeseries_max"],self.config["timeseries_per_page"]):
            print("getTimeseriesMulti, offset: %i" % i)
            output = output_dir / ("timeseriesResponse_%i.json" % i) if save_geojson else None
            timeseries = self.getTimeseriesMulti(offset=i,monitoringPoint=monitoringPoint,observedProperty=observedProperty,beginPosition=beginPosition,endPosition=endPosition,limit=self.config["timeseries_per_page"],output=output)
            if "features" not in timeseries:
                print("No timeseries found")
                break
            timeseries_fews = pandas.concat([timeseries_fews,self.timeseriesToFEWS(timeseries)])
            features.extend(timeseries["features"])
            if len(timeseries["features"]) < self.config["timeseries_per_page"]:
                break
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
        result = {
            "type": "featureCollection",
            "features": features
        }
        if json_output:
            f = open(json_output,"w")
            f.write(json.dumps(result, indent=2, ensure_ascii=False))
            f.close()
        if fews_output:
            if grouped:
                timeseries_fews_grouped = self.groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir) # ,fews=True)
                f = open(fews_output,"w")
                f.write(timeseries_fews_grouped.to_csv())
                f.close()
                return timeseries_fews_grouped
            else:
                f = open(fews_output,"w")
                f.write(timeseries_fews.to_csv())
                f.close()
                return timeseries_fews
        else:
            return result
    
    def getOrganization(self,timeseries : dict,stations_fews : pandas.DataFrame = None) -> pandas.DataFrame:  # , delete_none = False
        '''Reads organisationName from timeseries object. If stations_fews provided, updates ORGANIZATION column. Returns DataFrame with columns STATION_ID,organisationName'''

        station_organization = []
        # missing_organization = []
        for f in timeseries["features"]:
            if "relatedParties" in f["properties"]["timeseries"]["featureOfInterest"] and len(f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"]) and "organisationName" in f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]:
                station_organization.append({
                    "STATION_ID": f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
                    "organisationName": f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]["organisationName"]
                })
            # else:
                # missing_organization.append({
                #     "STATION_ID": f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
                #     "PARAMETER_ID": f["properties"]["timeseries"]["observedProperty"]["href"]
                # })
        station_organization = pandas.DataFrame(station_organization).drop_duplicates(ignore_index=True)
        # missing_organization = pandas.DataFrame(missing_organization)
        if stations_fews is not None:
            merged = stations_fews.merge(station_organization,how='left', on='STATION_ID')
            stations_fews["ORGANIZATION"] = merged["organisationName"].combine_first(merged["ORGANIZATION"]) # merged["organisationName"]
            del merged
            # if delete_none:
            #     stations_fews.drop(stations_fews[stations_fews["ORGANIZATION"].isnull()].index)
        return station_organization
    
    def deleteStationsWithNoTimeseries(self,stations_fews,timeseries_fews):
        ts_st = timeseries_fews[["STATION_ID"]].drop_duplicates(ignore_index=True)
        stations_fews = stations_fews.merge(ts_st,how='inner',on="STATION_ID")
        return stations_fews


if __name__ == "__main__":
    client = Client()
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument('action',help="Action to perform. Accepts: 'monitoringPoints', 'timeseries', 'all'", type=str)
    argparser.add_argument("-u","--url", help = "base url of WHOS server. Defaults to %s" % Client.default_config["url"],type=str)
    argparser.add_argument("-t","--token", help = "user token for WHOS server. Defaults to %s" % Client.default_config["token"],type=str)
    argparser.add_argument("-m","--monitoring_points_max", help = "Maximum index number of monitoring points request. Defaults to %s" % Client.default_config["monitoring_points_max"],type=int)
    argparser.add_argument("-i","--timeseries_max", help = "Maximum index number of timeseries request. Defaults to %s" % Client.default_config["timeseries_max"],type=int)
    argparser.add_argument("-M","--monitoring_points_per_page", help = "For pagination, number of items for each monitoring points request. Defaults to %s" % Client.default_config["monitoring_points_per_page"],type=int)
    argparser.add_argument("-T","--timeseries_per_page", help = "For pagination, number of items for each timeseries request. Defaults to %s" % Client.default_config["timeseries_per_page"],type=int)
    argparser.add_argument("-v","--view",help = "WHOS view. Defaults to %s" % Client.default_config["view"],type=str)
    argparser.add_argument("-b","--bbox",help = "Geographical bounds in decimal degrees (W S E N) for monitoring points request. Default none",nargs=4,type=float)
    argparser.add_argument("-f","--offset",help = "Start position of matched records",type=int)
    argparser.add_argument("-l","--limit",help = "Maximum number of matched records",type=int)
    argparser.add_argument("-p","--monitoringPoint",help="Identifier of the monitoring point for timeseries request. Default none",type=str,nargs="+")
    argparser.add_argument("-o","--observedProperty",help="Identifier of the observed property  for timeseries request. Default none",type=str,nargs="+")
    argparser.add_argument("-B","--beginPosition",help="Temporal interval begin position for timeseries request. Default none",type=str)
    argparser.add_argument("-E","--endPosition",help="Temporal interval end position for timeseries request. Default none",type=str)
    argparser.add_argument('-j','--json', help = "write output in json format to this file",type=str)
    argparser.add_argument('-F','--fews', help = 'write output in FEWS csv format to this file',type=str)
    argparser.add_argument('-O','--output_dir',help = 'output directory for fews csv', type=str)
    args = argparser.parse_args()
    config = {}
    for key in ["url","token","monitoring_points_max","monitoring_points_per_page","timeseries_max","timeseries_per_page","view"]:
        if key in vars(args) and vars(args)[key] is not None:
            config[key] = vars(args)[key]
    client = Client(config)
    if args.action.lower() == "monitoringpoints":
        # GET MONITORING POINTS
        mp_args = {}
        if args.view:
            mp_args["view"] = args.view
        if args.bbox:
            mp_args["west"] = args.bbox[0]
            mp_args["south"] = args.bbox[1]
            mp_args["east"] = args.bbox[2]
            mp_args["north"] = args.bbox[3]
        if args.offset:
            mp_args["offset"] = args.offset
        if args.limit:
            mp_args["limit"] = args.limit
        if args.json:
            mp_args["json_output"] = args.json
        if args.fews:
            mp_args["fews_output"] = args.fews
        if "json_output" not in mp_args and "fews_output" not in mp_args:
            raise Exception("Missing arguments: at least one of json fews must be defined")
        client.getMonitoringPointsWithPagination(**mp_args)
    elif args.action.lower() == "timeseries":
        ts_args = {}
        if args.view:
            ts_args["view"] = args.view
        if args.monitoringPoint:
            ts_args["monitoringPoint"] = args.monitoringPoint
        if args.observedProperty:
            ts_args["observedProperty"] = args.observedProperty
        if args.beginPosition:
            ts_args["beginPosition"] = args.beginPosition
        if args.endPosition:
            ts_args["endPosition"] = args.endPosition
        if args.offset:
            ts_args["offset"] = args.offset
        if args.limit:
            ts_args["limit"] = args.limit
        if args.json:
            ts_args["json_output"] = args.json
        if args.fews:
            ts_args["fews_output"] = args.fews
        if "json_output" not in ts_args and "fews_output" not in ts_args:
            raise Exception("Missing arguments: at least one of json fews must be defined")
        client.getTimeseriesWithPagination(**ts_args)
    elif args.action.lower() == "all":
        all_args = {}
        if args.output_dir:
            all_args["output_dir"] = args.output_dir
        # make FEWS tables for WHOS-Plata (all stations and variables). Save into specified folder
        client.makeFewsTables(**all_args)
    else:
        raise Exception("Invalid action. Choose one of 'monitoringPoints', 'timeseries', 'all'")

