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
import pytz
import re
import sys
import logging
logging.basicConfig(filename="log/whos_client.log",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(message)s")
handler = logging.FileHandler("log/whos_client.log","w+")

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
        "basins_geojson_file": "cuencas/cuencas.geojson",
        "begin_days": 120 
    }
    
    fews_var_map = {
        "Precipitation": "P",
        "PRECIPITAÇÃO TOTAL, DIARIO": "P",
        "PRECIPITAÇÃO TOTAL, MENSAL": "P",
        "PRECIPITAÇÃO TOTAL, HORÁRIO": "P",
        "PRECIPITAÇÃO TOTAL, MENSAL (AUT)": "P",
        "PRECIPITAÇÃO TOTAL, DIARIO (AUT)": "P",
        "totalPrecipitationPast24Hours": "P",
        "totalPrecipitationOrTotalWaterEquivalent": "P",
        "Amount of precipitation": "P",
        "Intensidad de la Precipitación": "P",
        "Chuva": "P",
        "nivel agua": "H",
        "Level": "H",
        "Gage height, stream": "H",
        "Stream level": "H",
        "Nivel de Río": "H",
        "Flux, discharge": "Q",
        "Inflow discharge (to the reservoir)": "Q",
        "Outflow discharge (from the reservoir)": "Q",
        "Stream discharge": "Q",
        "Vazao": "Q",
        "Volume": "volume",
        "Reservoir storage": "storage",
        "Air temperature (at specified distance from reference surface)": "temperature",
        "Velocity, wind": "wind velocity",
        "Velocidad del Viento": "wind velocity",
        "Intensidad del Viento": "wind velocity",
        "Velocidad media del viento": "wind velocity",
        "Wind direction": "wind direction",
        "Dirección del Viento": "wind direction",
        "vento, direção": "wind direction",
        "Atmospheric pressure": "atmospheric pressure",
        "Pressure": "atmospheric pressure",
        "Presión a Nivel de Estación": "atmospheric pressure",
        "Pressure reduced to mean sea level": "pressure reduced to mean sea level",
        "Presión a Nivel del Mar": "pressure reduced to mean sea level",
        "Radiation, Global": "solar radiation",
        "Radiación": "solar radiation",
        "Relative humidity": "relative humidity",
        "Snow depth": "snow depth",
        "Temperature, dew point": "dew point temperature",
        "Punto de rocio": "dew point temperature",
        "Temperatura de Rocio": "dew point temperature",
        "Temperature, water": "water temperature",
        "Temperatura del Agua": "water temperature",
        "Sunshine duration": "sunshine duration",
        "Evapotranspiration, actual": "real evapotranspiration",
        "Evapotranspiration, potential": "potential evapotranspiration",
        "Dry bulb temperature": "temperature",
        "Temperatura Media": "temperature",
        "Temperatura del Aire": "temperature",
        "Total reservoir capacity": "total reservoir capacity",
        "Inflow discharge (to the reservoir)": "inflow discharge",
        "Outflow discharge (from the reservoir)": "outflow discharge",
        "Wet bulb temperature": "wet bulb temperature",
        "Temperatura del Bulbo Húmedo": "wet bulb temperature",
        "Soil Moisture": "soil moisture",
        "Evapotranspiración": "real evapotranspiration"
    }

    fews_series_columns = {
        "P": ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN","PARENT_ID", "CHILD_ID"],
        "H": ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN","PARENT_ID", "CHILD_ID"],
        "Q": ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN","PARENT_ID", "CHILD_ID"]
    }

    fews_observed_properties = set(["02B12CBDEF3984F7ADB9CFDFBF065FC1D3AEF13F",
    "AF6C35E61AC362E0151B6458DADCB032043B67EA",
    "D2DB8BC2930F82D1E5EFA6B529F0262EB0FFE994",
    "5C34E7D199E2643B1AA949D920942C576A406AB4",
    "80B052462E277E0F7D3002CA6C67E481CD953CDC",
    "AEAF735EBFC73611C8A50B6D0319BD4258B45918",
    "54DF3A7E4C8ED47459CDC1E3C81DED8A48797642",
    "4E47D870E717581F520F6C4EBE8E23962A880107",
    "B838A449A5FBC64CBB8A204A5CD614519EB0844A",
    "CEE1F03F85E1231A57C7E756527ED41B9C43820B",
    "47BEF10439748057FFDEAC0E655B86E41E327C0B",
    "435723334E47400E56AAD9E883BC3314709A48D9",
    "7075721786F52D17847D1D6369D0F3E794795E5C",
    "B9240615C51DA438E9E02C9858887F1EE726EE2E",
    "C83AE7E059AF1DB0D94C4036CADEA3B613204DE7",
    "CD59B39DBBD02109859570B58EAA33621CD1F85B",
    "E1BC0D40B78608E3806DA51ED4F5CDE1852EC46E",
    "FA25FBBAD411D74E98BB9B663FA849BF03D0CEDE",
    "42E0859F19D8D3EEAD80DD47CE7A7B5DC94BB52C",
    "49934E86375E010327D50C168DE5EDA6FE12FDF3",
    "6A1207635C2220886869225C67B8F68E0627DDE3",
    "1B4FAD27021F87C7B176D4C5178619495C0777A4",
    "4847D38D5EC11F3A87B3821604DDC622DF6FB5CB",
    "514DB034F5483F30026291BB981096B437EFE11F",
    "6AA6C8AF684BF2DAC15E4B7D7B38EC2FF631A3A8",
    "8ABB624B5F6CE317BEC0D14A4B1AB7F121A3B1F2",
    "8D3F156BB84DC053CB935F58D0F7FB6F816BB38E",
    "941CC3F495C4F4D36921A953A419B73396362B7D",
    "F923311DA0ACB1793D8E5A053F2335192518CDAE",
    "973453E5B8A696A9C7FC01EFC5B6EA5D536A1107"])

    fews_organization_map = {
        "National Water Agency of Brazil": "ANA", # provider: brazil-ana
        "INMET": "INMET", # provider: brazil-inmet
        "Dirección de Meteorología e Hidrología (DMH), Paraguay": "DMH", # provider: paraguay-dmh
        "Instituto Nacional del Agua (INA)": "INA", # provider: argentina-ana
        "Hydrologic Research Center (HRC)": "PROHMSAT", # provider: ?
        "SAR - Agência Nacional de Águas (ANA)": "SAR" # provider: brazil-ana-sar
    }

    default_var_map = [
        {
            "variableCode": "http://hydro.geodab.eu/hydro-ontology/concept/3",
            "variableName": "Level",
            "unitName": None
        },
        {
            "variableCode": "http://hydro.geodab.eu/hydro-ontology/concept/76",
            "variableName": "Flux, discharge",
            "unitName": None
        },
        {
            "variableCode": "https://hydro.geodab.eu/hydro-ontology/concept/65",
            "variableName": "Precipitation",
            "unitName": None
        }
    ]
    
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
        self.threshold_begin_date = datetime.now() - timedelta(days=self.config["begin_days"])
        self.threshold_begin_date = pytz.utc.localize(self.threshold_begin_date)
        self.fews_observed_properties = set(self.config["fews_observed_properties"]) if "fews_observed_properties" in self.config else self.fews_observed_properties 
    
    def getMonitoringPoints(self, view: str = default_config["view"],east: float = None, west: float = None, north: float = None, south: float = None, offset: int = None, limit: int = None, output: str = None, country: str = None, provider : str = None) -> dict:
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
        country: string
            Country code (ISO3)
        provider: string
            Provider code
        
        Returns
        -------
        dict
            A dict containing geoJSON data
        """
        
        params = locals()
        del params["view"]
        del params["output"]
        del params["self"]
        for key in ["east","west","north","south","limit","offset","country","provider"]:
            if params[key] == None:
                del params[key]
        params["outputProperties"] = "country,monitoringPointOriginalIdentifier"
        url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/monitoring-points" % (self.config["url"], self.config["token"], view)
        logging.debug("url: %s" % url)
        logging.debug(str({"params": params}))
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
    
    def getTimeseries(self, view: str = default_config["view"], monitoringPoint: str = None, observedProperty: str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None, has_data = False, provider : str = None):
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
        del params["self"]
        del params["has_data"]
        for key in ["monitoringPoint","observedProperty","beginPosition","endPosition","limit","offset","provider"]:
            if params[key] == None:
                del params[key]
        url = "%s/gs-service/services/essi/token/%s/view/%s/timeseries-api/timeseries" % (self.config["url"], self.config["token"], view)
        # print("url: %s" % url)
        logging.debug("%s - %s?%s" % (str(datetime.now()), url, "&".join([ "%s=%s" % (key, params[key]) for key in params])))
        try:
            response = requests.get(url, params)
        except:
            raise Exception("request failed")
        if(response.status_code >= 400):
            raise Exception("request failed, status code: %s" % response.status_code)
        # filter out features with no data
        # xprint("%s - Elapsed: %s" % (str(datetime.now()),str(response.elapsed)))
        result = response.json()
        if has_data and "member" in result:
            result["member"] = self.filterByAvailability(result["member"],self.threshold_begin_date) 
        if output is not None:
            try: 
                f = open(output,"w")
            except:
                raise Exception("Couldn't open file %s for writing" % output)
            f.write(json.dumps(result, indent=2, ensure_ascii=False))
            f.close()
        return result
    
    def monitoringPointsToFEWS(self,monitoringPoints : Union[str, dict],output=None): 
        """Converts monitoringPoints JSON to FEWS table
        
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
        for item in monitoringPoints["results"]:
            monitoring_point_parameters = {}
            for i in item["parameter"]:
                monitoring_point_parameters[i["name"]] = i["value"]
            row = {
                "STATION_ID": item["id"],
                "STATION_NAME": item["name"],
                "STATION_SHORTNAME": item["name"].replace(" ","")[0:12],
                "TOOLTIP": item["name"],
                "LATITUDE": item["shape"]["coordinates"][1],
                "LONGITUDE": item["shape"]["coordinates"][0],
                "ALTITUDE": item["shape"]["coordinates"][2] if len(item["shape"]["coordinates"]) > 2 else None,
                "TYPE": None,
                "COUNTRY": monitoring_point_parameters["country"] if "country" in monitoring_point_parameters.keys() else None,
                "ORGANIZATION": self.getOrganizationCode(item["relatedParty"][0]["organisationName"]) if len(item["relatedParty"]) else "WHOS",
                "SUBBASIN": self.getSubBasin(item["shape"]["coordinates"]),
                "ORIGINAL_STATION_ID" : re.sub("^.*\:","",monitoring_point_parameters["identifier"]) if "identifier" in monitoring_point_parameters.keys() else None
            }
            row["PARENT_ID"] = "%s_%s_%s" % (row["COUNTRY"].upper()[0:2] if row["COUNTRY"] is not None else "", row["ORGANIZATION"], str(row["ORIGINAL_STATION_ID"]))
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
                subbasin = self.basins.nombre_3[i]
        return subbasin
    
    def timeseriesToFEWS(self,timeseries : Union[str,dict], output=None, stations=None):
        """Converts timeseries geoJSON to FEWS table
        
        Parameters
        ----------
        timeseries : dict or str
            Result of getTimeseries
            If str: JSON file to read from
            If dict: getTimeseries return value  
        output: string
            Write CSV output into this file
        stations: DataFrame
        result of monitoringPointsToFews
        if not None adds station metadata to series
    
        
        Returns
        -------
        DataFrame
            A data frame of the time series in FEWS format
        """
        if stations is not None:
            if stations.index.name != 'STATION_ID':
                stations = stations.set_index("STATION_ID")

         # timeseries: str => path to timeseries geojson file or dict => same already parsed into dict 
        if isinstance(timeseries,str):
            with open(timeseries,"r") as f: 
                timeseries = json.load(f)
        rows = []
        for item in timeseries["member"]:
            timestep_hour = self.isoDurationToHours(item["result"]["defaultPointMetadata"]["aggregationDuration"]) if "aggregationDuration" in item["result"]["defaultPointMetadata"] else self.isoDurationToHours(item["result"]["metadata"]["intendedObservationSpacing"]) if "metadata" in item["result"] and "intendedObservationSpacing" in item["result"]["metadata"] else None
            row = {
                "STATION_ID": item["featureOfInterest"]["href"],
                "EXTERNAL_LOCATION_ID": item["featureOfInterest"]["href"],
                "EXTERNAL_PARAMETER_ID": item["observedProperty"]["href"],
                "TIMESTEP_HOUR": timestep_hour,
                "UNIT": item["result"]["defaultPointMetadata"]["uom"] if "uom" in item["result"]["defaultPointMetadata"] else None,
                "IMPORT_SOURCE": "WHOS",
                "IMPORT": True
                # THRESHOLD_1   THRESHOLD_2	THRESHOLD_3	THRESHOLD_4 -> not present in WHOS
            }
            if stations is not None:
                if row["STATION_ID"] not in stations.index:
                    logging.warning("STATION_ID %s not found in stations" % row["STATION_ID"])
                    continue
                row["STATION_NAME"] =  stations["STATION_NAME"][row["STATION_ID"]]
                row["LATITUDE"] = stations["LATITUDE"][row["STATION_ID"]]
                row["LONGITUDE"] = stations["LONGITUDE"][row["STATION_ID"]]
                row["ALTITUDE"] = stations["ALTITUDE"][row["STATION_ID"]]
                row["TYPE"] = stations["TYPE"][row["STATION_ID"]]
                row["COUNTRY"] = stations["COUNTRY"][row["STATION_ID"]]
                row["ORGANIZATION"] = stations["ORGANIZATION"][row["STATION_ID"]]
                row["SUBBASIN"] = stations["SUBBASIN"][row["STATION_ID"]]
                row["ORIGINAL_STATION_ID"] = stations["ORIGINAL_STATION_ID"][row["STATION_ID"]]
                row["PARENT_ID"] = "%s_%s_%s"  % (row["COUNTRY"].upper()[0:2] if row["COUNTRY"] is not None else "", row["ORGANIZATION"], row["ORIGINAL_STATION_ID"])
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
        var_map = self.default_var_map
        for v in variables:
            variableCode = v.find("./wml:variableCode",namespaces=namespaces).text
            variableName = v.find("./wml:variableName",namespaces=namespaces).text
            unitName = v.find("./wml:unit/wml:unitName",namespaces=namespaces).text
            var_map.append({
                "variableCode": variableCode,
                "variableName": variableName,
                "unitName": unitName
            })
            if variableName in self.fews_var_map:
                self.fews_observed_properties.add(variableCode)
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
    
    def groupTimeseriesByVar(self,input_ts,var_map,output_dir=None,fews=False, set_child_id=True): 
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
        variable_name_column = []
        unit_column = []
        for variableCode in timeseries["EXTERNAL_PARAMETER_ID"]:
            if variableCode not in var_dict:
                logging.warning("Missing variable code " + variableCode + " from variable map")
                variable_name_column.append("Unknown")
                unit_column.append("Unknown")
            else:
                variable_name_column.append(var_dict[variableCode]["variableName"])
                unit_column.append(var_dict[variableCode]["unitName"])
        timeseries["variableName"] = variable_name_column
        timeseries["UNIT"] = timeseries["UNIT"].combine_first(pandas.Series(unit_column))
        if fews:
            timeseries["variableName"] = [self.fews_var_map[variableName] if variableName in self.fews_var_map else None for variableName in timeseries["variableName"]]
            timeseries = timeseries[timeseries["variableName"].notnull()]
            if set_child_id:
                timeseries["CHILD_ID"] = timeseries["PARENT_ID"] + "_WHO_" + timeseries["TIMESTEP_HOUR"].astype(int).astype(str) + "_" + timeseries["variableName"]
        if output_dir is not None:
            output_dir = Path(output_dir)
            variableNames = set(timeseries["variableName"])
            for variableName in variableNames:
                group = timeseries[timeseries["variableName"]==variableName]
                del group["variableName"]
                if fews and variableName in self.fews_series_columns:
                    fews_group = pandas.DataFrame(columns=self.fews_series_columns[variableName])
                    for column in fews_group.columns:
                        fews_group[column] = group[column] if column in group else None
                    group = fews_group
                f = open(output_dir / ("%s.csv" % variableName),"w")
                f.write(group.to_csv(index=False))
        return timeseries

    def makeFewsTables(self,output_dir="",save_geojson=False,has_data=True,observedProperty=None,country=None,has_timestep=True,east=None,west=None,north=None,south=None, provider : str = None):
        """Retrieves WHOS metadata and writes out FEWS tables
        
        Parameters
        ----------
        output_dir : str
            Write outputs in this directory. Defaults to current working directory
        save_geojson : bool
            Also writes out raw API responses (geoJSON files)
        observedProperty: list or str
        country: str - country code (ISO3)
        has_timestep: bool
            filter out series without timestep. Default True
        
        Returns
        -------
        dict
            dict containing retrieved stations and timeseries in FEWS format
        """
        output_dir = Path(output_dir)
        # get WHOS-Plata variable mapping table
        var_map = self.getVariableMapping()
        # if observedProperty and provider are both None, sets list of default observed properties to iterate over (to avoid huge load) 
        observedProperty = observedProperty if observedProperty is not None else None if provider is not None else list(self.fews_observed_properties)
        monitoringPoints = self.getMonitoringPointsWithPagination(
            json_output = output_dir / "monitoringPoints.json" if save_geojson else None,
            country = country,
            east=east,
            west=west,
            north=north,
            south=south, 
            provider = provider)
        stations_fews = self.monitoringPointsToFEWS(monitoringPoints)
        # get all WHOS-Plata timeseries metadata (using pagination)
        timeseries = self.getTimeseriesWithPagination(
            observedProperty=observedProperty, 
            json_output = Path(output_dir, "timeseries.json") if save_geojson else None, 
            has_data = has_data,
            provider = provider)
        logging.debug("timeseries length: %i" % len(timeseries["member"]))
        # station_organization = self.getOrganization(timeseries,stations_fews)
        timeseries_fews = self.timeseriesToFEWS(
            timeseries, 
            stations = stations_fews, 
            output = Path(output_dir, "timeseries.csv") if output_dir is not None and save_geojson else None
        )
        logging.debug("timeseries_fews length: %i" % len(timeseries_fews))
        timeseries_fews = self.deleteSeriesWithoutTimestep(timeseries_fews) if has_timestep else timeseries_fews  
        logging.debug("timeseries_fews with timestep length: %i" % len(timeseries_fews))
        if len(timeseries_fews) == 0:
            logging.error("No timeseries found")
            sys.exit("No timeseries found")
        # filter out stations with no timeseries
        stations_fews = self.deleteStationsWithNoTimeseries(stations_fews,timeseries_fews)
        stations_fews = self.setOriginalStationId(stations_fews)
        # get organization name from timeseries metadata
        # save stations to csv
        f = open(output_dir / "locations.csv","w")
        f.write(stations_fews.to_csv())
        f.close()
        timeseries_fews = self.setOriginalStationId(timeseries_fews)
        # timeseries_fews["PARENT_ID"] = [str(row["COUNTRY"].upper()[0:2] if row["COUNTRY"] is not None else "") + "_" + row["ORGANIZATION"] + "_" + row["STATION_ID"] for i, row in timeseries_fews.iterrows()]
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
        timeseries_fews_grouped = self.groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir,fews= True) # False)
        return {"stations": stations_fews, "timeseries": timeseries_fews_grouped}
    
    def setOriginalStationId(self,stations_or_timeseries_fews):
        stations_or_timeseries_fews_original_id = stations_or_timeseries_fews.assign(STATION_ID=stations_or_timeseries_fews["ORIGINAL_STATION_ID"])
        del stations_or_timeseries_fews_original_id["ORIGINAL_STATION_ID"]
        return stations_or_timeseries_fews_original_id

    def deleteSeriesWithoutTimestep(self,timeseries_fews):
        if len(timeseries_fews) == 0:
            return timeseries_fews
        return timeseries_fews[~pandas.isna(timeseries_fews["TIMESTEP_HOUR"])]

    def getMonitoringPointsWithPagination(self, view: str = default_config["view"],east: float = None, west: float = None, north: float = None, south: float = None, json_output: str = None, fews_output: str = None, save_geojson : bool = False, output_dir : str = "",country: str = None, provider : str = None) -> dict:
        output_dir = Path(output_dir)
        stations = pandas.DataFrame(columns= ["STATION_ID", "STATION_NAME", "STATION_SHORTNAME", "TOOLTIP", "LATITUDE", "LONGITUDE", "ALTITUDE", "COUNTRY", "ORGANIZATION", "SUBBASIN"])
        results = []
        for i in range(1,self.config["monitoring_points_max"],self.config["monitoring_points_per_page"]):
            logging.debug("getMonitoringPoints offset: %i" % i)
            output = output_dir / ("monitoringPointsResponse_%i.json" % i) if save_geojson else None
            monitoringPoints = self.getMonitoringPoints(offset=i,limit=self.config["monitoring_points_per_page"],west = west, south = south, east = east, north = north, output=output, country = country, provider = provider)
            # convert to FEWS stations CSV, output as gauges.csv
            if "results" not in monitoringPoints:
                logging.debug("no monitoring points found")
                break
            results.extend(monitoringPoints["results"])
            if fews_output:
                stations_i = self.monitoringPointsToFEWS(monitoringPoints)
                stations= pandas.concat([stations,stations_i])
            if len(monitoringPoints["results"]) < self.config["monitoring_points_per_page"]:
                break
        result = {
            # "type": "featureCollection",
            "results": results
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

    def getTimeseriesMulti(self, view: str = default_config["view"], monitoringPoint: list or str = None, observedProperty: list or str = None, beginPosition: str = None, endPosition: str = None, offset: int = 1, limit: int = 10, output: str = None, has_data=False, provider : str = None):
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
                return self.getTimeseries( view = view, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output, has_data=has_data, provider = provider)
            else:
                member = []
                for op in observedProperty:
                    logging.debug("observedProperty: %s" % op)
                    timeseries = self.getTimeseries(view, observedProperty = op, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output, has_data=has_data, provider = provider)
                    if "member" in timeseries:
                        member.extend(timeseries["member"])
                return {
                    # "type": "featureCollection",
                    "member": member
                }
        else:
            member = []
            for mp in monitoringPoint:
                logging.debug("monitoringPoint: %s" % mp)
                if len(observedProperty) == 0:
                    timeseries = self.getTimeseries( view = view, monitoringPoint = mp, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output, has_data=has_data)
                    if "member" in timeseries:
                        member.extend(timeseries["member"])
                else:
                    for op in observedProperty:
                        logging.debug("observedProperty: %s" % op)
                        timeseries = self.getTimeseries(view, monitoringPoint = mp, observedProperty = op, beginPosition = beginPosition, endPosition = endPosition, offset = offset, limit = limit, output = output, has_data=has_data)
                        if "member" in timeseries:
                            member.extend(timeseries["member"])
            return {
                # "type": "featureCollection",
                "member": member
            }

    def getTimeseriesWithPagination(self, view: str = default_config["view"], monitoringPoint: list or str = None, observedProperty: list or str = None, beginPosition: str = None, endPosition: str = None, json_output: str = None, fews_output: str = None, save_geojson : bool = False, output_dir : str = "", grouped : bool = False, has_data : bool = True, provider : str = None) -> dict:
        output_dir = Path(output_dir)
        member = []
        var_map = self.getVariableMapping()
        timeseries_fews = None # pandas.DataFrame(columns= ["STATION_ID", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE"])
        for i in range(1,self.config["timeseries_max"],self.config["timeseries_per_page"]):
            logging.debug("getTimeseriesMulti, offset: %i" % i)
            output = output_dir / ("timeseriesResponse_%i.json" % i) if save_geojson else None
            timeseries = self.getTimeseriesMulti(offset=i,monitoringPoint=monitoringPoint,observedProperty=observedProperty,beginPosition=beginPosition,endPosition=endPosition,limit=self.config["timeseries_per_page"],output=output,has_data=False, provider = provider)
            if "member" not in timeseries:
                logging.debug("No timeseries found")
                break
            timeseries_length = len(timeseries["member"])
            logging.debug("Found %i members" % timeseries_length)
            if has_data:
                timeseries["member"] = self.filterByAvailability(timeseries["member"],self.threshold_begin_date)
            logging.debug("Offset: %i, length: %i, got %i timeseries after filtering" % (i,self.config["timeseries_per_page"],len(timeseries["member"])))
            timeseries_fews = pandas.concat([timeseries_fews,self.timeseriesToFEWS(timeseries)]) if timeseries_fews is not None else self.timeseriesToFEWS(timeseries)
            member.extend(timeseries["member"])
            if timeseries_length < self.config["timeseries_per_page"]:
                logging.debug("last page, breaking")
                break
        #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
        result = {
            # "type": "featureCollection",
            "member": member
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
        
    def getOrganizationCode(self, organizationName : str) -> str:
        if organizationName not in self.fews_organization_map:
            return self.fews_organization_map[organizationName].upper().replace(" ","")[0:5]
        return self.fews_organization_map[organizationName]
    
    def getOrganization(self,timeseries : dict,stations_fews : pandas.DataFrame = None, fews=True) -> pandas.DataFrame:  # , delete_none = False
        '''Reads organisationName from timeseries object. If stations_fews provided, updates ORGANIZATION column. Returns DataFrame with columns STATION_ID,organisationName'''

        station_organization = []
        # missing_organization = []
        for f in timeseries["features"]:
            if "relatedParties" in f["properties"]["timeseries"]["featureOfInterest"] and len(f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"]) and "organisationName" in f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]:
                organizationName = f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]["organisationName"]
                if fews and organizationName in self.fews_organization_map:
                    organizationName = self.fews_organization_map[organizationName]
                station_organization.append({
                    "STATION_ID": f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
                    "organisationName": organizationName
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

    def filterByAvailability(self,members,threshold_begin_date):
        return [x for x in members if "phenomenonTime" in x and datetime.fromisoformat(x["phenomenonTime"]["end"].replace("Z","+00:00")) >=  threshold_begin_date]


if __name__ == "__main__":
    config = open("config.json")
    config = json.load(config)
    client = Client(config)
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
    argparser.add_argument('-c','--country',help = 'country code (ISO3)', type=str)
    argparser.add_argument('-P','--provider',help = 'provider code (i.e.: argentina-ina)', type=str)
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
        if args.country:
            mp_args["country"] = args.country
        if args.provider:
            mp_args["provider"] = args.provider
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
        if args.provider:
            ts_args["provider"] = args.provider
        if "json_output" not in ts_args and "fews_output" not in ts_args:
            raise Exception("Missing arguments: at least one of json fews must be defined")
        client.getTimeseriesWithPagination(**ts_args)
    elif args.action.lower() == "all":
        all_args = {
            "save_geojson": True
        }
        if args.output_dir:
            all_args["output_dir"] = args.output_dir
        if args.country:
            all_args["country"] = args.country
        if args.observedProperty:
            all_args["observedProperty"] = args.observedProperty
        if args.bbox:
            all_args["west"] = args.bbox[0]
            all_args["south"] = args.bbox[1]
            all_args["east"] = args.bbox[2]
            all_args["north"] = args.bbox[3]
        if args.provider:
            all_args["provider"] = args.provider
        # make FEWS tables for WHOS-Plata (all stations and variables). Save into specified folder
        client.makeFewsTables(**all_args)
    else:
        raise Exception("Invalid action. Choose one of 'monitoringPoints', 'timeseries', 'all'")

