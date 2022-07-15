import requests
import pandas as pd
import json
from typing import Union
import logging

class Client:
    """Functions to retrieve metadata and data from a5 JSON API"""
    
    default_config = {
        "url":"https://alerta.ina.gob.ar/a5",
        "authenticate": False,
        "token": "",
        "timeout": 120
    }
    
    last_result = None

    def __init__(self,url : str = None, authenticate : bool = False, token : str = ""):
        """
        Parameters
        ----------
            url : str
            authenticate : bool
            token : str
        """
        
        self.config = self.default_config
        if url is not None:
            self.config["url"] = url
        if authenticate is not None:
            self.config["authenticate"] = authenticate
        if token is not None:
            self.config["token"] = token

    
    def writeLastResult(self,output : str):
        f = open(output, "w")
        if isinstance(self.last_result,pd.DataFrame):
            f.write(self.last_result.to_csv(index=False)) #,{"encoding":"utf-8"})
        else:
            f.write(json.dumps(self.last_result, ensure_ascii=False, indent=2))
        f.close()
    
    def getSeries(self,tipo : str="puntual",id : Union[int,list]=None,area_id : Union[int,list]=None,estacion_id : Union[int,list]=None,escena_id : Union[int,list]=None,var_id : Union[int,list]=None,proc_id : Union[int,list]=None,unit_id : Union[int,list]=None,fuentes_id : Union[int,list]=None,tabla : Union[str,list]=None,id_externo : Union[str,list]=None,geom : str=None,include_geom : bool=None,no_metadata : bool=None, as_DataFrame : bool=False, getStats: bool=False, getMonthlyStats: bool=False, date_range_after: str=None, getPercentiles : bool=False, percentil : Union[float,list]=None):
        params = {"id": id,"area_id": area_id,"estacion_id": estacion_id,"escena_id": escena_id,"var_id": var_id,"proc_id": proc_id,"unit_id": unit_id,"fuentes_id": fuentes_id,"tabla": tabla,"id_externo": id_externo,"geom": geom,"include_geom": include_geom,"no_metadata": no_metadata, "getStats": getStats, "getMonthlyStats": getMonthlyStats, "date_range_after": date_range_after, "getPercentiles": getPercentiles, "percentil": percentil}
        for param in ["id","area_id","estacion_id","escena_id","var_id","proc_id","unit_id","fuentes_id","tabla","id_externo", "percentil"]:
            if isinstance(params[param],list):
                # print("%s: %s" % (param,str(params[param])))
                params[param] = ",".join([str(i) for i in params[param]])
                # print("%s: %s" % (param,str(params[param])))
        headers = {}
        if self.config["authenticate"]:
            headers["Authorization"] = "Bearer %s" % self.config["token"]
        response = requests.get(
            "%s/obs/%s/series" % (self.config["url"], tipo),
            params = params,
            headers = headers,
            timeout = self.config["timeout"]
        )
        # print("status_code: %s" % response.status_code)
        # print("url: %s" % response.url)
        if response.status_code > 299:
            raise Exception(response.text)
        json_response = response.json()
        series = None
        if as_DataFrame:
            series = pd.DataFrame.from_dict(json_response)
            if tipo == "puntual":
                if no_metadata:
                    series["longitude"] = [geom["coordinates"][0] for geom in series["geom"]]
                    series["latitude"] = [geom["coordinates"][1] for geom in series["geom"]]
                    del series["geom"]
                else:
                    series["longitude"] = [estacion["geom"]["coordinates"][0] for estacion in series["estacion"]]
                    series["latitude"] = [estacion["geom"]["coordinates"][1] for estacion in series["estacion"]]
            series["timestart"] = [date_range["timestart"] for date_range in series["date_range"]]
            series["timeend"] = [date_range["timeend"] for date_range in series["date_range"]]
            series["count"] = [date_range["count"] for date_range in series["date_range"]]
            del series["date_range"]
        else:
            series = json_response
        self.last_result = series
        return series
    
    def getObs(self, series_id : int,timestart : str,timeend : str, tipo : str="puntual", as_DataFrame : bool=False):
        headers = {}
        if self.config["authenticate"]:
            headers["Authorization"] = "Bearer %s" % self.config["token"]
        response = requests.get(
            '%s/obs/%s/observaciones' % (self.config["url"], tipo),
            params={
                'series_id': series_id,
                'timestart': timestart,
                'timeend': timeend
            },
            headers=headers,
            timeout = self.config["timeout"]
        )
        if response.status_code > 299:
            raise Exception(response.text)
        json_response = response.json()
        obs = None
        if as_DataFrame:
            df_obs = pd.DataFrame.from_dict(json_response)
            df_obs['timestart'] = pd.to_datetime(df_obs['timestart']).dt.round('min')            # Fecha a formato fecha -- CAMBIADO PARA QUE CORRA EN PYTHON 3.5
            df_obs['timeend'] = pd.to_datetime(df_obs['timeend']).dt.round('min')            # Fecha a formato fecha -- CAMBIADO PARA QUE CORRA EN PYTHON 3.5
            df_obs['valor'] = df_obs['valor'].astype(float)
            # df_obs.set_index(df_obs['fecha'], inplace=True)
            # del df_obs['fecha']
            obs = df_obs
        else:
            obs = json_response
        self.last_result = obs
        return obs
    
    def getVariables(self, id : Union[int,list] = None, var : Union[str,list] = None, nombre : Union[str,list] = None, abrev : Union[str,list] = None, type : Union[str,list] = None, dataType : Union[str,list] = None, valueType : Union[str,list] = None, GeneralCategory : Union[str,list] = None, VariableName : Union[str,list] = None, SampleMedium : Union[str,list] = None, def_unit_id : Union[int,list] = None, timeSupport : Union[str,list] = None, as_DataFrame : bool=False):
        params = {"id" : id, "var" : var, "nombre" : nombre, "abrev" : abrev, "type" : type, "dataType" : dataType, "valueType" : valueType, "GeneralCategory" : GeneralCategory, "VariableName" : VariableName, "SampleMedium" : SampleMedium, "def_unit_id" : def_unit_id, "timeSupport" : timeSupport}
        for param in ["id", "var", "nombre" , "abrev" , "type", "dataType", "valueType", "GeneralCategory", "VariableName", "SampleMedium", "def_unit_id", "timeSupport"]:
            if isinstance(params[param],list):
                params[param] = ",".join([str(i) for i in params[param]])
        headers = {}
        if self.config["authenticate"]:
            headers["Authorization"] = "Bearer %s" % self.config["token"]
        response = requests.get(
            "%s/obs/variables" % self.config["url"],
            params = params,
            headers = headers,
            timeout = self.config["timeout"]
        )
        logging.debug("status_code: %s" % response.status_code)
        if response.status_code > 299:
            raise Exception(response.text)
        json_response = response.json()
        variables = None
        if as_DataFrame:
            variables = pd.DataFrame.from_dict(json_response)
        else:
            variables = json_response
        self.last_result = variables
        return variables


    def getEstaciones(self, fuentes_id : int=None, nombre : str=None, unid : int=None, id : int=None, id_externo : str=None,distrito: str = None, pais: str = None, has_obs : bool=None, real : bool=None, habilitar : bool=None, has_prono : bool=None, rio : str=None, tipo_2 : str=None, geom : str=None, propietario : str=None, automatica : bool=None, ubicacion : str=None, localidad : str=None, tabla : str=None, as_DataFrame : bool=False):
        params = {"fuentes_id" :fuentes_id, "nombre" : nombre, "unid" : unid, "id" : id, "id_externo" : id_externo,"distrito": distrito, "pais": pais, "has_obs" : has_obs, "real" : real, "habilitar" : habilitar, "has_prono" : has_prono, "rio" : rio, "tipo_2" : tipo_2, "geom" : geom, "propietario" : propietario, "automatica" : automatica, "ubicacion" : ubicacion, "localidad" : localidad, "tabla" : tabla}
        headers = {}
        if self.config["authenticate"]:
            headers["Authorization"] = "Bearer %s" % self.config["token"]
        response = requests.get(
            "%s/obs/puntual/estaciones" % (self.config["url"]),
            params = params,
            headers = headers,
            timeout = self.config["timeout"]
        )
        logging.debug("status_code: %s" % response.status_code)
        if response.status_code > 299:
            raise Exception(response.text)
        json_response = response.json()
        estaciones = None
        if as_DataFrame:
            estaciones = pd.DataFrame.from_dict(json_response)
            estaciones["longitude"] = [geom["coordinates"][0] for geom in estaciones["geom"]]
            estaciones["latitude"] = [geom["coordinates"][1] for geom in estaciones["geom"]]
            del estaciones["geom"]
        else:
            estaciones = json_response
        self.last_result = estaciones
        return estaciones
    
    def getEstadisticosMensuales(self,series_id : int, as_DataFrame=False):
        # https://alerta.ina.gob.ar/a5/obs/puntual/series/19/estadisticosMensuales?format=json
        params = {"format" :"json"}
        headers = {}
        if self.config["authenticate"]:
            headers["Authorization"] = "Bearer %s" % self.config["token"]
        response = requests.get(
            "%s/obs/puntual/series/%i/estadisticosMensuales" % (self.config["url"],series_id),
            params = params,
            headers = headers,
            timeout = self.config["timeout"]
        )
        logging.debug("status_code: %s" % response.status_code)
        if response.status_code > 299:
            raise Exception(response.text)
        json_response = response.json()
        if as_DataFrame:
            # columns = json_response["varNames"]
            if not len(json_response["values"]):
                logging.info("No quantiles found for the specified series")
                return
            return pd.DataFrame(json_response["values"])
        else:
            return json_response["values"]

if __name__ == "__main__":
    pass
    # with open("config.json") as f:
    #     config = json.load(f)

    # apiLoginParams = config["api"]

    # client = Client(url=apiLoginParams["url"])

    # estaciones = client.getEstaciones(unid=96)
    # client.writeLastResult("results/estaciones.json")
    # estaciones = client.getEstaciones(tabla="alturas_prefe",as_DataFrame=True)
    # client.writeLastResult("results/estaciones.csv")
    
    # estaciones = getEstaciones(tabla="ina_delta")
    # series = getSeries()
    # series = getSeries(id=29)
    # series = getSeries(estacion_id=29)
    # series = getSeries(var_id=58)
    # series = getSeries(tabla="ina_delta")
    # series = getSeries(tabla="ina_delta",as_DataFrame=True)
    # series = getSeries(tabla="ina_delta", no_metadata=True, as_DataFrame=True)
    # series.columns
    # series["id"]
    # set(series["estacion_id"])
    # set(series["var_id"])
    # set(series["id_externo"])
    # set(series["var_nombre"])
    # series["geom"]
    # series = getSeries(tabla="ina_delta", no_metadata=True)

    # obs = getObs(29,"2021-01-01","2022-01-01")