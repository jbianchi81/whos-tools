from typing import Union
from pathlib import Path
from geopandas import read_file as gpd_read_file
from shapely.geometry import Point
import json
import pandas
from warnings import warn
import logging
import sys

config = {
    "basins_geojson_file": "cuencas/cuencas.geojson"
}

basins = gpd_read_file(config["basins_geojson_file"])

internal_var_ids = {
    1: "P",
    2: "H",
    4: "Q",
    39: "H",
    40: "Q",
    31: "P",
    101: "H",
    85: "H"
}


def interval2epoch(interval):
    seconds = 0
    for k in interval:
        if k == "milliseconds" or k == "millisecond":
            seconds = seconds + interval[k] * 0.001
        elif k == "seconds" or k == "second":
            seconds = seconds + interval[k]
        elif k == "minutes" or k == "minute":
            seconds = seconds + interval[k] * 60
        elif k == "hours" or k == "hour":
            seconds = seconds + interval[k] * 3600
        elif k == "days" or k == "day":
            seconds = seconds + interval[k] * 86400
        elif k == "weeks" or k == "week":
            seconds = seconds + interval[k] * 86400 * 7
        elif k == "months" or k == "month" or k == "mon":
            seconds = seconds + interval[k] * 86400 * 31
        elif k == "years" or k == "year":
            seconds = seconds + interval[k] * 86400 * 365
    return seconds

def estacionesToFews(estaciones : Union[str, list],output=None): 
    """Converts a5 estaciones to FEWS table
    
    Parameters
    ----------
    estaciones : dict or str
        Result of a5_client.getEstaciones
        If str: JSON file to read from
        If list: a5_client.getEstaciones 
    output: string
        Write CSV output into this file
    
    Returns
    -------
    DataFrame
        A data frame of the stations in FEWS format
    """
    if isinstance(estaciones,str):
        with open(estaciones,"r") as f: 
            estaciones = json.load(f)
    rows = []
    for item in estaciones:
        if not isinstance(item["geom"]["coordinates"][0],float):
            warn("Station %i with no geometry. Skipping" % item["id"])
            continue
        row = {
            "STATION_ID": item["id"],
            "STATION_NAME": item["nombre"],
            "STATION_SHORTNAME": item["abreviatura"] if item["abreviatura"] is not None else item["nombre"].replace(" ","")[0:12],
            "TOOLTIP": "station %i: %s%s" % (item["id"], item["nombre"], (" - property of %s" % item["propietario"]) if item["propietario"] is not None else ""),
            "LATITUDE": item["geom"]["coordinates"][1],
            "LONGITUDE": item["geom"]["coordinates"][0],
            "ALTITUDE": item["altitud"],
            "TYPE": "Automatic" if item["automatica"] else "Conventional",
            "COUNTRY": item["pais"],
            "ORGANIZATION": "INA",
            "SUBBASIN": getSubBasin(item["geom"]["coordinates"]),
            "PARENT_ID": "AR_INA_%i" % item["id"]
        }
        rows.append(row)
    data_frame = pandas.DataFrame(rows).sort_values("STATION_ID")
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise Exception("Couldn't open file %s for writing" % output)
        f.write(data_frame.to_csv(index=False))
        f.close()
    return data_frame

def getSubBasin(coordinates):
    # def get_region(lon,lat,getNotation=False):
    st0 = Point(coordinates[0],coordinates[1])
    subbasin = None
    for i in range(0,len(basins.geometry)):
        if st0.within(basins.geometry[i]):
            # print("is within subbasin %s" % self.basins.nombre_2[i])
            subbasin = basins.nombre_3[i]
    return subbasin

fews_series_columns = {
    1: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"],
    31: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"],
    39: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"],
    40: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"],
    85: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"],
    101: ["STATION_ID", "STATION_NAME", "EXTERNAL_LOCATION_ID", "EXTERNAL_PARAMETER_ID", "TIMESTEP_HOUR", "UNIT", "IMPORT_SOURCE", "THRESHOLD_LOW", "THRESHOLD_YELLOW", "THRESHOLD_ORANGE", "THRESHOLD_RED", "THRESHOLD_WATERINTAKE", "THRESHOLD_NAVIGATION", "THRESHOLD_MEAN", "THRESHOLD_P05", "THRESHOLD_P10", "THRESHOLD_P90", "THRESHOLD_P95", "IMPORT", "LATITUDE", "LONGITUDE", "ALTITUDE", "TYPE", "COUNTRY", "ORGANIZATION", "SUBBASIN", "PARENT_ID", "CHILD_ID"]
}


def seriesToFews(series : Union[str,list], output=None, monthly_stats=False,stations=None,percentil=None,var_id: int=None):
    """Converts a5 series list to FEWS table
    
    Parameters
    ----------
    series : str or list
        Result of a5_client.getSeries
        If str: JSON file to read from
        If list: list of a5_client.getSeries return value  
    output: string
        Write CSV output into this file
    stations: DataFrame
        result of stationsToFews
        if not None adds station metadata to series
    percentil: list
        list of numeric percentiles to add to series metadata table (if present in series)
    var_id: int
        id of variable. If not None, result columns are selected according to FEWS requirement 
    
    Returns
    -------
    DataFrame
        A data frame of the time series in FEWS format
    """
    
    if stations is not None:
        if stations.index.name != 'STATION_ID':
            stations = stations.set_index("STATION_ID")
    # timeseries: str => path to timeseries geojson file or dict => same already parsed into dict 
    if isinstance(series,str):
        with open(series,"r") as f: 
            series = json.load(f)
    rows = []
    for item in series:
        timestep_hour = interval2epoch(item["var"]["timeSupport"]) / 3600 if item["var"]["timeSupport"] is not None and len(item["var"]["timeSupport"].keys()) else None
        row = {
            "STATION_ID": item["estacion"]["id"],
            "STATION_NAME": item["estacion"]["nombre"],
            "EXTERNAL_LOCATION_ID": item["estacion"]["id"],
            "EXTERNAL_PARAMETER_ID": item["var"]["id"],
            "TIMESTEP_HOUR": timestep_hour,
            "UNIT": item["unidades"]["abrev"],
            "IMPORT_SOURCE": "INA",
            "THRESHOLD_LOW": item["estacion"]["nivel_aguas_bajas"] if item["var"]["VariableName"] == "Gage height" else None,
            "THRESHOLD_YELLOW": item["estacion"]["nivel_alerta"] if item["var"]["VariableName"] == "Gage height" else None,
            "THRESHOLD_RED": item["estacion"]["nivel_evacuacion"] if item["var"]["VariableName"] == "Gage height" else None,
            "IMPORT": True,
            "PARENT_ID": "AR_INA_%i" % item["estacion"]["id"],
            "CHILD_ID": "AR_INA_%i_INA_%i_%s" % (
                item["estacion"]["id"], 
                timestep_hour,  
                internal_var_ids[item["var"]["id"]] if item["var"]["id"] in internal_var_ids else "" 
            )
        }
        if "monthlyStats" in item:
            months = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
            quantiles = ["mean","p01","p10","p50","p90","p99"]
            sums = {"mean":0,"p01":0,"p10":0,"p50":0,"p90":0,"p99":0}
            counts = {"mean":0,"p01":0,"p10":0,"p50":0,"p90":0,"p99":0}
            for i in item["monthlyStats"]:
                mon = i["mon"]
                for q in quantiles:
                    if monthly_stats:
                        t_name = "THRESHOLD_%s_%s" % (months[mon], q)
                        row[t_name] = i[q]
                    sums[q] = sums[q] + i[q]
                    counts[q] = counts[q] + 1
            for q in quantiles:
                if counts[q] > 0:
                    t_name = "THRESHOLD_%s" % q.upper()
                    row[t_name] = sums[q] / counts[q]
        if percentil is not None:
            percentiles_columns = [(p,"THRESHOLD_P%02d" % int(p*100)) for p in percentil]
            for col in percentiles_columns:
                row[col[1]] = None
            if "percentiles" in item:
                logging.debug("Found percentiles for series_id %i" % item["id"])
                for col in percentiles_columns:
                    p_matches = [x for x in item["percentiles"] if x["percentile"] == col[0]]
                    if len(p_matches):
                        logging.debug("found percentil %.02f" % col[0])
                        row[col[1]] = p_matches[0]["valor"]
                    else:
                        logging.debug("percentil %i not found " % col[0])
            else:
                logging.debug("percentiles not found for series_id %i" % item["id"])
        if stations is not None and row["STATION_ID"] in stations.index:
            row["LATITUDE"] = stations["LATITUDE"][row["STATION_ID"]]
            row["LONGITUDE"] = stations["LONGITUDE"][row["STATION_ID"]]
            row["ALTITUDE"] = stations["ALTITUDE"][row["STATION_ID"]]
            row["TYPE"] = stations["TYPE"][row["STATION_ID"]]
            row["COUNTRY"] = stations["COUNTRY"][row["STATION_ID"]]
            row["ORGANIZATION"] = stations["ORGANIZATION"][row["STATION_ID"]]
            row["SUBBASIN"] = stations["SUBBASIN"][row["STATION_ID"]]
        rows.append(row)
    if not len(rows):
        logging.warning("No series found")
        return
    data_frame = pandas.DataFrame(rows).sort_values(["STATION_ID","EXTERNAL_PARAMETER_ID"])
    # print("var_id:%s. Type:%s" % (str(var_id),type(var_id)))
    if var_id is not None and var_id in fews_series_columns:
        logging.info("Set columns for var_id=%s",str(var_id))
            # raise Exception("Bad parameter var_id=%s. Valid values: %s" % (str(var_id),",".join([str(k) for k in fews_series_columns])))
        data_frame["THRESHOLD_MEAN"] = data_frame["THRESHOLD_P50"] if "THRESHOLD_P50" in data_frame.columns else None
        for column in fews_series_columns[var_id]:
            if column not in data_frame.columns:
                data_frame[column] = None
        data_frame = data_frame[fews_series_columns[var_id]]
    logging.debug("columns: %s" % ",".join(data_frame.columns))
    if output is not None:
        try: 
            f = open(output,"w")
        except:
            raise Exception("Couldn't open file %s for writing" % output)
        f.write(data_frame.to_csv(index=False))
        f.close()
    return data_frame


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Metadata files generation for a5 service in FEWS required format')
    parser.add_argument('--monthly_stats', action='store_true',
        help='add monthly percentiles')
    parser.add_argument('--debug',action='store_true', help='activate debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(stream=sys.stdout,level=logging.DEBUG,format="%(asctime)s %(levelname)s %(message)s")
    else:
        logging.basicConfig(stream=sys.stdout,level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    exclude_stations = [2122,2123,2127,2180,2220,2221,2849,2196]
    exclude_test_stations = True
    import datetime
    from a5_client import Client
    a5_client = Client()
    estaciones = a5_client.getEstaciones(has_obs=True, pais="Argentina", habilitar=True, geom="-68,-38,-53,-21")
    # len(estaciones)
    # estaciones_fews = estacionesToFews("results/estaciones.json",output="results/estaciones_fews.csv")
    if exclude_stations is not None:
        estaciones = [e for e in estaciones if e["id"] not in exclude_stations]
    if exclude_test_stations:
        estaciones = [ e for e in estaciones if "zprueba" not in e["nombre"].lower()]
    estaciones_fews = estacionesToFews(estaciones,output="results/INA_locations.csv")
    # SERIES
    percentil = [0.05,0.5,0.95]
    variable_id_list = [1,2,4,31,39,40,85,101]
    output_series_raw = "results/series.json"
    estacion_ids = [id for id in estaciones_fews["STATION_ID"]]
    series = []
    i = 0
    by = 40
    while i < len(estacion_ids):
        logging.debug(datetime.datetime.now())
        date_range_after = datetime.datetime.now() - datetime.timedelta(days=180)
        logging.info("downloading series for stations %i to %i" % (i, i+by))
        series_part = a5_client.getSeries(proc_id=[1,2],var_id=variable_id_list,estacion_id=estacion_ids[i:i+by],date_range_after=date_range_after.isoformat(),getMonthlyStats=args.monthly_stats,getPercentiles=True,percentil=percentil)
        series.extend(series_part)
        i = i + by
    if output_series_raw is not None:
        series_raw = open(output_series_raw, "w")
        json.dump(series_part, series_raw, indent = 2)
    #len(series)
    #set([s["procedimiento"]["id"] for s in series])
    #set([s["estacion"]["id"] for s in series])
    # FILTER OUT by timeend
    # from datetime import datetime, timedelta
    # how_old_days = 180
    # series_filter = filter(lambda serie: serie["date_range"]["timeend"] is not None and datetime.fromisoformat(serie["date_range"]["timeend"].replace("Z","")) > datetime.now() - timedelta(days=how_old_days),series)
    # series = list(series_filter)
    series_fews = seriesToFews(series,output="results/series_fews.csv",stations=estaciones_fews,monthly_stats=args.monthly_stats,percentil=percentil)
    if series_fews is None:
        logging.error("No series found")
        exit(1)
    # VARIABLES
    variables = a5_client.getVariables(id=variable_id_list,as_DataFrame=True)
    a5_client.writeLastResult("results/variables.csv")
    # WRITE SERIES IN SEPARATE FILES
    series_file_map = {
        1: "results/INA_P.csv",
        39 : "results/INA_H.csv",
        40 : "results/INA_Q.csv",
        31 : "results/INA_P1h.csv",
        101 : "results/INA_H4h.csv",
        85: "results/INA_H1h.csv" 
    }
    series_final_files = {
        "results/INA_P_all.csv": {
            "ids": [1, 31],
            "df": None
        },
        "results/INA_H_all.csv": {
            "ids": [39, 85, 101],
            "df": None
        },
        "results/INA_Q_all.csv": {
            "ids": [40],
            "df": None
        }
    }
    for i in variables.index:
        series_filter_by_var_id = filter(lambda serie: serie["var"]["id"] == variables["id"][i],series)
        series_subset = list(series_filter_by_var_id)
        filename = series_file_map[variables["id"][i]] if variables["id"][i] in series_file_map else "results/INA_%s.csv" % variables["nombre"][i]
        var_id = variables["id"][i] # if variables["id"][i] in fews_series_columns else None
        series_subset_fews = seriesToFews(series_subset,output=filename,stations=estaciones_fews,monthly_stats=args.monthly_stats,percentil=percentil,var_id=var_id)
        for file, v in series_final_files.items():
            if variables["id"][i] in v["ids"]:
                if v["df"] is None:
                    v["df"] = series_subset_fews
                else:
                    v["df"] = pandas.concat([v["df"], series_subset_fews], axis=0)
        
    for file, v in series_final_files.items():
        if v["df"] is None:
            logging.error("No data to write for file %s" % file)
            exit(2)
        with open(file, "w") as outfile:
            outfile.write(v["df"].sort_values(["STATION_ID","EXTERNAL_PARAMETER_ID"]).to_csv(index=False))
            outfile.close()





