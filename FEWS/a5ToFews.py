from typing import Union
from pathlib import Path
from geopandas import read_file as gpd_read_file
from shapely.geometry import Point
import json
import pandas
from warnings import warn

config = {
    "basins_geojson_file": "cuencas/cuencas.geojson"
}

basins = gpd_read_file(config["basins_geojson_file"])

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
            "COUNTRY": item["pais"],
            "ORGANIZATION": "INA",
            "SUBBASIN": getSubBasin(item["geom"]["coordinates"])
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

def seriesToFews(series : Union[str,list], output=None, monthly_stats=False,stations=None):
    """Converts a5 series list to FEWS table
    
    Parameters
    ----------
    series : dict or list
        Result of a5_client.getSeries
        If str: JSON file to read from
        If dict: a5_client.getSeries return value  
    output: string
        Write CSV output into this file
    stations: DataFrame
        result of stationsToFews
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
    if isinstance(series,str):
        with open(series,"r") as f: 
            series = json.load(f)
    rows = []
    for item in series:
        row = {
            "STATION_ID": item["estacion"]["id"],
            "EXTERNAL_LOCATION_ID": item["estacion"]["id"],
            "EXTERNAL_PARAMETER_ID": item["var"]["id"],
            "TIMESTEP_HOUR": interval2epoch(item["var"]["timeSupport"]) / 3600 if item["var"]["timeSupport"] is not None and len(item["var"]["timeSupport"].keys()) else None,
            "UNIT": item["unidades"]["abrev"],
            "IMPORT_SOURCE": "INA",
            "THRESHOLD_LOW": item["estacion"]["nivel_aguas_bajas"] if item["var"]["VariableName"] == "Gage height" else None,
            "THRESHOLD_YELLOW": item["estacion"]["nivel_alerta"] if item["var"]["VariableName"] == "Gage height" else None,
            "THRESHOLD_RED": item["estacion"]["nivel_evacuacion"] if item["var"]["VariableName"] == "Gage height" else None,
            "IMPORT": True
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
        if stations is not None and row["STATION_ID"] in stations.index:
            row["LATITUDE"] = stations["LATITUDE"][row["STATION_ID"]]
            row["LONGITUDE"] = stations["LONGITUDE"][row["STATION_ID"]]
            row["ALTITUDE"] = stations["ALTITUDE"][row["STATION_ID"]]
            # row["TYPE"] = stations["TYPE"][row["STATION_ID"]]
            row["COUNTRY"] = stations["COUNTRY"][row["STATION_ID"]]
            row["ORGANIZATION"] = stations["ORGANIZATION"][row["STATION_ID"]]
            row["SUBBASIN"] = stations["SUBBASIN"][row["STATION_ID"]]
        rows.append(row)
    data_frame = pandas.DataFrame(rows).sort_values(["STATION_ID","EXTERNAL_PARAMETER_ID"])
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
    args = parser.parse_args()

    import datetime
    from a5_client import Client
    a5_client = Client()
    estaciones = a5_client.getEstaciones(has_obs=True, pais="Argentina", habilitar=True)
    # len(estaciones)
    # estaciones_fews = estacionesToFews("results/estaciones.json",output="results/estaciones_fews.csv")
    estaciones_fews = estacionesToFews(estaciones,output="results/INA_locations.csv")
    # SERIES
    estacion_ids = [id for id in estaciones_fews["STATION_ID"]]
    series = []
    i = 0
    by = 40
    while i < len(estacion_ids):
        print(datetime.datetime.now())
        date_range_after = datetime.datetime.now() - datetime.timedelta(days=180)
        print("downloading series for stations %i to %i" % (i, i+by))
        series_part = a5_client.getSeries(proc_id=[1,2],var_id=[1,2,4,39,40],estacion_id=estacion_ids[i:i+by],date_range_after=date_range_after.isoformat(),getMonthlyStats=True)
        series.extend(series_part)
        i = i + by
    #len(series)
    #set([s["procedimiento"]["id"] for s in series])
    #set([s["estacion"]["id"] for s in series])
    # FILTER OUT by timeend
    # from datetime import datetime, timedelta
    # how_old_days = 180
    # series_filter = filter(lambda serie: serie["date_range"]["timeend"] is not None and datetime.fromisoformat(serie["date_range"]["timeend"].replace("Z","")) > datetime.now() - timedelta(days=how_old_days),series)
    # series = list(series_filter)
    series_fews = seriesToFews(series,output="results/series_fews.csv",stations=estaciones_fews,monthly_stats=args.monthly_stats)
    # VARIABLES
    variables = a5_client.getVariables(id=[1,2,4,39,40],as_DataFrame=True)
    a5_client.writeLastResult("results/variables.csv")
    # WRITE SERIES IN SEPARATE FILES
    series_file_map = {
        1: "results/INA_P.csv",
        39 : "results/INA_H.csv",
        40 : "results/INA_Q.csv"
    }
    for i in variables.index:
        series_filter_by_var_id = filter(lambda serie: serie["var"]["id"] == variables["id"][i],series)
        series_subset = list(series_filter_by_var_id)
        filename = series_file_map[variables["id"][i]] if variables["id"][i] in series_file_map else "results/INA_%s.csv" % variables["nombre"][i]
        series_subset_fews = seriesToFews(series_subset,output=filename,stations=estaciones_fews,monthly_stats=args.monthly_stats)
    



