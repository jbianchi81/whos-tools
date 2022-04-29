from lxml.html import parse
import json
from pandas import DataFrame

default_token = "YOUR_TOKEN_HERE"
default_view = "whos-plata"
default_url = "http://gs-service-production.geodab.eu/gs-service/services/essi/token/%s/view/%s/semantic" % (default_token, default_view)

def parse_semantic(url=default_url,output_json=None,output_csv=None):
    '''
    Downloads and parses semantic variable mapping from whos server

    Parameters
    ----------
    url : str
        url of semantic web page (html)
    output_json : str
        write output in json format to this file
    output_csv : str
        write output in csv format to this file

    Returns
    -------
    DataFrame
        the parsed variable list
    '''
    h = parse(url)
    doc = h.getroot()
    rows = doc.xpath("./body/table/tr")
    variables = []
    for tr in rows:
        cells = tr.getchildren()
        for td in cells:
            if td.text is not None:
                td.text = td.text.replace("\n","").replace("\r","")
        if not isinstance(cells[0].text,str):
            continue
        if len(cells[0].text) != 40: # len("B838A449A5FBC64CBB8A204A5CD614519EB0844A"):
            continue
        variable = {
            "variableCode": cells[0].text,
            "variableIdAtProvider": cells[1].text,
            "variableName": cells[2].text,
            "variableURI": cells[3].find("a").get("href") if cells[3].find("a").get("href") != "null" else None,
            "variableDescription": cells[4].text,
            "units": cells[5].text,
            "unitsURI": cells[6].find("a").get("href") if cells[6].find("a").get("href") != "null" else None,
            "interpolationType": cells[7].text if cells[7].text != "null" else None,
            "timeSupport": cells[8].text if cells[8].text != "null" else None,
            "timeInterval": cells[9].text if cells[9].text != "null" else None,
            "timeUnits": cells[10].text if cells[10].text != "null" else None,
            "realTime": cells[11].text if cells[11].text != "null" else None,
            "country": cells[12].text,
            "countryISO3": cells[13].text
        }
        variables.append(variable)
    if output_json is not None:
        f = open(output_json,"w")
        f.write(json.dumps(variables,indent=2,ensure_ascii=False))
    variables_df = DataFrame(variables)
    if output_csv is not None:
        f = open(output_csv,"w")
        f.write(variables_df.to_csv(index=False))
    return variables_df

if __name__ == "__main__":
    # import getopt, sys
    # argumentList = sys.argv[1:]
    # try:
    #     arguments, values = getopt.getopt(argumentList,"u:j:c:",["url=","json=","csv="])
    # except getopt.error as err:
    #     # output error, and return with an error code
    #     print (str(err))
    #     exit(1)
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-u","--url", help = "url of semantic web page (html)")
    argparser.add_argument('-j','--json', help = "write output in json format to this file")
    argparser.add_argument('-c','--csv', help = 'write output in csv format to this file')
    args = argparser.parse_args()
    if args.json is None and args.csv is None:
        print("Please choose a json or csv output file name")
        exit(2)
    # parse_semantic(url=args.url if args.url is not None else default_url, output_json=args.json, output_csv=args.csv)
    args_dict = {}
    if args.url is not None:
        args_dict["url"] = args.url
    if args.json is not None:
        args_dict["output_json"] = args.json
    if args.csv is not None:
        args_dict["output_csv"] = args.csv
    
    parse_semantic(**args_dict)