from FEWS.whos_client import Client
from pandas import DataFrame

def test_get_features_inmet():
    client = Client({"url":"https://gs-service-preproduction.geodab.eu","token":"my_token","view":"whos-plata"})
    features = client.getFeaturesWithPagination(country="BRA",provider="brazil-inmet",json_output="tmp/features_inmet.json", fews_output="tmp/features_inmet.csv")
    assert type(features) == DataFrame
    assert len(features) > 100
