from FEWS.whos_client import Client
from pandas import DataFrame

def test_get_observations_inmet():
    client = Client({"url":"https://gs-service-preproduction.geodab.eu","token":"my_token","view":"whos-plata"})
    observations = client.getObservationsWithPagination(country="BRA",provider="brazil-inmet",observedProperty="Precipitation",aggregationDuration="P1D",ontology="whos",json_output="tmp/observations_inmet.json",fews_output="tmp/observations_inmet.csv")
    assert type(observations) == DataFrame
    assert len(observations) > 100