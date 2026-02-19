from FEWS.whos_client import Client

def test_get_features():
    client = Client({"url":"https://gs-service-preproduction.geodab.eu","token":"my_token","view":"whos-plata"})
    features = client.getFeatures(country="ARG",limit=10,output="tmp/features.json")
    assert "id" in features
    assert features["id"] == "observation collection"
    assert "results" in features
    assert len(features["results"]) == 10

    df = client.monitoringPointsToFEWS(features, output="tmp/features.csv")
    assert len(df) == 10