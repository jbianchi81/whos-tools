from FEWS.whos_client import Client

def test_get_observations():
    client = Client({"url":"https://gs-service-preproduction.geodab.eu","token":"my_token","view":"whos-plata"})
    features = client.getObservations(country="ARG",limit=10,output="tmp/observations.json")
    assert "id" in features
    assert features["id"] == "observation collection"
    assert "member" in features
    assert len(features["member"]) == 10
