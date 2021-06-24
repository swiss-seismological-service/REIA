

def test_route_frontend(client):
    response = client.get('/frontend')
    assert response.status_code == 200
    assert b'<div id="entry"></div>' in response.data
