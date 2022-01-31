from esloss.datamodel import Municipality
from app.database import session
from flask import Blueprint, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
from urllib.request import urlopen
import time

municipalities = session.query(Municipality).all()
start = time.time()
m_json = {'type': 'FeatureCollection', 'features': []}
for m in municipalities:
    try:
        with urlopen(f'https://api3.geo.admin.ch/rest/services/api/MapServer/ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill/{m._oid}?geometryFormat=geojson&sr=4326') as response:
            m_json['features'].append(json.load(response)['feature'])
    except:
        print(m._oid)
print(time.time() - start)

with open('gemeinden.txt', 'w') as json_file:
    json.dump(m_json, json_file)
