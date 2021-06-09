from datamodel import MeanAssetLoss, Asset
from datamodel.base import session, engine
from flask import Blueprint, render_template
import pandas as pd
import json
import plotly
import plotly.express as px
from urllib.request import urlopen
import time

frontend = Blueprint('frontend', __name__, template_folder='templates')


@frontend.route('/frontend')
def index():
    return render_template('frontend/home.html')


@frontend.route('/notdash')
def notdash():
    df = pd.DataFrame({'Fruit': ['Apples', 'Oranges', 'Bananas', 'Apples',
                                 'Oranges', 'Bananas'], 'Amount': [
                      4, 1, 2, 2, 4, 5], 'City': ['SF', 'SF', 'SF', 'Montreal',
                                                  'Montreal', 'Montreal']})
    fig = px.bar(df, x='Fruit', y='Amount', color='City', barmode='group')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    print(graphJSON)
    return render_template('frontend/notdash.html', graphJSON=graphJSON)


@frontend.route('/plotlymap')
def plotlymap():

    with open('gemeinden.txt', 'rb') as file:
        m_json = json.load(file)

    asset_query = session.query(MeanAssetLoss, Asset._municipality_oid).filter(
        MeanAssetLoss._lossCalculation_oid == 1).join(Asset)

    dm = pd.read_sql_query(asset_query.statement, engine)
    dm = dm.drop(['_oid', '_oid_1', '_lossCalculation_oid',
                 'loss_Uncertainty', '_asset_oid'], axis=1)
    dm = dm.groupby('_municipality_oid').sum()
    dm['_oid'] = dm.index

    fig = px.choropleth_mapbox(dm, geojson=m_json, locations='_oid', color='loss_value',
                               color_continuous_scale="OrRd",
                               range_color=(0, 100000000),
                               mapbox_style="carto-positron",
                               zoom=7, center={"lat": 47.488212, "lon": 8.665373},
                               opacity=0.5,
                               labels={'loss_value': 'sum loss'}, height=800
                               )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    # with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    #     counties = json.load(response)
    # df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
    #                  dtype={"fips": str})
    # fig = px.choropleth_mapbox(df, geojson=counties, locations='fips', color='unemp',
    #                            color_continuous_scale="Viridis",
    #                            range_color=(0, 12),
    #                            mapbox_style="carto-positron",
    #                            zoom=3, center={"lat": 37.0902, "lon": -95.7129},
    #                            opacity=0.5,
    #                            labels={'unemp': 'unemployment rate'}
    #                            )

    # fig.update_geos(projection_type='mercator')
    # fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    # us_cities = pd.read_csv(
    #     "https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
    # fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
    #                         color_discrete_sequence=["fuchsia"], zoom=3)
    # fig.update_layout(mapbox_style="open-street-map")
    # fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('frontend/plotlymap.html', graphJSON=graphJSON)
