from . import frontend
from datamodel import MeanAssetLoss, Asset
from datamodel import session, engine
from flask import render_template
import pandas as pd
import json
import plotly
import plotly.express as px


@frontend.route('/frontend')
def index():
    return render_template('frontend/home.html')


# @frontend.route('/notdash')
# def notdash():
#     df = pd.DataFrame({'Fruit': ['Apples', 'Oranges', 'Bananas', 'Apples',
#                                  'Oranges', 'Bananas'], 'Amount': [
#                       4, 1, 2, 2, 4, 5], 'City': ['SF', 'SF', 'SF', 'Montreal',
#                                                   'Montreal', 'Montreal']})
#     fig = px.bar(df, x='Fruit', y='Amount', color='City', barmode='group')
#     graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
#     print(graphJSON)
#     return render_template('frontend/notdash.html', graphJSON=graphJSON)


@frontend.route('/plotlymap/<int:oid>')
def plotlymap(oid):

    with open('gemeinden.txt', 'rb') as file:
        m_json = json.load(file)

    asset_query = session.query(MeanAssetLoss, Asset._municipality_oid).filter(
        MeanAssetLoss._losscalculation_oid == oid).join(Asset)

    dm = pd.read_sql_query(asset_query.statement, engine)
    dm = dm.drop(['_oid', '_oid_1', '_losscalculation_oid',
                 'loss_uncertainty', '_asset_oid'], axis=1)
    dm = dm.groupby('_municipality_oid').sum()
    dm['_oid'] = dm.index

    fig = px.choropleth_mapbox(dm, geojson=m_json, locations='_oid',
                               color='loss_value',
                               color_continuous_scale="OrRd",
                               range_color=(0, 100000000),
                               mapbox_style="carto-positron",
                               zoom=7,
                               center={"lat": 47.488212, "lon": 8.665373},
                               opacity=0.5,
                               labels={'loss_value': 'sum loss'}, height=800
                               )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('frontend/plotlymap.html', graphJSON=graphJSON)
