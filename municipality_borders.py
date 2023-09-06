import geopandas as gpd
from shapely.wkt import loads
from SPARQLWrapper import JSON, SPARQLWrapper

CANTONS = {
    'Thurgau': 'TG',
    'Aargau': 'AG',
    'Bern': 'BE',
    'Vaud': 'VD',
    'Ticino': 'TI',
    'Luzern': 'LU',
    'Zürich': 'ZH',
    'Solothurn': 'SO',
    'Basel-Landschaft': 'BL',
    'Valais': 'VS',
    'Genève': 'GE',
    'Graubünden': 'GR',
    'Jura': 'JU',
    'Obwalden': 'OW',
    'Schwyz': 'SZ',
    'Uri': 'UR',
    'St. Gallen': 'SG',
    'Appenzell Innerrhoden': 'AI',
    'Fribourg': 'FR',
    'Zug': 'ZG',
    'Schaffhausen': 'SH',
    'Basel-Stadt': 'BS',
    'Nidwalden': 'NW',
    'Neuchâtel': 'NE',
    'Appenzell Ausserrhoden': 'AR',
    'Glarus': 'GL'
}

# Set the SPARQL endpoint URL
endpoint_url = "https://geo.ld.admin.ch/query"

# Set the SPARQL query
query = """
PREFIX urn: <http://fliqz.com/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ogc: <http://www.opengis.net/ont/geosparql#>
PREFIX schema: <http://schema.org/>
PREFIX geold: <https://geo.ld.admin.ch/def/>
PREFIX gn: <http://www.geonames.org/ontology#>

SELECT DISTINCT ?name ?bfs_nummer ?canton ?geometry WHERE {
  GRAPH <urn:bgdi:boundaries:municipalities:2023>
  {
    ?s a ogc:Feature .
    ?s geo:defaultGeometry ?Geometry .
    ?s geold:bfsNumber ?id .
    ?s schema:name ?name .
    ?s gn:parentADM1 ?parent .
    ?Geometry geo:asWKT ?geometry .
    BIND(str(?id) as ?bfs_nummer)
  }
?parent schema:name ?canton .
FILTER NOT EXISTS {
   ?s rdf:type <https://geo.ld.admin.ch/def/CantonalTerritory> .
 }
}
ORDER by ASC(?name)
"""

# Create a SPARQLWrapper object and set the query and endpoint URL
sparql = SPARQLWrapper(endpoint_url)
sparql.setQuery(query)

# Set the response format to JSON
sparql.setReturnFormat(JSON)

# Execute the query and parse the results
results = sparql.query().convert()

# Loop through the results and print the BFS-Number, Name and
# polygon/geometry for each municipality

for result in results["results"]["bindings"]:
    result['name'] = result['name']['value']
    result['bfs_nummer'] = result['bfs_nummer']['value']
    result['geometry'] = loads(result['geometry']['value'])
    result['canton'] = CANTONS[result['canton']['value']]


gdf = gpd.GeoDataFrame(results["results"]["bindings"])

gdf = gdf[~gdf['name'].str.contains('Comunanza')]
print(gdf)
