import psycopg2
import pandas as pd
import json


def set_query():
    '''
    Input:
    - None

    Output:
    - Query string

    Convert postGIS polygon to GeoJSON geometry.
    Join on area_features to ensure that only areas with crime counts are included.
    '''
    map_query = '''
                SELECT DISTINCT shp_table.ogc_fid as area_id,
                ST_AsGeoJSON(shp_table.wkb_geometry) as geom
                FROM shp_table
                JOIN area_features
                ON shp_table.ogc_fid = area_features.ogc_fid
                ORDER BY area_id
                ;'''
    return map_query

def query_db(conn, query):
    '''
    Input:
    - Connection, Query

    Output:
    - Data
    '''
    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall())
    df.columns = ['area_id', 'geom']
    return df

def to_json(idx, geom, clusters, crime_data, color_scheme='default'):
    '''
    Input:
    - Index of neighborhood, geometry of neighborhood
    - Dictionaries of clusters and crime_data (one entry per year, from model.py)

    Output:
    - GeoJSON file
    '''
    # Set color scheme for map
    if color_scheme == 'default':
        # Green, light blue, yellow, red, pink
        color = ['#CC0066', '#00F7FF', '#006600', '#FFF703', '#CC0066', '#FF3300', '#FF3300']
    elif color_scheme == 'color_blind':
        # Colorblind friendly
        color = ['#FFFF00', '#3366FF', '#000066', '#66FFFF', '#FFFF00', '#CC9900', '#CC9900']
    elif color_scheme == 'green_to_red':
        # Green to red
        color = ['#CC0066', '#00F7FF', '#006600', '#FFF703', '#CC0066', '#FF0000', '#FF0000']

    # Create properties dictionary to:
    #    - Assign color to location corresponding to yearly cluster
    #    - Store yearly crime data for retrieval by javascript/Google Charts API
    properties = {}
    properties['data'] = {}
    
    for year, values in clusters.iteritems():
        properties[year] = color[values[idx]]
        properties['data'][year] = crime_data[year].iloc[[idx]].values.tolist()

    # Create properties dictionary to store features for display in visualization.
    geo_json = {'type':'Feature', 'geometry':json.loads(geom), 'properties':properties }

    return geo_json

def join_json(conn, clusters, crime_data):
    '''
    Input:
    - Dictionary of clusters (from model.py)
    - Dictionary of crime_data (from model.py)

    Output:
    - GeoJSON Feature Collection (ready for mapping)
    '''
    df = query_db(conn, set_query())
    geo_list = []

    for idx, row in df.iterrows():
        geo_list.append(to_json(idx, row['geom'], clusters, crime_data))

    return json.dumps( { "type":"FeatureCollection", 
                "features":geo_list } )
