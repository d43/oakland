from flask import Flask
from flask import request
from flask import render_template
import requests
import psycopg2
from database_to_json import join_json
from model import clusters


def start_app():
    '''
    Input:
    - None

    Output:
    - None

    Boots flask application
    '''
    # Initialize flask app
    app = Flask(__name__)

    # Define routes/pages and get template
    @app.route('/')
    @app.route('/oakland', methods=['GET'])
    def oakland():
        if request.method == 'GET':
            return render_template("oakland.html",
                                   map_jsons=map_jsons)

    # Boot application
    app.run(host='127.0.0.1', port=8088, debug=True)


if __name__ == "__main__":
    # Connect to database
    conn_dict = {'dbname': 'oakland', 'user': 'danaezoule', 'host': '/tmp'}
    conn = psycopg2.connect(dbname=conn_dict['dbname'],
                            user=conn_dict['user'], host=conn_dict['host'])
    c = conn.cursor()

    # Get clusters and data (from model.py)
    clus, crime_data = clusters(conn)

    # Create geo jsons (from database_to_json.py)
    map_jsons = join_json(conn, clus, crime_data)

    start_app()
