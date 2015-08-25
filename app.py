from flask import Flask
from flask import request
from flask import render_template
import requests
import psycopg2
from database_to_json import join_json
from model import clusters

conn_dict = {'dbname':'oakland', 'user':'danaezoule', 'host':'/tmp'}
conn = psycopg2.connect(dbname=conn_dict['dbname'], user=conn_dict['user'], host=conn_dict['host'])
c = conn.cursor()

clus = clusters(conn)
map_jsons = join_json(clus)

# Initialize Flask App

app = Flask(__name__)

@app.route('/')
@app.route('/oakland', methods=['GET'])
def oakland():
    if request.method == 'GET':
        return render_template("oakland.html",
        						map_jsons = map_jsons)


if __name__ == "__main__":

    app.run(host='127.0.0.1', port=8088, debug=True)