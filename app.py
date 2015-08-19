from flask import Flask
from flask import request
from flask import render_template
import requests

#load in geo_json: select from table where crime count is one or greater
#parse into json

app = Flask(__name__)

@app.route('/')
@app.route('/index')
@app.route('/oakland', methods=['GET', 'POST'])
def oakland():
    if request.method == 'GET':
        return render_template("oakland.html",
        						geo_json = )

if __name__ == "__main__":

    app.run(host='127.0.0.1', port=8088, debug=True)