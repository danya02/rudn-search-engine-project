import os
from flask import Flask, render_template, send_file
from database import *
from blueprints import *

app = Flask(__name__)
for bp in BLUEPRINT_LIST:
    app.register_blueprint(bp)


@app.before_request
def before_request():
    db.connect()

@app.after_request
def after_request(response):
    db.close()
    return response


@app.route('/')
def index():
    db_size = os.path.getsize(database_path)
    return render_template('index.html', db_path=database_path, db_size=db_size)

@app.route('/database.db')
def download_db():
    return send_file(database_path)

if __name__ == '__main__':
    app.run('0.0.0.0', 5000)