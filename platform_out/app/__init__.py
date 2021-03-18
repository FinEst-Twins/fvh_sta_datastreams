from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import logging
import os

logging.basicConfig(level=logging.INFO)

db = SQLAlchemy()

url = "localhost:1338/OGCSensorThings/v1.0/"

def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    # set up extensions
    db.init_app(app)

    # register blueprints

    from app.resources.datastreams import datastreams_blueprint
    app.register_blueprint(datastreams_blueprint)

    from app.resources.foi import foi_blueprint
    app.register_blueprint(foi_blueprint)

    from app.resources.observations import observations_blueprint
    app.register_blueprint(observations_blueprint)

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app, "db": db}

    @app.route("/")
    def hello_world():
        return jsonify(health="ok")

    return app


