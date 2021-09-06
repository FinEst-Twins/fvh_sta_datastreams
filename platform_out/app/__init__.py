from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import logging
import os

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        integrations=[FlaskIntegration()]
    )

db = SQLAlchemy()


def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(pathname)s %(funcName)s: %(message)s",level=app.config["LOG_LEVEL"])
    #logging.getLogger().setLevel(app.config["LOG_LEVEL"])

    # set up extensions
    db.init_app(app)

    # register blueprints
    with app.app_context():
        from app.resources.datastreams import datastreams_blueprint
        app.register_blueprint(datastreams_blueprint)

        from app.resources.foi import foi_blueprint
        app.register_blueprint(foi_blueprint)

        from app.resources.observations import observations_blueprint
        app.register_blueprint(observations_blueprint)

        from app.resources.things import things_blueprint
        app.register_blueprint(things_blueprint)

        from app.resources.sensors import sensors_blueprint
        app.register_blueprint(sensors_blueprint)

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app, "db": db}

    @app.route("/")
    def hello_world():
        return jsonify(health="ok")

    @app.route('/debug-sentry')
    def trigger_error():
        division_by_zero = 1 / 0

    return app


