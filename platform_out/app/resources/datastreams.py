from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api, reqparse, abort
from datetime import datetime
from app import swagger
from flasgger.utils import swag_from
import json
import os
import redis
from rq import Queue, Connection
from flask_jwt_extended import jwt_required
from app.models import AssetData
from confluent_kafka.admin import AdminClient, NewTopic

datastreams_blueprint = Blueprint("datastream", __name__)
api = Api(datastreams_blueprint)


class DataStream(Resource):
    def get(self):
        """
        gets list of all data streams
        """
        asset_data = AssetData.return_all()
        if asset_data:
            response = jsonify(asset_data)
            response.status_code = 200
            return response
        else:
            result = {"message": "No exisitng datastreams"}
            response = jsonify(result)
            response.status_code = 200
            return response

api.add_resource(DataStream, "/datastream")

