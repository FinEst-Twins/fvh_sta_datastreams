from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api
from app.models import Datastreams
import logging

logging.basicConfig(level=logging.INFO)

datastreams_blueprint = Blueprint("datastream", __name__)
api = Api(datastreams_blueprint)


class DataStream(Resource):
    def get(self):
        """
        query data streams
        #TODO pagination
        """
        try:
            query_parameters = request.args
            #print(query_parameters)
            if query_parameters:
                thing = None
                sensor = None
                if "thing" in query_parameters:
                    thing = request.args["thing"]
                if "sensor" in query_parameters:
                    sensor = request.args["sensor"]
                #print(thing, sensor)
                datastreams = Datastreams.filter_by_thing_sensor(thing, sensor)
            else:
                result = {"message": "no known query parameters"}
                response = jsonify(result)
                response.status_code = 400
                return response

        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if datastreams:
            response = jsonify(datastreams)
            response.status_code = 200
            return response
        else:
            result = {"message": "No datastreams found"}
            response = jsonify(result)
            response.status_code = 200
            return response

api.add_resource(DataStream, "/datastream")
