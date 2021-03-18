from flask import jsonify, Blueprint, request
from flask_restful import Resource, Api
from app.models.observations import Observations
import logging


logging.basicConfig(level=logging.INFO)


observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)


class Observation(Resource):
    def get(self, id):
        """
        query data streams
        #TODO pagination
        """
        try:
            obs = Observations.filter_by_id(id)
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if obs:
            response = jsonify(obs)
            response.status_code = 200
            return response
        else:
            result = {"message": "No Observation with given Id found"}
            response = jsonify(result)
            response.status_code = 200
            return response


api.add_resource(Observation, "/OGCSensorThings/v1.0/Observations(<int:id>)")


class ObservationbyDSId(Resource):
    def get(self, id):
        """
        query data streams
        #TODO pagination
        """
        try:
            obs = Observations.filter_by_datastream_id(id)
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if obs:
            response = jsonify(obs)
            response.status_code = 200
            return response
        else:
            result = {"message": "No Observation with given Id found"}
            response = jsonify(result)
            response.status_code = 200
            return response


api.add_resource(
    ObservationbyDSId, "/OGCSensorThings/v1.0/Datastreams(<int:id>)/Observations"
)


class ObservationsList(Resource):
    def get(self):
        """
        query observations
        #TODO pagination
        """
        try:
            query_parameters = request.args

            top = int(query_parameters["$top"]) if "$top" in query_parameters else 100
            if (top>500): top=500

            skip = int(query_parameters["$skip"]) if "$skip" in query_parameters else 100

            obs_list = Observations.return_page(top, skip)

        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if obs_list:
            response = jsonify(obs_list)
            response.status_code = 200
            return response
        else:
            result = {"message": "No Observations found"}
            response = jsonify(result)
            response.status_code = 200
            return response


api.add_resource(ObservationsList, "/OGCSensorThings/v1.0/Observations")
