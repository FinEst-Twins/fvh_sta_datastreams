from flask import jsonify, Blueprint
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


api.add_resource(Observation, "/OGCSensorThings/v1.0/Observations/<int:id>")


# class ObservationsList(Resource):
#     def get(self):
#         """
#         query data streams
#         #TODO pagination
#         """
#         try:
#             foi = Observations.return_all()
#         except Exception as e:
#             logging.warning(e)
#             result = {"message": "error"}
#             response = jsonify(result)
#             response.status_code = 400
#             return response

#         if foi:
#             response = jsonify(foi)
#             response.status_code = 200
#             return response
#         else:
#             result = {"message": "No Features Of Interest found"}
#             response = jsonify(result)
#             response.status_code = 200
#             return response


# api.add_resource(ObservationsList, "/OGCSensorThings/v1.0/Observations")
