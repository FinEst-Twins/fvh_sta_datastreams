from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api, reqparse, abort
from datetime import datetime
from app import swagger
from flasgger.utils import swag_from
import json
import os
from app.models import Observations
from datetime import datetime

observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)


class Observation(Resource):
    # @jwt_required
    #@swag_from("apispec/observation.yml")
    def get(self):
        """
        gets list of all observations
        """
        try:
            query_parameters = request.args
            print(query_parameters)
            if query_parameters:
                min_resulttime = None
                max_resulttime = None
                if 'minresulttime' in query_parameters:
                    min_resulttime = request.args['minresulttime']
                    min_resulttime = datetime.strptime(min_resulttime, '%Y-%m-%d,%H:%M:%S.%f')
                if 'maxresulttime' in query_parameters:
                    max_resulttime = request.args['maxresulttime']
                    max_resulttime = datetime.strptime(max_resulttime, '%Y-%m-%d,%H:%M:%S.%f')
                print(min_resulttime, max_resulttime)
                obs = Observations.filter_by_resultime(min_resulttime, max_resulttime)
            else:
                result = {"message":"not time period requested in query"}
                response = jsonify(result)
                response.status_code = 400
                return response
        
        except Exception as e:
            print(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        #obs = Observations.return_all()
        if obs:
            response = jsonify(obs)
            response.status_code = 200
            return response
        else:
            result = {"message": "No observations found"}
            response = jsonify(result)
            response.status_code = 200
            return response


api.add_resource(Observation, "/observation")


