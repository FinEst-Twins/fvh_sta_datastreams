from flask import jsonify, Blueprint, request
from flask_restful import Resource, Api
from app.models.observations import Observations
import logging


logging.basicConfig(level=logging.INFO)


observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)


def parse_args(query_parameters):

    top = None
    skip = None
    expand_code = None

    top = int(query_parameters["$top"]) if "$top" in query_parameters else 100

    skip = int(query_parameters["$skip"]) if "$skip" in query_parameters else 0

    expand_type_list = []
    expand_code = 0
    if "$expand" in query_parameters:
        expand_type_list = list(set(query_parameters["$expand"].lower().split(",")))
        if len(expand_type_list) == 0:
            expand_code = -1
        if "datastream" in expand_type_list:
            expand_code += 1
            expand_type_list.remove("datastream")
        if "featureofinterest" in expand_type_list:
            expand_code += 2
            expand_type_list.remove("featureofinterest")
        if len(expand_type_list) != 0:
            expand_code = -1

    selects = set()
    allowed_selects = set(
        [
            "phenomenontimebegin",
            "phenomenontimeend",
            "result",
            "resulttime",
            "datastream",
            "featureofinterest",
        ]
    )

    if "$select" in query_parameters:
        selects = set(query_parameters["$select"].lower().split(","))

        if (selects - allowed_selects) != set():
            logging.debug(f" selects - allowed selects {selects - allowed_selects}")
            raise Exception("Unrecognized select options")
    else:
        selects = None

    return top, skip, expand_code, selects



class Observation(Resource):
    def get(self, id):
        """
        query observations by obervation id
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
            obs = Observations.filter_by_id(id, expand_code, selects)

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
        query observations by datastream id
        """
        try:
            top, skip, expand_code = parse_args(request.args)

            obs = Observations.filter_by_datastream_id(id, top, skip, expand_code)
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
            result = {"message": "No Observation found for given datastream"}
            response = jsonify(result)
            response.status_code = 200
            return response


api.add_resource(
    ObservationbyDSId, "/OGCSensorThings/v1.0/Datastreams(<int:id>)/Observations"
)


class ObservationsList(Resource):
    def get(self):
        """
        query all obervations
        """
        try:
            top, skip, expand_code = parse_args(request.args)
            obs_list = Observations.return_page_with_expand(top, skip, expand_code)
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
