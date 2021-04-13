from flask import jsonify, Blueprint, request
from flask_restful import Resource, Api
from app.models.observations import Observations
import logging
from app.resources.parser import ArgParser
from flask import current_app

logging.basicConfig(level=logging.INFO)

observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)


def parse_args(query_parameters):

    top, skip, expand, select = ArgParser.get_args()
    print(top, skip, expand, select)

    expand_type_list = []
    expand_code = 0
    if expand:
        expand_type_list = list(set(expand.lower().split(",")))
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

    if select:
        selects = set(select.lower().split(","))

        if (selects - allowed_selects) != set():
            logging.debug(f" selects - allowed selects {selects - allowed_selects}")
            raise Exception("Unrecognized select options")
    else:
        selects = None

    return top, skip, expand_code, selects


class BaseResources(Resource):
    def get(self):
        base_urls = {
            "value": [
                {
                    "name": "Things",
                    "url": f"{current_app.config['HOSTED_URL']}/Things",
                },
                # {
                #     "name": "Locations",
                #     "url": "https://toronto-bike-snapshot.sensorup.com/v1.0/Locations",
                # },
                # {
                #     "name": "HistoricalLocations",
                #     "url": "https://toronto-bike-snapshot.sensorup.com/v1.0/HistoricalLocations",
                # },
                {
                    "name": "Datastreams",
                    "url": f"{current_app.config['HOSTED_URL']}/Datastreams",
                },
                {
                    "name": "Sensors",
                    "url": f"{current_app.config['HOSTED_URL']}/Sensors",
                },
                {
                    "name": "Observations",
                    "url": f"{current_app.config['HOSTED_URL']}/Observations",
                },
                # {
                #     "name": "ObservedProperties",
                #     "url": "https://toronto-bike-snapshot.sensorup.com/v1.0/ObservedProperties",
                # },
                {
                    "name": "FeaturesOfInterest",
                    "url": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest",
                },
            ]
        }

        return jsonify(base_urls)


api.add_resource(BaseResources, "/")


class Observation(Resource):
    def get(self, id):
        """
        query observations by obervation id
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
            obs = Observations.filter_by_id(id, expand_code, selects)
            response = jsonify(obs)
            response.status_code = 200

        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400

        finally:
            return response


api.add_resource(Observation, "/Observations(<int:id>)")


class ObservationbyDSId(Resource):
    def get(self, id):
        """
        query observations by datastream id
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)

            obs = Observations.filter_by_datastream_id(
                id, top, skip, expand_code, selects
            )
            response = jsonify(obs)
            response.status_code = 200

        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400

        finally:
            return response


api.add_resource(ObservationbyDSId, "/Datastreams(<int:id>)/Observations")


class ObservationsList(Resource):
    def get(self):
        """
        query all obervations
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
            obs_list = Observations.return_page_with_expand(
                top, skip, expand_code, selects
            )
            response = jsonify(obs_list)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400
            return response

        finally:
            return response


api.add_resource(ObservationsList, "/Observations")
