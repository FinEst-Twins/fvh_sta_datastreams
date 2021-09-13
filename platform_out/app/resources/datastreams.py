from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api
from app.models.datastreams import Datastreams
from app.models.observations import Observations
import logging
from app.resources.parser import ArgParser

logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",
    level=current_app.config["LOG_LEVEL"],
)

datastreams_blueprint = Blueprint("datastream", __name__)
api = Api(datastreams_blueprint)


def parse_args(query_parameters):

    top, skip, expand, select = ArgParser.get_args()

    expand_type_list = []
    expand_code = 0
    if expand:
        expand_type_list = list(set(expand.lower().split(",")))
        if len(expand_type_list) == 0:
            expand_code = -1
        if "thing" in expand_type_list:
            expand_code += 1
            expand_type_list.remove("thing")
        if "sensor" in expand_type_list:
            expand_code += 2
            expand_type_list.remove("sensor")
        if len(expand_type_list) != 0:
            expand_code = -1

    selects = set()
    allowed_selects = set(
        [
            "name",
            "description",
            "unitofmeasurement",
            "thing",
            "sensor"
        ]
    )

    if select:
        selects = set(select.lower().split(","))

        if (selects - allowed_selects) != set():
            logging.debug(f" selects - allowed selects {selects - allowed_selects}")
            raise Exception("Unrecognized select options")
    else:
        selects = None

    logging.debug(
        f"parseg args top, skip, expand_code, selects - {top}, {skip}, {expand_code}, {selects}"
    )

    return top, skip, expand_code, selects


class DataStream(Resource):
    def get(self):
        """
        query data streams using thing and sensor as parameters
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {query_parameters}")
            if query_parameters:
                thing = None
                sensor = None
                if "thing" in query_parameters:
                    thing = request.args["thing"]
                if "sensor" in query_parameters:
                    sensor = request.args["sensor"]
                logging.debug(f"thing={thing},sensor={sensor}")
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


class DSbyID(Resource):
    def get(self, ds_id):
        """
        query data streams by Datastream Id
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {query_parameters}")
            top, skip, expand_code, selects = parse_args(query_parameters)
            datastream_entity = Datastreams.filter_by_id(ds_id, expand_code, selects)
            response = jsonify(datastream_entity)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
        finally:
            return response

    def patch(self, ds_id):
        """
        post new sensor
        """
        try:
            data = request.get_json() or {}
            logging.debug(f"patching with - {data}")

            if (
                "unitofmeasurement" not in data.keys()
                or "thing_id" not in data.keys()
                or "sensor_id" not in data.keys()
                or "name" not in data.keys()
                or "description" not in data.keys()
            ):
                result = {
                    "message": "error - must include name, description, unit of measurement, thing_id(int) and sensor_id(int) fields"
                }
                response = jsonify(result)
                response.status_code = 200
            else:
                result = Datastreams.update_item(
                    ds_id,
                    data["name"],
                    data["description"],
                    data["unitofmeasurement"],
                    data["thing_id"],
                    data["sensor_id"],
                )
                response = jsonify(result)
                response.status_code = 200
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400

        finally:
            return response

    def delete(self, ds_id):
        """
        delete  datastreams
        """
        try:
            logging.debug(f"delete - {ds_id}")
            result = Datastreams.delete_item(ds_id)
            response = jsonify(result)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400

        finally:
            return response


api.add_resource(DSbyID, "/Datastreams(<int:ds_id>)")


class DatastreamsbyThingsId(Resource):
    def get(self, id):
        """
        query observations by thing id
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {query_parameters}")
            top, skip, expand_code, selects = parse_args(query_parameters)

            ds_list = Datastreams.filter_by_thing_id(
                id, top, skip, expand_code, selects
            )
            response = jsonify(ds_list)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400
            return response

        finally:
            return response


api.add_resource(DatastreamsbyThingsId, "/Things(<int:id>)/Datastreams")


class DatastreamsbySensorsId(Resource):
    def get(self, id):
        """
        query datastream by sensor id
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {query_parameters}")
            top, skip, expand_code, selects = parse_args(query_parameters)

            ds_list = Datastreams.filter_by_sensor_id(
                id, top, skip, expand_code, selects
            )
            response = jsonify(ds_list)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400
            return response

        finally:
            return response


api.add_resource(DatastreamsbySensorsId, "/Sensors(<int:id>)/Datastreams")


class DatastreamsbyObservationId(Resource):
    def get(self, id):
        """
        query datastream by observation id
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {str(query_parameters)}")
            obs = Observations.find_observation_by_observation_id(id)

            top, skip, expand_code, selects = parse_args(query_parameters)
            if obs:
                ds_list = Datastreams.filter_by_id(
                    obs.datastream_id, expand_code, selects
                )
                response = jsonify(ds_list)

            else:
                response = jsonify({"message": "No Observations with given Id found"})
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400
            return response

        finally:
            return response


api.add_resource(DatastreamsbyObservationId, "/Observations(<int:id>)/Datastreams")


class DSList(Resource):
    def get(self):
        """
        query data streams
        #TODO pagination
        """
        try:
            query_parameters = request.args
            logging.debug(f" query params - {query_parameters}")
            top, skip, expand_code, selects = parse_args(query_parameters)
            ds_list = Datastreams.return_page_with_expand(
                top, skip, expand_code, selects
            )
            response = jsonify(ds_list)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400

        finally:
            return response

    def post(self):
        """
        post new sensor
        """
        try:
            data = request.get_json() or {}
            logging.debug(f"posting with - {data}")

            if (
                "unitofmeasurement" not in data.keys()
                or "thing_id" not in data.keys()
                or "sensor_id" not in data.keys()
                or "name" not in data.keys()
                or "description" not in data.keys()
            ):
                result = {
                    "message": "error - must include name, description, unit of measurement, thing_id(int) and sensor_id(int) fields"
                }
                response = jsonify(result)
                response.status_code = 200
            else:
                result = Datastreams.add_item(
                    data["name"],
                    data["description"],
                    data["unitofmeasurement"],
                    data["thing_id"],
                    data["sensor_id"],
                )
                response = jsonify(result)
                response.status_code = 201
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400

        finally:
            return response


api.add_resource(DSList, "/Datastreams")
