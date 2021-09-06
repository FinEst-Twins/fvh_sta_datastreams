from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api
from app.models.datastreams import Datastreams
import logging
from app.resources.parser import ArgParser

logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",level=current_app.config["LOG_LEVEL"])

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
            "sensor",
            "observation",
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


class DataStream(Resource):
    def get(self):
        """
        query data streams using thing and sensor as parameters
        """
        try:
            query_parameters = request.args
            logging.debug(f"{query_parameters}")
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
            top, skip, expand_code, selects = parse_args(request.args)
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
        query observations by datastream id
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)

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




class DSList(Resource):
    def get(self):
        """
        query data streams
        #TODO pagination
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
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
