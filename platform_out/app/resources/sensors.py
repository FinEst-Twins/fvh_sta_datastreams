from flask import jsonify, request, Blueprint
from flask_restful import Resource, Api
from app.models.sensors import Sensors
import logging
from app.resources.parser import ArgParser

logging.basicConfig(level=logging.DEBUG)

sensors_blueprint = Blueprint("Sensor", __name__)
api = Api(sensors_blueprint)


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
        if len(expand_type_list) != 0:
            expand_code = -1

    selects = set()
    allowed_selects = set(
        [
            "name",
            "description"
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


class SensorByID(Resource):
    def get(self, id):
        """
        query data streams by Sensor Id
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
            Sensor_entity = Sensors.filter_by_id(id, expand_code, selects)
            response = jsonify(Sensor_entity)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
        finally:
            return response


api.add_resource(SensorByID, "/Sensors(<int:id>)")


class SensorList(Resource):
    def get(self):
        """
        query data streams
        #TODO pagination
        """
        try:
            top, skip, expand_code, selects = parse_args(request.args)
            ds_list = Sensors.return_page_with_expand(
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


api.add_resource(SensorList, "/Sensors")