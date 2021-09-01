from flask import jsonify, Blueprint, request, current_app
from flask_restful import Resource, Api
from app.models.observations import Observations
import logging
from app.resources.parser import ArgParser
from flask import current_app


logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",level=current_app.config["LOG_LEVEL"])
#logging.getLogger().setLevel(current_app.config["LOG_LEVEL"])

observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)


def parse_args(query_parameters):

    (
        top,
        skip,
        expand,
        select,
        orderby,
        count,
        filter_,
        resultformat,
    ) = ArgParser.get_all_args()

    logging.debug(f" top={top}, skip={skip}, expand={expand}, select={select}, orderby={orderby}, count={count}, filter={filter_}, resultformat={resultformat} ")
    param_error = None

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

    if not orderby:
        orderby = "resulttime asc"
    allowed_orderby = ["result", "resulttime"]
    allowed_orders = ["asc", "desc"]
    orderbystrings = orderby.lower().split()
    logging.debug(orderbystrings)
    if len(orderbystrings) != 2:
        param_error = {"message": "unrecognised order by"}
    else:
        if (
            orderbystrings[0] not in allowed_orderby
            or orderbystrings[1] not in allowed_orders
        ):
            param_error = {"message": "unrecognised order by"}

    if resultformat:
        if resultformat.lower() != "dataarray":
            resultformat == None

    allowed_filter_expressions = ["eq", "ne", "gt", "ge", "le", "lt"]
    allowed_filter_fields = ["result", "resulttime"]
    if filter_:
        splits = filter_.lower().split()
        if len(splits) != 3:
            param_error = {"message": "unrecognised filter query"}
        else:
            filter_field = splits[0]
            filter_expression = splits[1]
            filter_value = splits[2]

            if (
                filter_field not in allowed_filter_fields
                or filter_expression not in allowed_filter_expressions
            ):
                param_error = {"message": "unrecognised filter query"}

    return (
        top,
        skip,
        expand_code,
        selects,
        orderby,
        count,
        filter_,
        resultformat,
        param_error,
    )


class BaseResources(Resource):
    def get(self):
        logging.info("Get Base URLS")
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
            (
                top,
                skip,
                expand_code,
                selects,
                orderby,
                count,
                filter_,
                resultformat,
                param_error,
            ) = parse_args(request.args)
            obs = Observations.filter_by_id(id, expand_code, selects, orderby, filter_, resultformat)
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
            (
                top,
                skip,
                expand_code,
                selects,
                orderby,
                count,
                filter_,
                resultformat,
                param_error,
            ) = parse_args(request.args)
            if param_error:
                response = jsonify(param_error)
            else:
                obs = Observations.filter_by_datastream_id(
                    id, top, skip, expand_code, selects, orderby, filter_, resultformat
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
            (
                top,
                skip,
                expand_code,
                selects,
                orderby,
                count,
                filter_,
                resultformat,
                param_error,
            ) = parse_args(request.args)
            logging.debug(f"param error = {param_error}")
            if param_error:
                response = jsonify(param_error)
            else:
                if filter_ and filter_.lower().split()[0] == "result":
                    response = jsonify(
                        {
                            "message": "Error: Try filtering results with uniform dataformats, tip: Datastreams(x)/Observations "
                        }
                    )
                else:
                    obs_list = Observations.return_page_with_expand(
                        top, skip, expand_code, selects, orderby, filter_, resultformat
                    )
                    response = jsonify(obs_list)
                    if "message" in obs_list.keys():
                        response.status_code = 204
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            response = jsonify({"message": "error"})
            response.status_code = 400
            return response

        finally:
            return response


api.add_resource(ObservationsList, "/Observations")
