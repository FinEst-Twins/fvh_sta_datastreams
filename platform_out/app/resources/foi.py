from flask import jsonify, Blueprint, request, current_app
from flask_restful import Resource, Api
from app.models.foi import FeaturesofInterest
import logging
import json

logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",level=current_app.config["LOG_LEVEL"])

foi_blueprint = Blueprint("featuresofinterest", __name__)
api = Api(foi_blueprint)


class FoI(Resource):
    def get(self, foi_id):
        """
        query data streams
        #TODO pagination
        """
        try:
            foi = FeaturesofInterest.filter_by_id(foi_id)
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if foi:
            response = jsonify(foi)
            response.status_code = 200
            return response
        else:
            result = {"message": "No Feature Of Interest with given Id found"}
            response = jsonify(result)
            response.status_code = 200
            return response

    def patch(self, foi_id):
        """
        update existing feature fo interest
        """
        try:
            data = request.get_json() or {}

            if (
                "encodingtype" not in data.keys()
                or "feature" not in data.keys()
                or "name" not in data.keys()
                or "description" not in data.keys()
            ):
                result = {
                    "message": "error - must include name, description, encodingtype and feature fields"
                }
                response = jsonify(result)
                response.status_code = 200
            else:
                result = FeaturesofInterest.update_item(
                    foi_id,
                    data["name"],
                    data["description"],
                    data["encodingtype"],
                    json.dumps(data["feature"]),
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

    def delete(self, foi_id):
        """
        delete  foi
        """
        try:

            result = FeaturesofInterest.delete_item(foi_id)
            response = jsonify(result)
            response.status_code = 200
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400

        finally:
            return response


api.add_resource(FoI, "//FeaturesOfInterest(<int:foi_id>)")


class FoIList(Resource):
    def get(self):
        """
        query data streams
        #TODO pagination
        """
        try:
            foi = FeaturesofInterest.return_all()
        except Exception as e:
            logging.warning(e)
            result = {"message": "error"}
            response = jsonify(result)
            response.status_code = 400
            return response

        if foi:
            response = jsonify(foi)
            response.status_code = 200
            return response
        else:
            result = {"message": "No Features Of Interest found"}
            response = jsonify(result)
            response.status_code = 200
            return response

    def post(self):
        """
        post new feature fo interest
        """
        try:
            data = request.get_json() or {}

            if (
                "encodingtype" not in data.keys()
                or "feature" not in data.keys()
                or "name" not in data.keys()
                or "description" not in data.keys()
            ):
                result = {
                    "message": "error - must include name, description, encodingtype and feature fields"
                }
                response = jsonify(result)
                response.status_code = 200
            else:
                result = FeaturesofInterest.add_item(
                    data["name"],
                    data["description"],
                    data["encodingtype"],
                    json.dumps(data["feature"]),
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


api.add_resource(FoIList, "/FeaturesOfInterest")
