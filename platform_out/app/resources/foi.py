from flask import jsonify, Blueprint
from flask_restful import Resource, Api
from app.models.foi import FeaturesofInterest
import logging

logging.basicConfig(level=logging.INFO)

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

#api.add_resource(DSbyID, "/OGCSensorThings/v1.0/FeaturesOfInterest(<int:foi_id>)")
api.add_resource(FoI, "/OGCSensorThings/v1.0/FeaturesOfInterest/<int:foi_id>")


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


api.add_resource(FoIList, "/OGCSensorThings/v1.0/FeaturesOfInterest")
