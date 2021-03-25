
from flask_restful import reqparse

class ArgParser():


    def get_args():

        parser = reqparse.RequestParser()
        parser.add_argument('$top', type=int, location="args", default=100)
        parser.add_argument('$skip', type=int, location="args", default=0)
        parser.add_argument('$expand', location="args", default=None)
        parser.add_argument('$select', location="args", default=None)
        query_parameters = parser.parse_args()

        top = query_parameters["$top"]
        skip = query_parameters["$skip"]
        expand = query_parameters["$expand"]
        select = query_parameters["$select"]

        return top, skip, expand, select

