from app import db
from flask import current_app
import logging
from enum import Enum
from app.models.datastreams import Datastreams
from app.models.foi import FeaturesofInterest
import json

logging.basicConfig(level=logging.INFO)


# class ExpandType(Enum):
#     Neither = 0
#     Datastream = 1
#     FeatureOfInterest = 2
#     Both = 3


class Observations(db.Model):
    __tablename__ = "observation"
    id = db.Column(db.Integer, primary_key=True)
    phenomenontime_begin = db.Column(db.DateTime(), index=True)
    phenomenontime_end = db.Column(db.DateTime(), index=True)
    resulttime = db.Column(db.DateTime(), index=True)
    result = db.Column(db.String(), index=True)
    resultquality = db.Column(db.String(), index=True)
    validtime_begin = db.Column(db.DateTime(), index=True)
    validtime_end = db.Column(db.DateTime(), index=True)
    resultquality = db.Column(db.String(), index=True)
    # parameters = db.Column(JsonEncodedDict)
    # parameters = db.Column(MutableDict.as_mutable(JSON))
    datastream_id = db.Column(db.Integer, index=True)
    featureofintrest_link = db.Column(db.String(), index=True)
    featureofinterest_id = db.Column(db.Integer(), index=True)

    def __repr__(self):
        return f"<Observation {self.result}, {self.resulttime}>"

    def to_json(x):
        return {
            "@iot.id": x.id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Observations({x.id})",
            "phenomenonTimeBegin": x.phenomenontime_begin,
            "phenomenonTimeEnd": x.phenomenontime_end,
            "resultTime": x.resulttime,
            "result": x.result,
            "Datastream@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Observations({x.id})/Datastream",
            "FeatureOfInterest@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Observations({x.id})/FeatureOfInterest",
        }

    def to_expanded_datastream_json(data_dict, x):
        del data_dict["Datastream@iot.navigationLink"]
        data_dict["Datastream"] = {
            "@iot.id": x.datastream_id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Datastreams({x.datastream_id})",
            "description": x.ds_name,
            "name": x.ds_description,
            "unitOfMeasurement": x.ds_unitofmeasurement,
            "Observations@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Datastreams({x.datastream_id})/Observations",
            "Sensor@iot.navigationLink": f"to be replaced with link - {x.ds_sensor_link}",
            "Thing@iot.navigationLink": f"to be replaced with link - {x.ds_thing_link}",
        }
        return data_dict

    def to_expanded_foi_json(data_dict, x):
        del data_dict["FeatureOfInterest@iot.navigationLink"]
        data_dict["FeatureOfInterest"] = {
            "@iot.id": x.featureofinterest_id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest({x.featureofinterest_id})",
            "name": x.foi_name,
            "description": x.foi_description,
            "encodingtype": x.foi_encodingtype,
            "feature": json.loads(x.foi_feature),
            "Observations@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest({x.featureofinterest_id})/Observations",
        }
        return data_dict

    def expand_to_json(x, expand_code):
        result = Observations.to_json(x)

        if (expand_code == 1 or expand_code == 3):
            result = Observations.to_expanded_datastream_json(result, x)
        if (expand_code == 2 or expand_code == 3):
            result = Observations.to_expanded_foi_json(result, x)

        return result

    def get_query(top, skip, expand_code):
        query = None
        if (expand_code == 0) :
            query = Observations.query.limit(top).offset(skip)
        elif (expand_code == 1) :
            query = Observations.query.join(
                    Datastreams, Observations.datastream_id == Datastreams.id
                ).add_columns(
                    Observations.id,
                    Observations.result,
                    Observations.resulttime,
                    Observations.phenomenontime_begin,
                    Observations.phenomenontime_end,
                    Observations.featureofinterest_id,
                    Observations.datastream_id,
                    Datastreams.unitofmeasurement.label("ds_unitofmeasurement"),
                    Datastreams.name.label("ds_name"),
                    Datastreams.description.label("ds_description"),
                    Datastreams.thing_link.label("ds_thing_link"),
                    Datastreams.sensor_link.label("ds_sensor_link"),
                ).limit(top).offset(skip)
        elif (expand_code == 2) :
            query = Observations.query.join(
                    FeaturesofInterest,
                    Observations.featureofinterest_id == FeaturesofInterest.id,
                ).add_columns(
                    Observations.id,
                    Observations.result,
                    Observations.resulttime,
                    Observations.phenomenontime_begin,
                    Observations.phenomenontime_end,
                    Observations.featureofinterest_id,
                    Observations.datastream_id,
                    FeaturesofInterest.name.label("foi_name"),
                    FeaturesofInterest.description.label("foi_description"),
                    FeaturesofInterest.feature.label("foi_feature"),
                    FeaturesofInterest.encodingtype.label("foi_encodingtype"),
                ).limit(top).offset(skip)
        elif (expand_code == 3) :
            query = Observations.query.join(
                    FeaturesofInterest,
                    Observations.featureofinterest_id == FeaturesofInterest.id,
                ).join(Datastreams, Observations.datastream_id == Datastreams.id).add_columns(
                    Observations.id,
                    Observations.result,
                    Observations.resulttime,
                    Observations.phenomenontime_begin,
                    Observations.phenomenontime_end,
                    Observations.datastream_id,
                    Datastreams.unitofmeasurement.label("ds_unitofmeasurement"),
                    Datastreams.name.label("ds_name"),
                    Datastreams.description.label("ds_description"),
                    Datastreams.thing_link.label("ds_thing_link"),
                    Datastreams.sensor_link.label("ds_sensor_link"),
                    Observations.featureofinterest_id,
                    FeaturesofInterest.name.label("foi_name"),
                    FeaturesofInterest.description.label("foi_description"),
                    FeaturesofInterest.feature.label("foi_feature"),
                    FeaturesofInterest.encodingtype.label("foi_encodingtype"),
                ).limit(top).offset(skip)

        return query



    @classmethod
    def filter_by_id(cls, id):

        obs_list = []
        if id:
            obs_list = Observations.query.filter(Observations.id == id)

        if obs_list.count() == 0:
            result = None
        else:
            result = Observations.to_json(obs_list[0])

        return result

    @classmethod
    def filter_by_datastream_id(cls, id, top, skip):

        count = Observations.query.filter(Observations.datastream_id == id).count()
        obs_list = {
            "@iot.count": count,
            "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Datastreams({id})Observations?$top={top}&$skip={skip+100}",
            "value": list(
                map(
                    lambda x: Observations.to_json(x),
                    Observations.query.filter(Observations.datastream_id == id)
                    .limit(top)
                    .offset(skip)
                    .all(),
                )
            ),
        }

        return obs_list

    # @classmethod
    # def return_page(cls, top, skip):
    #     count = Observations.query.count()
    #     obs_list = {
    #         "@iot.count": count,
    #         "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}",
    #         "value": list(
    #             map(
    #                 lambda x: Observations.to_json(x),
    #                 Observations.query.limit(top).offset(skip).all(),
    #             )
    #         ),
    #     }
    #     return obs_list

    @classmethod
    def return_page_with_expand(cls, top, skip, expand_code):
        count = Observations.query.count()
        if expand_code != -1 :
            obs_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}&$expand=datastream,featureofinterest",
                "value": list(map(lambda x: Observations.expand_to_json(x, expand_code), Observations.get_query(top,skip,expand_code).all())),
            }
        # if expand_code == 0:
        #     obs_list = {
        #         "@iot.count": count,
        #         "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}",
        #         "value": list(
        #             map(
        #                 lambda x: Observations.to_json(x),
        #                 Observations.query.limit(top).offset(skip).all(),
        #             )
        #         ),
        #     }
        # elif expand_code == 1:  # datastream
        #     queried_list = (
        #         Observations.query.join(
        #             Datastreams, Observations.datastream_id == Datastreams.id
        #         )
        #         .add_columns(
        #             Observations.id,
        #             Observations.result,
        #             Observations.resulttime,
        #             Observations.phenomenontime_begin,
        #             Observations.phenomenontime_end,
        #             Observations.featureofinterest_id,
        #             Observations.datastream_id,
        #             Datastreams.unitofmeasurement.label("ds_unitofmeasurement"),
        #             Datastreams.name.label("ds_name"),
        #             Datastreams.description.label("ds_description"),
        #             Datastreams.thing_link.label("ds_thing_link"),
        #             Datastreams.sensor_link.label("ds_sensor_link"),
        #         )
        #         .limit(top)
        #         .offset(skip)
        #         .all()
        #     )

        #     # def expand_to_json(x):
        #     #     result = Observations.to_json(x)
        #     #     result = Observations.to_expanded_datastream_json(result, x)
        #     #     return result

        #     obs_list = {
        #         "@iot.count": count,
        #         "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}&$expand=datastream",
        #         "value": list(map(lambda x: Observations.expand_to_json(x, expand_code), queried_list)),
        #     }
        # elif expand_code == 2:  # featureofinterest
        #     queried_list = (
        #         Observations.query.join(
        #             FeaturesofInterest,
        #             Observations.featureofinterest_id == FeaturesofInterest.id,
        #         )
        #         .add_columns(
        #             Observations.id,
        #             Observations.result,
        #             Observations.resulttime,
        #             Observations.phenomenontime_begin,
        #             Observations.phenomenontime_end,
        #             Observations.featureofinterest_id,
        #             Observations.datastream_id,
        #             FeaturesofInterest.name.label("foi_name"),
        #             FeaturesofInterest.description.label("foi_description"),
        #             FeaturesofInterest.feature.label("foi_feature"),
        #             FeaturesofInterest.encodingtype.label("foi_encodingtype"),
        #         )
        #         .limit(top)
        #         .offset(skip)
        #         .all()
        #     )
        #     # def object_as_dict(obj):
        #     #    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

        #     # print(queried_list)

        #     # def expand_to_json(x):
        #     #     result = Observations.to_json(x)
        #     #     result = Observations.to_expanded_foi_json(result, x)
        #     #     return result

        #     obs_list = {
        #         "@iot.count": count,
        #         "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}&$expand=featureofinterest",
        #         "value": list(map(lambda x: Observations.expand_to_json(x, expand_code), queried_list)),
        #     }

        # elif expand_code == 3:  # both
        #     queried_list = (
        #         Observations.query.join(
        #             FeaturesofInterest,
        #             Observations.featureofinterest_id == FeaturesofInterest.id,
        #         )
        #         .join(Datastreams, Observations.datastream_id == Datastreams.id)
        #         .add_columns(
        #             Observations.id,
        #             Observations.result,
        #             Observations.resulttime,
        #             Observations.phenomenontime_begin,
        #             Observations.phenomenontime_end,
        #             Observations.datastream_id,
        #             Datastreams.unitofmeasurement.label("ds_unitofmeasurement"),
        #             Datastreams.name.label("ds_name"),
        #             Datastreams.description.label("ds_description"),
        #             Datastreams.thing_link.label("ds_thing_link"),
        #             Datastreams.sensor_link.label("ds_sensor_link"),
        #             Observations.featureofinterest_id,
        #             FeaturesofInterest.name.label("foi_name"),
        #             FeaturesofInterest.description.label("foi_description"),
        #             FeaturesofInterest.feature.label("foi_feature"),
        #             FeaturesofInterest.encodingtype.label("foi_encodingtype"),
        #         )
        #         .limit(top)
        #         .offset(skip)
        #         .all()
        #     )

        #     # # print(queried_list)
        #     # def expand_to_json(x):
        #     #     result = Observations.to_json(x)
        #     #     result = Observations.to_expanded_foi_json(result, x)
        #     #     result = Observations.to_expanded_datastream_json(result, x)
        #     #     return result

        #     obs_list = {
        #         "@iot.count": count,
        #         "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}&$expand=datastream,featureofinterest",
        #         "value": list(map(lambda x: Observations.expand_to_json(x, expand_code), queried_list)),
        #     }
        else:
            obs_list = {"error": "unrecognized expand options"}

        return obs_list
