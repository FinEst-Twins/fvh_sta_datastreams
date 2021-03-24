from app import db
from flask import current_app
import logging
from enum import Enum
from app.models.datastreams import Datastreams
from app.models.foi import FeaturesofInterest
import json

logging.basicConfig(level=logging.DEBUG)


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

    @classmethod
    def to_json(cls, x):
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

    @classmethod
    def to_selected_json(cls, x, selectparams):
        datadict = Observations.to_json(x)

        key_dict = {
            "datastream": "Datastream@iot.navigationLink",
            "featureofinterest": "FeatureOfInterest@iot.navigationLink",
            "phenomenontimebegin": "phenomenonTimeBegin",
            "phenomenontimeend": "phenomenonTimeEnd",
            "result": "result",
            "resulttime": "resultTime",
        }

        new_result = {}
        for key in selectparams:
            new_result[key] = datadict[key_dict[key]]

        return new_result

    @classmethod
    def to_expanded_datastream_json(cls, data_dict, x):
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

    @classmethod
    def to_expanded_foi_json(cls, data_dict, x):
        del data_dict["FeatureOfInterest@iot.navigationLink"]
        if x.featureofinterest_id:
            data_dict["FeatureOfInterest"] = {
                "@iot.id": x.featureofinterest_id,
                "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest({x.featureofinterest_id})",
                "name": x.foi_name,
                "description": x.foi_description,
                "encodingtype": x.foi_encodingtype,
                "feature": json.loads(x.foi_feature),
                "Observations@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest({x.featureofinterest_id})/Observations",
            }
        else:
            data_dict["FeatureOfInterest"] = None
        return data_dict

    @classmethod
    def expand_to_json(cls, x, expand_code):
        result = Observations.to_json(x)

        if expand_code == 1 or expand_code == 3:
            result = Observations.to_expanded_datastream_json(result, x)
        if expand_code == 2 or expand_code == 3:
            result = Observations.to_expanded_foi_json(result, x)

        return result

    @classmethod
    def get_nextlink_queryparams(cls, top, skip, expand_code):
        # TODO see if skip overreaches limit

        query_params = []
        if top:
            query_params.append(f"$top={top}")
        if skip >= 0:
            query_params.append(f"$skip={skip+100}")

        expand_strings_list = []
        if expand_code == 1 or expand_code == 3:
            expand_strings_list.append("datastream")
        if expand_code == 2 or expand_code == 3:
            expand_strings_list.append("featureofinterest")

        query_params.append(f"$expand={','.join(expand_strings_list)}")

        url_string = f"?{'&'.join(query_params)}"

        if url_string == "?":
            url_string == ""

        return url_string

    @classmethod
    def get_expanded_query(cls, base_query, top, skip, expand_code):
        query = None
        if expand_code == 0:
            query = base_query.limit(top).offset(skip)
        elif expand_code == 1:
            query = (
                base_query.join(
                    Datastreams, Observations.datastream_id == Datastreams.id
                )
                .add_columns(
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
                )
                .limit(top)
                .offset(skip)
            )
        elif expand_code == 2:
            query = (
                base_query.outerjoin(
                    FeaturesofInterest,
                    Observations.featureofinterest_id == FeaturesofInterest.id,
                )
                .add_columns(
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
                )
                .limit(top)
                .offset(skip)
            )
        elif expand_code == 3:
            query = (
                base_query.join(
                    Datastreams, Observations.datastream_id == Datastreams.id
                )
                .outerjoin(
                    FeaturesofInterest,
                    Observations.featureofinterest_id == FeaturesofInterest.id,
                )
                .add_columns(
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
                )
                .limit(top)
                .offset(skip)
            )

        return query

    @classmethod
    def filter_by_id(cls, id, selects):

        obs_list = []
        if id:
            obs_list = Observations.query.filter(Observations.id == id)

        if obs_list.count() == 0:
            result = None
        elif selects:
            result = Observations.to_selected_json(obs_list[0], selects)
        else:
            result = Observations.to_json(obs_list[0])

        return result

    @classmethod
    def filter_by_datastream_id(cls, id, top, skip, expand_code):

        count = Observations.query.filter(Observations.datastream_id == id).count()
        if count == 0:
            obs_list = {"@iot.count": count}
        elif expand_code != -1:
            obs_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Datastreams({id})/Observations{Observations.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Observations.expand_to_json(x, expand_code),
                        Observations.get_expanded_query(
                            Observations.query.filter(Observations.datastream_id == id),
                            top,
                            skip,
                            expand_code,
                        ).all(),
                    )
                ),
            }
        else:
            obs_list = {"error": "unrecognized expand options"}
        return obs_list

    @classmethod
    def return_page_with_expand(cls, top, skip, expand_code):
        count = Observations.query.count()
        if count == 0:
            obs_list = {"@iot.count": count}
        elif expand_code != -1:
            obs_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations{Observations.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Observations.expand_to_json(x, expand_code),
                        Observations.get_expanded_query(
                            Observations.query, top, skip, expand_code
                        ).all(),
                    )
                ),
            }
        else:
            obs_list = {"error": "unrecognized expand options"}

        return obs_list
