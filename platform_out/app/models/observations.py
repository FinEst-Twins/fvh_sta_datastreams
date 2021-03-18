from app import db
from flask import current_app
import logging

logging.basicConfig(level=logging.INFO)


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
            "Datastream@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Observations({x.id})/Datastream" ,
            "FeatureOfInterest@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Observations({x.id})/FeatureOfInterest",
        }

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
    def filter_by_datastream_id(cls, id):

        obs_list = []
        if id:
            obs_list = Observations.query.filter(Observations.datastream_id == id)

        if obs_list.count() == 0:
            result = None
        else:
            result =  {
            "Datastreams": list(map(lambda x: Observations.to_json(x), obs_list))
        }

        return result


    @classmethod
    def return_page(cls, top, skip):
        count = Observations.query.count()
        obs_list ={
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Observations?$top={top}&$skip={skip+100}",
            "value": list(
                map(lambda x: Observations.to_json(x), Observations.query.limit(top).offset(skip).all())
            )
        }
        return obs_list

