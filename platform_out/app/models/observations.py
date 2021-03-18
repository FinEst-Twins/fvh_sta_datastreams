from app import db
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
            "id": x.id,
            "phenomenontime_begin": x.phenomenontime_begin,
            "phenomenontime_end": x.phenomenontime_end,
            "resulttime": x.resulttime,
            "result": x.result,
            "datastream_id": x.datastream_id,
            "featureofinterest_id": x.featureofinterest_id,
        }

    @classmethod
    def filter_by_id(cls, id):

        obs_list = []
        if id:
            obs_list = Observations.query.filter(Observations.id == id)

        if obs_list.count() == 0:
            result = None
        else:
            result = {f"Observation_{id}": Observations.to_json(obs_list[0])}

        return result
