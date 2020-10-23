from app import db
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import HSTORE, JSON
from sqlalchemy.ext.mutable import MutableDict


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
    parameters = db.Column(MutableDict.as_mutable(JSON))
    datastream_id = db.Column(db.Integer, index=True)
    featureofintrest_link = db.Column(db.String(), index=True)

    def __repr__(self):
        return f"<Observation {self.result}, {self.resulttime}>"

    @classmethod
    def filter_by_resultime(cls, mintime, maxtime):

        if not mintime:
            obs_list = Observations.query.filter(Observations.resulttime <= maxtime)

        elif not maxtime:
            obs_list = Observations.query.filter(Observations.resulttime >= mintime)

        else:
            obs_list = Observations.query.filter(
                and_(
                    Observations.resulttime <= maxtime,
                    Observations.resulttime >= mintime,
                )
            )

        def to_json(x):
            return {"result": x.result, "result time": x.resulttime}

        return {"Observations": list(map(lambda x: to_json(x), obs_list))}

    @classmethod
    def return_all(cls):
        def to_json(x):
            return {"result": x.result, "result time": x.resulttime}

        return {
            "Observations": list(map(lambda x: to_json(x), Observations.query.all()))
        }


class AssetData(db.Model):
    __tablename__ = "asset_data_hstore"
    id = db.Column(db.Integer, primary_key=True)
    asset_name = db.Column(db.String(64), index=True, unique=True)
    asset_data = db.Column(MutableDict.as_mutable(HSTORE))

    # def __init__(self, asset_name, asset_data):
    #     self.asset_data = asset_data
    #     self.asset_name = asset_name

    def __repr__(self):
        return f"<Data Stream {self.asset_name}, {self.asset_data}>"

    @classmethod
    def return_all(cls):
        def to_json(x):
            return {"asset name": x.asset_name, "asset data": x.asset_data}

        return {"DataStreams": list(map(lambda x: to_json(x), AssetData.query.all()))}
