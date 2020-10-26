from app import db
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import HSTORE, JSON
from sqlalchemy.ext.mutable import MutableDict

class Datastreams(db.Model):
    __tablename__ = "datastream"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    description = db.Column(db.String())
    observedarea = db.Column(db.String())
    unitofmeasurement = db.Column(db.String())
    observationtype = db.Column(db.String())
    phenomenontime_begin = db.Column(db.DateTime())
    phenomenontime_end = db.Column(db.DateTime())
    resulttime_begin = db.Column(db.DateTime())
    resulttime_end = db.Column(db.DateTime())
    sensor_link = db.Column(db.String())
    thing_link = db.Column(db.String())
    observedproperty_link = db.Column(db.String())

    def __repr__(self):
        return f"<Observation {self.name}, {self.description}>"

    @classmethod
    def filter_by_thing_sensor(cls, thing, sensor):

        datastream_list = []
        if (not thing) and sensor:
            datastream_list = Datastreams.query.filter(Datastreams.sensor_link == sensor)

        elif (not sensor) and thing:
            datastream_list = Datastreams.query.filter(Datastreams.thing_link == thing)

        else:
            datastream_list = Datastreams.query.filter(
                and_(
                    Datastreams.thing_link == thing,
                    Datastreams.sensor_link == sensor,
                )
            )

        def to_json(x):
            return {"datastream_id": x.id, "name": x.name, "description": x.description}

        return {"Datastreams": list(map(lambda x: to_json(x), datastream_list))}


    @classmethod
    def return_all(cls):
        def to_json(x):
            return {"datastream id": x.id, "name": x.name, "description": x.description}

        return {
            "Observations": list(map(lambda x: to_json(x), Datastreams.query.all()))
        }


# class AssetData(db.Model):
#     __tablename__ = "asset_data_hstore"
#     id = db.Column(db.Integer, primary_key=True)
#     asset_name = db.Column(db.String(64), index=True, unique=True)
#     asset_data = db.Column(MutableDict.as_mutable(HSTORE))

#     # def __init__(self, asset_name, asset_data):
#     #     self.asset_data = asset_data
#     #     self.asset_name = asset_name

#     def __repr__(self):
#         return f"<Data Stream {self.asset_name}, {self.asset_data}>"

#     @classmethod
#     def return_all(cls):
#         def to_json(x):
#             return {"asset name": x.asset_name, "asset data": x.asset_data}

#         return {"DataStreams": list(map(lambda x: to_json(x), AssetData.query.all()))}
