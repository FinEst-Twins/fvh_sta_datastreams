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


class FeaturesofInterest(db.Model):
    __tablename__ = "featureofinterest"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), index=True )
    description = db.Column(db.String())
    encodingtype = db.Column(db.String())
    feature = db.Column(db.String())

    def __repr__(self):
        return f"<Feature Of Interest {self.name}, {self.description}, {self.encodingtype}, {self.feature}>"

    @classmethod
    def to_json(cls, x):
        return { "name": x.name, "description": x.description, "encodingtype": x.encodingtype, "feature": x.feature }

    @classmethod
    def return_all(cls):
        return {"Features Of Interest": list(map(lambda x: FeaturesofInterest.to_json(x), FeaturesofInterest.query.all()))}

    @classmethod
    def filter_by_id(cls, id):

        FoI_list = []
        if (id):
            FoI_list = FeaturesofInterest.query.filter(
                    FeaturesofInterest.id == id
            )

        if FoI_list.count() == 0:
           result = None
        else:
            result = {f"Feature Of Interest {id}": FeaturesofInterest.to_json(FoI_list[0])}

        return result
