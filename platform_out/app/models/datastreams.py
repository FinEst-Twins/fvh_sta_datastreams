from app import db
from sqlalchemy import and_


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

    def to_json(x):
        return {
            "datastream_id": x.id,
            "name": x.name,
            "description": x.description,
            "unit_of_measurement": x.unitofmeasurement,
            "thing_link": x.thing_link,
            "sensor_link": x.sensor_link,
        }

    @classmethod
    def filter_by_id(cls, id):

        FoI_list = []
        if id:
            FoI_list = Datastreams.query.filter(Datastreams.id == id)

        if FoI_list.count() == 0:
            result = None
        else:
            result = {f"Datastream_{id}": Datastreams.to_json(FoI_list[0])}

        return result

    @classmethod
    def filter_by_thing_sensor(cls, thing, sensor):

        datastream_list = []
        if (not thing) and sensor:
            datastream_list = Datastreams.query.filter(
                Datastreams.sensor_link == sensor
            )

        elif (not sensor) and thing:
            datastream_list = Datastreams.query.filter(Datastreams.thing_link == thing)

        else:
            datastream_list = Datastreams.query.filter(
                and_(
                    Datastreams.thing_link == thing,
                    Datastreams.sensor_link == sensor,
                )
            )

        return {
            "Datastreams": list(map(lambda x: Datastreams.to_json(x), datastream_list))
        }

    @classmethod
    def return_all(cls):
        return {
            "Datastreams": list(
                map(lambda x: Datastreams.to_json(x), Datastreams.query.all())
            )
        }
