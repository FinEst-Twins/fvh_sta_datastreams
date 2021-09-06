from app import db
from flask import current_app
import logging

logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",level=current_app.config["LOG_LEVEL"])
class Sensors(db.Model):
    __tablename__ = "sensor"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    description = db.Column(db.String())

    @classmethod
    def to_json(cls, x):
        return {
            "@iot.id": x.id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Sensors({x.id})",
            "name": x.name,
            "description": x.description,
        }

    @classmethod
    def to_selected_json(cls, x, selectparams):
        """
        returns selected fields of observations in json format
        """
        datadict = Sensors.to_json(x)

        if selectparams:

            new_result = {}
            for key in selectparams:
                new_result[key] = datadict[key]

            datadict = new_result

        return datadict

    @classmethod
    def filter_by_id(cls, id, expand_code, selects):

        sensor_list = []
        if id:
            sensor_list = Sensors.query.filter(Sensors.id == id)


        if sensor_list.count() == 0:
            result = {"message":"No Sensors found with given Id"}
        else:
            result = Sensors.to_selected_json(sensor_list[0], selects)

        return result



    @classmethod
    def return_page_with_expand(cls, top, skip, expand_code, selects):
        """
        applies query to join Datastreams table with Sensors table or sensor table or both
        based on expand code
        """
        count = Sensors.query.count()
        if count == 0:
            sensor_list = {"@iot.count": count}
        elif expand_code != -1:
            sensor_list = {
                "@iot.count": count,
                # "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Sensors{Sensors.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Sensors.to_selected_json(x, selects),
                        Sensors.query.all(),
                    )
                ),
            }
        else:
            sensor_list = {"error": "unrecognized expand options"}

        return sensor_list

    @classmethod
    def add_item(cls, name, description):

        sensor = Sensors(name=name, description=description)
        db.session.add(sensor)
        db.session.commit()
        return {"created id" : sensor.id}

    @classmethod
    def update_item(cls, id, name, description):
        try:
            sensor = Sensors.query.filter(Sensors.id == id).first()
            if sensor:
                sensor.name = (name,)
                sensor.description = description
                db.session.commit()
                resp = {"updated id": id}
            else:
                resp = {"message": "non existent id"}
        except Exception as e:
            logging.warning(e)
            resp = {"message": "error in update"}

        return resp

    @classmethod
    def delete_item(cls, id):
        try:
            sensor = Sensors.query.filter(Sensors.id == id).first()
            if sensor:
                db.session.delete(sensor)
                db.session.commit()
                resp = {"deleted id": id}
            else:
                resp = {"message": "non existent id"}
        except Exception as e:
            logging.warning(e)
            resp = {"message": "error in delete"}

        return resp