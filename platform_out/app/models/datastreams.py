from app import db
from sqlalchemy import and_
from flask import current_app
from app.models.things import Things
from app.models.sensors import Sensors
import logging

logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",
    level=current_app.config["LOG_LEVEL"],
)


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
    observedproperty_link = db.Column(db.String())
    thing_id = db.Column(db.Integer, index=True)
    sensor_id = db.Column(db.Integer, index=True)

    @classmethod
    def to_json(cls, x):
        """
        returns datastreams in json format
        """
        return {
            "@iot.id": x.id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Datastreams({x.id})",
            "name": x.name,
            "description": x.description,
            "unitOfMeasurement": x.unitofmeasurement,
            "Sensor@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Datastreams({x.id})/Sensors",
            "Thing@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Datastreams({x.id})/Things",
        }

    @classmethod
    def to_selected_json(cls, x, selectparams):
        """
        returns selected fields of observations in json format
        """
        datadict = Datastreams.to_json(x)

        if selectparams:
            key_dict = {
                "thing": "Thing@iot.navigationLink",
                "sensor": "Sensor@iot.navigationLink",
                "name": "name",
                "description": "description",
                "unitofmeasurement": "unitOfMeasurement",
            }

            new_result = {}
            for key in selectparams:
                new_result[key_dict[key]] = datadict[key_dict[key]]

            datadict = new_result

        return datadict

    @classmethod
    def to_expanded_thing_json(cls, data_dict, x):
        """
        returns Datastreams json with Thing expanded as its own json object
        """
        del data_dict["Thing@iot.navigationLink"]
        data_dict["Thing"] = {
            "@iot.id": x.thing_id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Things({x.thing_id})",
            "name": x.thingname,
            "description": x.thingdesc,
        }
        return data_dict

    @classmethod
    def to_expanded_sensor_json(cls, data_dict, x):
        """
        returns Observations json with FeatreOfInterest expanded as its own json object
        """
        del data_dict["Sensor@iot.navigationLink"]
        if x.sensor_id:
            data_dict["Sensor"] = {
                "@iot.id": x.sensor_id,
                "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Sensors({x.sensor_id})",
                "name": x.sensorname,
                "description": x.sensordesc,
            }
        else:
            data_dict["Sensor"] = None
        return data_dict

    @classmethod
    def expand_to_selected_json(cls, x, expand_code, selects):
        """
        applies expansion of fields as per expand code given
        """
        result = Datastreams.to_selected_json(x, selects)

        if selects is None:
            select_thing = True
            select_sensor = True
        else:
            select_thing = True if ("thing" in selects) else False
            select_sensor = True if ("sensor" in selects) else False

        if (expand_code == 1 or expand_code == 3) and select_thing:
            result = Datastreams.to_expanded_thing_json(result, x)
        if (expand_code == 2 or expand_code == 3) and select_sensor:
            result = Datastreams.to_expanded_sensor_json(result, x)

        return result

    @classmethod
    def get_nextlink_queryparams(cls, top, skip, expand_code):
        """
        returns nextLink used for paginating based on current parameters
        """
        # TODO see if skip overreaches limit

        query_params = []
        if top:
            query_params.append(f"$top={top}")
        if skip >= 0:
            query_params.append(f"$skip={skip+100}")

        if expand_code > 0:
            expand_strings_list = []
            if expand_code == 1 or expand_code == 3:
                expand_strings_list.append("thing")
            if expand_code == 2 or expand_code == 3:
                expand_strings_list.append("sensor")

            query_params.append(f"$expand={','.join(expand_strings_list)}")

        url_string = f"?{'&'.join(query_params)}"

        if url_string == "?":
            url_string == ""

        logging.debug(url_string)
        return url_string

    @classmethod
    def get_expanded_query(cls, base_query, top, skip, expand_code):
        """
        applies join query based on expand code.
        applies limit() and offset() with top and skip paremeters
        """
        query = None
        if expand_code >= 0:
            base_query = base_query.add_columns(
                Datastreams.id,
                Datastreams.name,
                Datastreams.description,
                Datastreams.unitofmeasurement,
                Datastreams.thing_id,
                Datastreams.sensor_id,
            )

            if expand_code == 1 or expand_code == 3:
                base_query = base_query.join(
                    Things, Datastreams.thing_id == Things.id
                ).add_columns(
                    Things.name.label("thingname"),
                    Things.description.label("thingdesc"),
                )
            if expand_code == 2 or expand_code == 3:
                base_query = base_query.join(
                    Sensors, Datastreams.sensor_id == Sensors.id
                ).add_columns(
                    Sensors.name.label("sensorname"),
                    Sensors.description.label("sensordesc"),
                )

            query = base_query.limit(top).offset(skip)

        # logging.debug(str(query))

        return query

    @classmethod
    def filter_by_id(cls, id, expand_code, selects):

        if expand_code != -1:
            result = Datastreams.get_expanded_query(
                Datastreams.query.filter(Datastreams.id == id),
                1,
                0,
                expand_code,
            ).first()

            if result is None:
                result = {"message": "No Datastream with given Id found"}
            else:
                result = Datastreams.expand_to_selected_json(
                    result, expand_code, selects
                )
        else:
            result = {"error": "unrecognized expand options"}
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
    def filter_by_thing_id(cls, id, top, skip, expand_code, selects):
        """
        applies query to filter Observations by datastream id
        """
        logging.debug("func called")
        count = Datastreams.query.filter(Datastreams.thing_id == id).count()
        if count == 0:
            logging.debug("No daatstreams found for given thing id")
            obs_list = {"@iot.count": count}
        elif expand_code != -1:
            obs_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Things({id})/Datastreams{Datastreams.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Datastreams.expand_to_selected_json(
                            x, expand_code, selects
                        ),
                        Datastreams.get_expanded_query(
                            Datastreams.query.filter(Datastreams.thing_id == id),
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
    def filter_by_sensor_id(cls, id, top, skip, expand_code, selects):
        """
        applies query to filter Observations by datastream id
        """
        logging.debug("func called")
        count = Datastreams.query.filter(Datastreams.sensor_id == id).count()
        if count == 0:
            logging.debug("No daatstreams found for given sensor id")
            obs_list = {"@iot.count": count}
        elif expand_code != -1:
            obs_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Sensors({id})/Datastreams{Datastreams.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Datastreams.expand_to_selected_json(
                            x, expand_code, selects
                        ),
                        Datastreams.get_expanded_query(
                            Datastreams.query.filter(Datastreams.sensor_id == id),
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
    def return_page_with_expand(cls, top, skip, expand_code, selects):
        """
        applies query to join Datastreams table with things table or sensor table or both
        based on expand code
        """
        logging.debug(
            f"top={top}, skip={skip}, expand={expand_code}, selects={selects}"
        )
        count = Datastreams.query.count()
        if count == 0:
            ds_list = {"@iot.count": count}
        elif expand_code != -1:
            ds_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Datastreams{Datastreams.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Datastreams.expand_to_selected_json(
                            x, expand_code, selects
                        ),
                        Datastreams.get_expanded_query(
                            Datastreams.query, top, skip, expand_code
                        ).all(),
                    )
                ),
            }
        else:
            ds_list = {"error": "unrecognized expand options"}

        return ds_list

    @classmethod
    def add_item(cls, name, description, unitofmeasurement, thing_id, sensor_id):
        logging.debug("func called")

        ds = Datastreams(
            name=name,
            description=description,
            unitofmeasurement=unitofmeasurement,
            thing_id=int(thing_id),
            sensor_id=int(sensor_id),
        )
        db.session.add(ds)
        db.session.commit()
        return {"created id": ds.id}

    @classmethod
    def update_item(cls, id, name, description, unitofmeasurement, thing_id, sensor_id):
        logging.debug("func called")
        try:
            ds = Datastreams.query.filter(Datastreams.id == id).first()
            if ds:
                ds.name = (name,)
                ds.description = description
                ds.unitofmeasurement = unitofmeasurement
                ds.thing_id = int(thing_id)
                ds.sensor_id = int(sensor_id)
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
        logging.debug("func called")
        try:
            ds = Datastreams.query.filter(Datastreams.id == id).first()
            if ds:
                db.session.delete(ds)
                db.session.commit()
                resp = {"deleted id": id}
            else:
                resp = {"message": "non existent id"}
        except Exception as e:
            logging.warning(e)
            resp = {"message": "error in delete"}

        return resp

    @classmethod
    def find_datastream_by_datastream_id(cls, id):
        """
        applies query to filter Observations by Observation id
        """

        result = Datastreams.query.filter(Datastreams.id == id).first()

        return result
