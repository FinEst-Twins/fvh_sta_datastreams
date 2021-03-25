from app import db
from sqlalchemy import and_
from flask import current_app
from app.models.things import Things
from app.models.sensors import Sensors


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
            "unit_of_measurement": x.unitofmeasurement,
            "Sensor@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Datastream({x.id})/Sensor",
            "Thing@iot.navigationLink": f"{current_app.config['HOSTED_URL']}/Datastream({x.id})/Thing",
        }

    @classmethod
    def to_expanded_thing_json(cls, data_dict, x):
        """
        returns Datastreams json with Thing expanded as its own json object
        """
        del data_dict["Thing@iot.navigationLink"]
        data_dict["Thing"] = {
            "@iot.id": x.thing_id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Things({x.thing_id})",
            "link": x.th_link
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
                "@iot.id": x.featureofinterest_id,
                "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Sensor({x.featureofinterest_id})",
                "link": x.ss_name
            }
        else:
            data_dict["Sensor"] = None
        return data_dict

    @classmethod
    def expand_to_json(cls, x, expand_code):
        """
        applies expansion of fields as per expand code given
        """
        result = Datastreams.to_json(x)

        if expand_code == 1 or expand_code == 3:
            result = Datastreams.to_expanded_thing_json(result, x)
        if expand_code == 2 or expand_code == 3:
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

        expand_strings_list = []
        if expand_code == 1 or expand_code == 3:
            expand_strings_list.append("thing")
        if expand_code == 2 or expand_code == 3:
            expand_strings_list.append("sensor")

        query_params.append(f"$expand={','.join(expand_strings_list)}")

        url_string = f"?{'&'.join(query_params)}"

        if url_string == "?":
            url_string == ""

        return url_string

    @classmethod
    def get_expanded_query(cls, base_query, top, skip, expand_code):
        """
        applies join query based on expand code.
        applies limit() and offset() with top and skip paremeters
        """
        query = None
        if expand_code == 0:
            query = base_query.limit(top).offset(skip)
        elif expand_code == 1:
            query = (
                base_query.join(
                    Things, Datastreams.thing_id == Things.id
                )
                .add_columns(
                    Datastreams.id,
                    Datastreams.name,
                    Datastreams.description,
                    Datastreams.thing_id,
                    Datastreams.sensor_id,
                    Things.link.label("th_link")
                )
                .limit(top)
                .offset(skip)
            )
        elif expand_code == 2:
            query = (
                base_query.join(
                    Sensors,
                    Datastreams.sensor_id == Sensors.id,
                )
                .add_columns(
                    Datastreams.id,
                    Datastreams.name,
                    Datastreams.description,
                    Datastreams.thing_id,
                    Datastreams.sensor_id,
                    Sensors.link.label("ss_link")
                )
                .limit(top)
                .offset(skip)
            )
        elif expand_code == 3:
            query = (
                base_query.join(
                    Things, Datastreams.thing_id == Things.id
                )
                .join(
                    Sensors,
                    Datastreams.sensor_id == Sensors.id,
                )
                .add_columns(
                    Datastreams.id,
                    Datastreams.name,
                    Datastreams.description,
                    Datastreams.thing_id,
                    Datastreams.sensor_id,
                    Things.link.label("th_link"),
                    Sensors.link.label("ss_link"),
                )
                .limit(top)
                .offset(skip)
            )

        return query

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
    def return_page_with_expand(cls, top, skip, expand_code, selects):
        """
        applies query to join Datastreams table with things table or sensor table or both
        based on expand code
        """
        count = Datastreams.query.count()
        if count == 0:
            ds_list = {"@iot.count": count}
        elif expand_code != -1:
            ds_list = {
                "@iot.count": count,
                "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Datastreams{Datastreams.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Datastreams.expand_to_json(x, expand_code),
                        Datastreams.get_expanded_query(
                            Datastreams.query, top, skip, expand_code
                        ).all(),
                    )
                ),
            }
        else:
            ds_list = {"error": "unrecognized expand options"}

        return ds_list