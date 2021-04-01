from app import db


class Sensors(db.Model):
    __tablename__ = "sensor"
    id = db.Column(db.Integer, primary_key=True)
    link = db.Column(db.String())

    @classmethod
    def to_json(cls, x):
        return {
            "id": x.id,
            "link": x.link
        }

    @classmethod
    def filter_by_id(cls, id, expand_code, selects):

        FoI_list = []
        if id:
            FoI_list = Sensors.query.filter(Sensors.id == id)

        if FoI_list.count() == 0:
            result = {"message":"No Sensors found with given Id"}
        else:
            result = {f"Thing {id}": Sensors.to_json(FoI_list[0])}

        return result

    @classmethod
    def return_all(cls):
        return {
            "Sensors": list(
                map(lambda x: Sensors.to_json(x), Sensors.query.all())
            )
        }
