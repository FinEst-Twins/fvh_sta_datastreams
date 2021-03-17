from app import db
from sqlalchemy import and_
import json


class FeaturesofInterest(db.Model):
    __tablename__ = "featureofinterest"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), index=True)
    description = db.Column(db.String())
    encodingtype = db.Column(db.String())
    feature = db.Column(db.String())

    def __repr__(self):
        return f"<Feature Of Interest {self.name}, {self.description}, {self.encodingtype}, {self.feature}>"

    @classmethod
    def to_json(cls, x):
        result = {
            "name": x.name,
            "description": x.description,
            "encodingtype": x.encodingtype,
            "feature": x.feature,
        }
        result["feature"] = json.loads(result["feature"])
        return result

    @classmethod
    def return_all(cls):
        return {
            "Features Of Interest": list(
                map(
                    lambda x: FeaturesofInterest.to_json(x),
                    FeaturesofInterest.query.all(),
                )
            )
        }

    @classmethod
    def filter_by_id(cls, id):

        FoI_list = []
        if id:
            FoI_list = FeaturesofInterest.query.filter(FeaturesofInterest.id == id)

        if FoI_list.count() == 0:
            result = None
        else:
            result = {
                f"Feature Of Interest {id}": FeaturesofInterest.to_json(FoI_list[0])
            }

        return result
