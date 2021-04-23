from app import db
from sqlalchemy import and_
import json
from flask import current_app
import logging


class FeaturesofInterest(db.Model):
    __tablename__ = "featureofinterest"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(), index=True)
    description = db.Column(db.String())
    encodingtype = db.Column(db.String())
    feature = db.Column(db.String())

    def __repr__(self):
        return f"<Feature_Of_Interest {self.name}, {self.description}, {self.encodingtype}, {self.feature}>"

    @classmethod
    def to_json(cls, x):
        result = {
            "@iot.id": x.id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/FeaturesOfInterest({x.id})",
            "name": x.name,
            "description": x.description,
            "encodingtype": x.encodingtype,
            "feature": json.loads(x.feature),
        }
        return result

    @classmethod
    def return_all(cls):
        return {
            "Features_Of_Interest": list(
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
            result = FeaturesofInterest.to_json(FoI_list[0])

        return result

    @classmethod
    def add_item(cls, name, description, encodingtype, feature):

        foi = FeaturesofInterest(
            name=name,
            description=description,
            encodingtype=encodingtype,
            feature=feature,
        )
        db.session.add(foi)
        db.session.commit()
        return {"created id": foi.id}

    @classmethod
    def update_item(cls, id, name, description, encodingtype, feature):

        try:
            foi = FeaturesofInterest.query.filter(FeaturesofInterest.id == id).first()
            if foi:
                foi.name = name
                foi.description = description
                foi.encodingtype = encodingtype
                foi.feature = feature
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
            foi = FeaturesofInterest.query.filter(FeaturesofInterest.id == id).first()
            if foi:
                db.session.delete(foi)
                db.session.commit()
                resp = {"deleted id": id}
            else:
                resp = {"message": "non existent id"}
        except Exception as e:
            logging.warning(e)
            resp = {"message": "error in update"}

        return resp
