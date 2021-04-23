from app import db
from flask import current_app
import logging


class Things(db.Model):
    __tablename__ = "thing"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    description = db.Column(db.String())

    @classmethod
    def to_json(cls, x):
        return {
            "@iot.id": x.id,
            "@iot.selfLink": f"{current_app.config['HOSTED_URL']}/Things({x.id})",
            "name": x.name,
            "description": x.description,
        }

    @classmethod
    def to_selected_json(cls, x, selectparams):
        """
        returns selected fields of observations in json format
        """
        datadict = Things.to_json(x)

        if selectparams:

            new_result = {}
            for key in selectparams:
                new_result[key] = datadict[key]

            datadict = new_result

        return datadict

    @classmethod
    def filter_by_id(cls, id, expand_code, selects):

        FoI_list = []
        if id:
            FoI_list = Things.query.filter(Things.id == id)

        if FoI_list.count() == 0:
            result = {"message": "No Things found with given Id"}
        else:
            result = Things.to_selected_json(FoI_list[0], selects)

        return result

    @classmethod
    def return_page_with_expand(cls, top, skip, expand_code, selects):
        """
        applies query to join Datastreams table with things table or sensor table or both
        based on expand code
        """
        count = Things.query.count()
        if count == 0:
            thing_list = {"@iot.count": count}
        elif expand_code != -1:
            thing_list = {
                "@iot.count": count,
                # "@iot.nextLink": f"{current_app.config['HOSTED_URL']}/Things{Things.get_nextlink_queryparams(top, skip, expand_code)}",
                "value": list(
                    map(
                        lambda x: Things.to_selected_json(x, selects),
                        Things.query.all(),
                    )
                ),
            }
        else:
            thing_list = {"error": "unrecognized expand options"}

        return thing_list

    @classmethod
    def add_item(cls, name, description):

        thing = Things(name=name, description=description)
        db.session.add(thing)
        db.session.commit()
        return {"created id" : thing.id}


    @classmethod
    def update_item(cls, id, name, description):
        try:
            thing = Things.query.filter(Things.id == id).first()
            if thing:
                thing.name =name
                thing.description = description
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
            thing = Things.query.filter(Things.id == id).first()
            if thing:
                db.session.delete(thing)
                db.session.commit()
                resp = {"deleted id": id}
            else:
                resp = {"message": "non existent id"}
        except Exception as e:
            logging.warning(e)
            resp = {"message": "error in delete"}

        return resp