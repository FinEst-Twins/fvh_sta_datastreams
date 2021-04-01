from app import db


class Things(db.Model):
    __tablename__ = "thing"
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
            FoI_list = Things.query.filter(Things.id == id)

        if FoI_list.count() == 0:
            result = {"message":"No Things found with given Id"}
        else:
            result = {f"Thing {id}": Things.to_json(FoI_list[0])}

        return result

    @classmethod
    def return_all(cls):
        return {
            "Things": list(
                map(lambda x: Things.to_json(x), Things.query.all())
            )
        }
