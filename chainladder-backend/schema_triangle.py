from datetime import datetime
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphql_relay import from_global_id
from database.base import db_session
from database.model_triangle import ModelTriangle
import graphene
import json
import utils
import chainladder as cl
import sys
import copy
import docstring_parser

class DocString:
    def __init__(self, func):
        self.doc = docstring_parser.parse(func.__doc__)
        self.description = self.doc.short_description + (self.doc.long_description if self.doc.long_description else '')
        self.arg_names = [item.arg_name for item in self.doc.params]

    def params(self, param):
        idx = self.arg_names.index(param)
        return self.doc.params[idx].description


def upsert(name, triangle):
    exists = db_session.query(ModelTriangle).filter_by(name=name)
    if exists.first():
        exists.update({'edited': datetime.utcnow(), 'data': triangle.to_json()})
    else:
        data = {'name': name,
                'data': triangle.to_json(),
                'created': datetime.utcnow(),
                'edited': datetime.utcnow()}
        db_session.add(ModelTriangle(**data))
    db_session.commit()
    return db_session.query(ModelTriangle).filter_by(name=name).first()

class SerializeTriangle(object):
    """ Serialize Triangle from JSON into its cl.Triangle form """
    def resolve(self, next, root, info, **args):
        if root and hasattr(root, 'data'):
            root.triangle = cl.read_json(root.data)
        return next(root, info, **args)

class Grain(graphene.Mutation):
    triangle = graphene.Field(lambda: Triangle, description="Triangle updated by this mutation.")
    class Meta:
        description = DocString(cl.Triangle.grain).description
    class Arguments:
        name = graphene.String(required=True, description="Name of Triangle.")
        grain = graphene.String(description=DocString(cl.Triangle.grain).params('grain'))
        trailing = graphene.Boolean(description=DocString(cl.Triangle.grain).params('trailing'))

    def mutate(self, info, name, grain, **kwargs):
        triangle = db_session.query(ModelTriangle).filter_by(name=name)
        updated_tri = cl.read_json(triangle.first().data)
        updated_tri.grain(grain, trailing=kwargs.get('trailing', None))
        if kwargs.get('assign_to'):
            return upsert(assign_to, updated_triangle)
        triangle.update({'edited': datetime.utcnow(),
                         'data': updated_tri.to_json()})
        return Grain(triangle=triangle.first())

class Triangle(SQLAlchemyObjectType):
    """Triangle node."""

    class Meta:
        model = ModelTriangle
        interfaces = (graphene.relay.Node,)

    shape = graphene.List(graphene.Int)
    origin_grain = graphene.String()
    development_grain = graphene.String()
    development = graphene.List(graphene.String)
    origin = graphene.List(graphene.String)
    is_val_tri = graphene.Boolean()
    key_labels = graphene.String()
    nan_override = graphene.Boolean()
    is_cumulative = graphene.Boolean()
    valuation_date = graphene.types.datetime.Date()
    columns = graphene.List(graphene.String)
    values = graphene.List(graphene.List(graphene.List(graphene.List(graphene.String))))
    index = graphene.List(graphene.List(graphene.String))

    def resolve_shape(root, info):
        return root.triangle.shape

    def resolve_origin_grain(root, info):
        return root.triangle.origin_grain

    def resolve_development_grain(root, info):
        return root.triangle.origin_grain

    def resolve_development(root, info):
        return root.triangle.ddims

    def resolve_origin(root, info):
        return list(root.triangle.origin.astype(str))

    def resolve_is_val_tri(root, info):
        return root.triangle.is_val_tri

    def resolve_key_labels(root, info):
        return root.triangle.key_labels

    def resolve_nan_override(root, info):
        return root.triangle.nan_override

    def resolve_is_cumulative(root, info):
        return root.triangle.is_cumulative

    def resolve_valuation_date(root, info):
        return root.triangle.valuation_date

    def resolve_values(root, info):
        return root.triangle.values.tolist()

    def resolve_columns(root, info):
        return list(root.triangle.vdims)

    def resolve_index(root, info):
        return root.triangle.kdims.tolist()


class CreateTriangle(graphene.Mutation):
    """Mutation to create a triangle."""
    triangle = graphene.Field(lambda: Triangle, description="Triangle created by this mutation.")

    class Arguments:
        name = graphene.String()

    def mutate(self, info, name, **kwargs):
        return CreateTriangle(triangle=upsert(name, cl.load_dataset(name)))


class DeleteTriangle(graphene.Mutation):
    """Mutation to create a triangle."""
    ok = graphene.String(description="Delete Triangle")

    class Arguments:
        name = graphene.String()

    def mutate(self, info, name):
        db_session.query(ModelTriangle).filter_by(name=name).delete()
        db_session.commit()
        return DeleteTriangle(ok='ok')


class MethodChain(graphene.Mutation):
    """Update a triangle."""
    triangle = graphene.Field(lambda: Triangle, description="Triangle updated by this mutation.")

    class Arguments:
        name = graphene.String(required=True, description="Name of Triangle.")
        assign_to = graphene.String(description="Where to assign results.")
        chain = graphene.List(graphene.List(graphene.String), required=True, description="the grain")

    def mutate(self, info, name, chain, assign_to=None):
        triangle = db_session.query(ModelTriangle).filter_by(name=name)
        updated_tri = cl.read_json(triangle.first().data)
        operators = ['__eq__', '__ne__', '__gt__', '__ge__', '__lt__', '__le__']
        for item in chain:
            func = item[0]
            if len(item) == 1:
                if func in properties_as_methods:
                    updated_tri = getattr(updated_tri, func)
                else:
                    updated_tri = getattr(updated_tri, func)()
            else:
                func, args = item[0], item[1]
                if args[0] in ('{', '['):
                    args =  json.loads(args.replace('\'','"'))
                if type(args) is str:
                    updated_tri = getattr(updated_tri, func)(args)
                if type(args) is list:
                    if func == '__getitem__':
                        if args[1] in operators:
                            if args[0] in ['origin', 'development', 'valuation']:
                                updated_tri = updated_tri[getattr(getattr(updated_tri, args[0]), args[1])(args[2])]
                            else:
                                updated_tri = updated_tri[getattr(updated_tri[args[0]], args[1])(args[2])]
                        else:
                            updated_tri = getattr(updated_tri, func)(args)
                    else:
                        updated_tri = getattr(updated_tri, func)(*args)
                if type(args) is dict:
                    updated_tri = getattr(updated_tri, func)(**args)



        triangle.update({'edited': datetime.utcnow(),
                         'data': updated_tri.to_json()})
        if assign_to:
            triangle.update({'name':assign_to})
            exists = db_session.query(ModelTriangle).filter_by(name=assign_to)
            if exists:
                exists.delete()
                triangle.update({'edited': datetime.utcnow()})
            else:
                triangle.update({'name': assign_to, 'created': datetime.utcnow()})


            data['data'] = updated_tri.to_json()
            data['created'] = datetime.utcnow()
            data['edited'] = datetime.utcnow()
            triangle = ModelTriangle(**data)
            db_session.add(triangle)
            db_session.commit()

            triangle = ModelTriangle(**data)
            db_session.add(triangle)

            db_session.add(triangle.first())
            db_session.commit()
            #db_session.commit()
            #triangle = db_session.query(ModelTriangle).filter_by(id=database_id).first()
        return MethodChain(triangle=triangle.first())
