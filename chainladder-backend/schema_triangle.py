from datetime import datetime
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphql_relay import from_global_id
from database.base import db_session
from database.model_triangle import ModelTriangle
import graphene
import json
import chainladder as cl
import sys
import docstring_parser

class DocString:
    def __init__(self, func):
        self.doc = docstring_parser.parse(func.__doc__)
        self.description = self.doc.short_description + \
            (self.doc.long_description if self.doc.long_description else '')
        self.arg_names = [item.arg_name for item in self.doc.params]

    def params(self, param):
        idx = self.arg_names.index(param)
        return self.doc.params[idx].description


def upsert(name, triangle):
    """ Handles upsert funtionality of mutations

    Parameters
    ----------
    name :
        The name of the Triangle
    triangle:
        The triangle instance
    """
    exists = db_session.query(ModelTriangle).filter_by(name=name)
    if exists.first():
        exists.update({'edited': datetime.utcnow(),
                       'data': triangle.to_json()})
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


def create_class(klass, transform, **kwargs):
    """ Graphene inheritance sucks.  This is a helper function to get around
    its limitations.  This function is intended for methods that return self,
    i.e. those methods of a class that can be method-chained.

    Parameters
    ----------
    klass :
        The name of the Mutation class you'd like to build
    transform :
        A function with signature (triangle : cl.Triangle, **kwargs) that must
        be written to define the class mutation.
    kwargs :
        The keyword arguments available to the transform function.
    """
    kwargs = kwargs or {}
    AbstractTriangleMutation = type(
        'AbstractTriangleMutation', (graphene.ObjectType, ),
        {'triangle':graphene.Field(lambda: Triangle, description="Triangle updated by this mutation."),
         'Arguments': type('Arguments', (object, ), {**{
            'name': graphene.String(required=True, description="Name of triangle to mutate."),
            'assign_to': graphene.String()}, **kwargs}),
         'transform': staticmethod(transform)})
    class CreateClass(graphene.Mutation, AbstractTriangleMutation):
        def mutate(self, info, name, **kwargs):
            triangle = db_session.query(ModelTriangle).filter_by(name=name)
            updated_tri = AbstractTriangleMutation.transform(
                cl.read_json(triangle.first().data), **kwargs)
            assign_to = kwargs.get('assign_to', None)
            if assign_to:
                return AbstractTriangleMutation(triangle=upsert(assign_to, updated_tri))
            else:
                triangle.update({'data': updated_tri.to_json()})
                return AbstractTriangleMutation(triangle=triangle.first())
    return type(klass, (CreateClass,), {})

def transform(triangle, **kwargs):
    return triangle.dev_to_val()
DevToVal = create_class('DevToVal', transform=transform)

def transform(triangle, **kwargs):
    return triangle.val_to_dev()
ValToDev = create_class('ValToDev', transform=transform)

def transform(triangle, **kwargs):
    return triangle.latest_diagonal
LatestDiagonal = create_class('LatestDiagonal', transform=transform)

def transform(triangle, **kwargs):
    return triangle.link_ratio
LinkRatio = create_class('LinkRatio', transform=transform)

def transform(triangle, **kwargs):
    return triangle.incr_to_cum()
IncrToCum = create_class('IncrToCum', transform=transform)

def transform(triangle, **kwargs):
    return triangle.cum_to_incr()
CumToIncr = create_class('CumToIncr', transform=transform)

def transform(triangle, **kwargs):
    kw = ['grain', 'trailing']
    kwargs = {k: v for k, v in kwargs.items() if k in kw}
    return triangle.grain(**kwargs)
Grain = create_class(
    'Grain', transform=transform,
    grain = graphene.String(description=DocString(cl.Triangle.grain).params('grain')),
    trailing = graphene.Boolean(description=DocString(cl.Triangle.grain).params('trailing')))

class Aggregates(graphene.Enum):
    SUM = "sum"
    MEAN = "mean"
    STD = "std"
    VAR = "var"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    PROD = "prod"
    CUMSUM = "cumsum"

def transform(triangle, **kwargs):
    kw = ['by', 'aggregation']
    kwargs = {k: v for k, v in kwargs.items() if k in kw}
    return getattr(triangle.groupby(kwargs.get('by')), kwargs.get('aggregation'))()
GroupBy = create_class(
    'GroupBy', transform=transform,
    by = graphene.List(graphene.String, description=DocString(cl.Triangle.groupby).params('by')),
    aggregation = Aggregates(description="Aggregation method"))


class BoolOperators(graphene.Enum):
    GT = '__gt__'
    LT = '__lt__'
    GE = '__ge__'
    LE = '__le__'
    EQ = '__eq__'
    NE = '__ne__'
class IndexFilter(graphene.InputObjectType):
    key = graphene.String(required=True, description='Index column to be filtered')
    operator = BoolOperators(required=True)
    value = graphene.String(required=True)
class OtherFilter(graphene.InputObjectType):
    operator = BoolOperators(required=True)
    value = graphene.String(required=True)
def transform(triangle, **kwargs):
    if kwargs.get('index'):
        triangle = triangle.loc[kwargs.get('index')]
    if kwargs.get('where_index'):
        where = kwargs.get('where_index')
        triangle = triangle[getattr(triangle[where.key], where.operator)(where.value)]
    if kwargs.get('columns'):
        triangle = triangle[kwargs.get('columns')]
    if kwargs.get('where_origin'):
        where = kwargs.get('where_origin')
        triangle = triangle[getattr(triangle.origin, where.operator)(where.value)]
    if kwargs.get('where_development'):
        where = kwargs.get('where_development')
        triangle = triangle[getattr(triangle.development, where.operator)(where.value)]
    if kwargs.get('where_valuation'):
        where = kwargs.get('where_valuation')
        triangle = triangle[getattr(triangle.valuation, where.operator)(where.value)]
    return triangle
Loc = create_class(
    'Loc', transform=transform,
    index = graphene.List(graphene.String),
    columns = graphene.List(graphene.String),
    where_index = IndexFilter(description="Filter for Index"),
    where_origin = OtherFilter(description="Optional filter for origin period"),
    where_development = OtherFilter(description="Optional filter for development period"),
    where_valuation = OtherFilter(description="Optional filter for valuation period"),
    )



def transform(triangle, **kwargs):
    kw = ['trend', 'axis', 'valuation_date', 'ultimate_lag']
    kwargs = {k: v for k, v in kwargs.items() if k in kw}
    return triangle.trend(**kwargs)
Trend = create_class(
    'Trend', transform=transform,
    trend = graphene.Float(description=DocString(cl.Triangle.trend).params('trend')),
    axis = graphene.String(description=DocString(cl.Triangle.trend).params('axis')),
    valuation_date = graphene.Date(description=DocString(cl.Triangle.trend).params('valuation_date')),
    ultimate_lag = graphene.Int(description=DocString(cl.Triangle.trend).params('ultimate_lag')))

def transform(triangle, **kwargs):
    return triangle
Copy = create_class('Copy', transform=transform)

def transform(triangle, **kwargs):
    return triangle.dropna()
DropNa = create_class('DropNa', transform=transform)


class Triangle(SQLAlchemyObjectType):
    """Triangle node."""

    class Meta:
        model = ModelTriangle
        interfaces = (graphene.relay.Node,)

    shape = graphene.List(graphene.Int, description=DocString(cl.Triangle).params('shape'))
    origin_grain = graphene.String(description=DocString(cl.Triangle).params('origin_grain'))
    development_grain = graphene.String(description=DocString(cl.Triangle).params('development_grain'))
    development = graphene.List(graphene.String, description=DocString(cl.Triangle).params('development'))
    origin = graphene.List(graphene.String,description=DocString(cl.Triangle).params('origin'))
    is_val_tri = graphene.Boolean(description=DocString(cl.Triangle).params('is_val_tri'))
    key_labels = graphene.String(description=DocString(cl.Triangle).params('key_labels'))
    is_full = graphene.Boolean(description=DocString(cl.Triangle).params('is_full'))
    is_ultimate = graphene.Boolean(description=DocString(cl.Triangle).params('is_ultimate'))
    is_cumulative = graphene.Boolean(description=DocString(cl.Triangle).params('is_cumulative'))
    valuation_date = graphene.types.datetime.Date(description=DocString(cl.Triangle).params('valuation_date'))
    columns = graphene.List(graphene.String,description=DocString(cl.Triangle).params('columns'))
    values = graphene.List(graphene.List(graphene.List(graphene.List(graphene.String))), description=DocString(cl.Triangle).params('values'))
    index = graphene.List(graphene.List(graphene.String), description=DocString(cl.Triangle).params('index'))

    def resolve_is_ultimate(root, info):
        return root.triangle.is_ultimate

    def resolve_is_full(root, info):
        return root.triangle.is_full

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


class LoadDataset(graphene.Mutation):
    """Mutation to create a triangle."""
    triangle = graphene.Field(lambda: Triangle, description="Triangle created by this mutation.")

    class Arguments:
        name = graphene.String()

    def mutate(self, info, name, **kwargs):
        return LoadDataset(triangle=upsert(name, cl.load_dataset(name)))


class DeleteTriangle(graphene.Mutation):
    """Mutation to create a triangle."""
    ok = graphene.String(description="Delete Triangle")

    class Arguments:
        name = graphene.String()

    def mutate(self, info, name):
        db_session.query(ModelTriangle).filter_by(name=name).delete()
        db_session.commit()
        return DeleteTriangle(ok='ok')
