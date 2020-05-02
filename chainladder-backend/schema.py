from graphene_sqlalchemy import SQLAlchemyConnectionField
import graphene
import schema_triangle


class Query(graphene.ObjectType):
    """Nodes which can be queried by this API."""
    node = graphene.relay.Node.Field()

    # Triangle
    triangle = graphene.relay.Node.Field(schema_triangle.Triangle)
    triangleList = SQLAlchemyConnectionField(schema_triangle.Triangle)


class Mutation(graphene.ObjectType):
    """Mutations which can be performed by this API."""
    # Triangle mutation
    loadDataset = schema_triangle.LoadDataset.Field()
    deleteTriangle = schema_triangle.DeleteTriangle.Field()
    grain = schema_triangle.Grain.Field()
    valToDev = schema_triangle.ValToDev.Field()
    devToVal = schema_triangle.DevToVal.Field()
    incrToCum = schema_triangle.IncrToCum.Field()
    cumToIncr = schema_triangle.CumToIncr.Field()
    latestDiagonal = schema_triangle.LatestDiagonal.Field()
    linkRatio = schema_triangle.LinkRatio.Field()
    trend = schema_triangle.Trend.Field()
    copy = schema_triangle.Copy.Field()
    groupBy = schema_triangle.GroupBy.Field()
    loc = schema_triangle.Loc.Field()
    dropNa = schema_triangle.DropNa.Field()

schema = graphene.Schema(query=Query, mutation=Mutation)
