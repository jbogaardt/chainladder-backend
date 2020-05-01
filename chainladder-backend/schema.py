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
    createTriangle = schema_triangle.CreateTriangle.Field()
    deleteTriangle = schema_triangle.DeleteTriangle.Field()
    methodChain = schema_triangle.MethodChain.Field()
    grain = schema_triangle.Grain.Field()

schema = graphene.Schema(query=Query, mutation=Mutation)
