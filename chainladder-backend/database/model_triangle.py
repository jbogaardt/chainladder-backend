from .base import Base
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean


class ModelTriangle(Base):
    """Triangle model."""

    __tablename__ = 'triangle'

    id = Column('id', Integer, primary_key=True, doc="Id of the Triangle.")
    name = Column('name', String, doc="Name of the Triangle.")
    data = Column('data', String, doc="Data of the triangle")
    created = Column('created', String, doc="Record created date.")
    edited = Column('edited', String, doc="Record last updated date.")
