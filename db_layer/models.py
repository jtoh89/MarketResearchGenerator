from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, String, Float, BigInteger, Date
import datetime

Base = declarative_base()

class Market_Trends(Base):
    __tablename__ = "Market_Trends"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Date = Column(String(50), unique=False)
    UnemploymentRate = Column(Float, unique=False)
    HomeValues = Column(Integer, unique=False)
    AverageRent = Column(Integer, unique=False)
    ShareofPriceCuts = Column(Float, unique=False)


class Market_Population_Trends(Base):
    __tablename__ = "Market_Population_Trends"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Year = Column(Integer, unique=False)
    Population = Column(Integer, unique=False)


class Market_Geo_ID_Lookup(Base):
    __tablename__ = "Market_Geo_ID_Lookup"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)


Market_Geo_ID_Lookup
class InitiateDeclaratives():
    @staticmethod
    def create_tables(engine_string):
        engine = create_engine(engine_string)
        Base.metadata.create_all(engine)


