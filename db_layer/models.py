from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, String, Float, BigInteger, Date
import datetime

Base = declarative_base()

class MarketTrends_HistoricalTrends(Base):
    __tablename__ = "MarketTrends_HistoricalTrends"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Date = Column(String(50), unique=False)
    UnemploymentRate = Column(Float, unique=False)
    HomeValues = Column(Integer, unique=False)
    AverageRent = Column(Integer, unique=False)
    ShareofPriceCuts = Column(Float, unique=False)


class MarketTrends_Population_Trends(Base):
    __tablename__ = "MarketTrends_Population_Trends"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Year = Column(Integer, unique=False)
    Population = Column(Integer, unique=False)


class MarketTrends_BuildingPermits(Base):
    __tablename__ = "MarketTrends_BuildingPermits"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Permit_1unit = Column(Integer, unique=False)
    Permit_2unit = Column(Integer, unique=False)
    Permit_3to4unit = Column(Integer, unique=False)
    Permit_5plus = Column(Integer, unique=False)
    Date = Column(String(15), unique=False)

class MarketTrends_Geo_ID_Lookup(Base):
    __tablename__ = "MarketTrends_Geo_ID_Lookup"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)


class InitiateDeclaratives():
    @staticmethod
    def create_tables(engine_string):
        engine = create_engine(engine_string)
        Base.metadata.create_all(engine)


