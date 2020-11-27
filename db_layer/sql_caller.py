from sqlalchemy import create_engine
from db_layer import models
import pandas as pd
import json
import os
class SqlCaller():
    """
    SqlCaller() --> This module is meant to dump various data into a database.
    Must instantiate SqlCaller() with census api key and db connection string

    Parameters:
    engine_string: define db connection here
    api_key: define api_key for census data. You can go to www.census.gov.
    """

    def __init__(self, create_tables=False):
        connectagain = False
        try:
            with open("./un_pw.json", "r") as file:
                mysql_engine = json.load(file)['aws_mysql']
        except:
            connectagain = True

        if connectagain:
            with open("../un_pw.json", "r") as file:
                mysql_engine = json.load(file)['aws_mysql']

        engine_string = mysql_engine
        self.engine = create_engine(engine_string)

        if create_tables == True:
            print("Creating tables")
            models.InitiateDeclaratives.create_tables(engine_string)


    def db_dump_Market_Geo_ID_Lookup(self, df):
        df.to_sql("MarketTrends_Geo_ID_Lookup", if_exists='replace', con=self.engine, index=False)

    def db_dump_MarketTrends_HistoricalTrends(self, df):
        df.to_sql("MarketTrends_HistoricalTrends", if_exists='replace', con=self.engine, index=False)


    def db_dump_MarketTrends_BuildingPermits(self, df):
        df.to_sql("MarketTrends_BuildingPermits", if_exists='replace', con=self.engine, index=False)

    def db_get_MarketTrends_BuildingPermits(self):
        arcgis_population = pd.read_sql_query("""select * from MarketTrends_BuildingPermits""", self.engine)
        return arcgis_population


    def db_dump_MarketTrends_Population(self, df):
        df.to_sql("MarketTrends_Population_Trends", if_exists='replace', con=self.engine, index=False)

    def db_get_MarketTrends_Population(self):
        arcgis_population = pd.read_sql_query("""select * from MarketTrends_Population_Trends""", self.engine)
        return arcgis_population


    def db_get_Zillow_MSAID_Lookup(self):
        msa_ids = pd.read_sql_query("""select Geo_ID, Zillow_Id from Zillow_MSAID_Lookup""", self.engine)
        return msa_ids
