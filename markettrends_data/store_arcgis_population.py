import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r



variables = {
    'TSPOP10_CY': '2010',
    'TSPOP11_CY': '2011',
    'TSPOP12_CY': '2012',
    'TSPOP13_CY': '2013',
    'TSPOP14_CY': '2014',
    'TSPOP15_CY': '2015',
    'TSPOP16_CY': '2016',
    'TSPOP17_CY': '2017',
    'TSPOP18_CY': '2018',
    'TSPOP19_CY': '2019',
    'TOTPOP_CY': '2020',
    # 'ACSUNT1DET_P': 'Single Family (detached)',
    # 'ACSUNT1ATT_P': 'Single Family (attached)',
    # 'ACSUNT2_P': 'Duplex',
    # 'ACSUNT3_P': 'Triplex and Fourplex',
    # 'ACSUNT5_P': '5 to 9 Units',
    # 'ACSUNT10_P': '10 to 19 Units',
    # 'ACSUNT20_P': '20 to 49 Units',
    # 'ACSUNT50UP_P': '50+ Units',
    # 'ACSUNTMOB_P': 'Mobile Homes',
}

path = os.path.dirname(os.path.abspath(__file__))
msa_ids = pd.read_csv(path+'/arcgis_msa_names.csv')['Geo_ID'].apply(lambda x: str(x))


gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')

data = enrich(study_areas=[{"sourceCountry": "US", "layer": "US.CBSA", "ids": list(msa_ids)}],
              analysis_variables=list(variables.keys()),
              comparison_levels=['US.WholeUSA'],
              return_geometry=False)

data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'aggregationMethod', 'populationToPolygonSizeRating',
             'HasData', 'sourceCountry', 'StdGeographyLevel'])
data = data.rename(columns={'StdGeographyID': 'Geo_ID', 'StdGeographyName': 'Geo_Name'})
data = data.rename(columns=variables)
data['Geo_ID'] = data['Geo_ID'].apply(lambda x: x.replace('102001','99999'))

df = pd.melt(data, id_vars=['Geo_ID', 'Geo_Name']).rename(columns={'variable': 'Year','value':'Population'})

df = df.rename(columns=variables)

sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_MarketTrends_Population(df)


