from arcgis.gis import GIS
from arcgis.geoenrichment import enrich
from data.arcgismacrovariables import variables
import json
import pandas as pd
from sqlalchemy import create_engine
import math

def get_arcgisdata(msaid):
    gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')

    ##   CALL GEOENRICH API and DROP USELESS FIELDS
    print('Calling GeoEnrichment API')
    data = enrich(study_areas=[{"sourceCountry": "US", "layer": "US.CBSA", "ids": [msaid]}],
                  analysis_variables=list(variables.keys()),
                  # comparison_levels=['US.WholeUSA'],
                  return_geometry=False)

    data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID', 'aggregationMethod',
                              'populationToPolygonSizeRating', 'HasData', 'sourceCountry'])

    data = data.rename(
        columns={'StdGeographyLevel': 'GeoType', 'StdGeographyID': 'Geo_ID', 'StdGeographyName': 'Market'})

    data = data.rename(columns={'Geo_ID': 'MSA_ID'}).drop(columns=['GeoType'])

    data['OwnerOccupancyRate'] = round(data['OWNER_CY'] / data['TOTHU_CY'] * 100, 2)
    data['RenterOccupancyRate'] = round(data['RENTER_CY'] / data['TOTHU_CY'] * 100, 2)
    data['VacancyRate'] = round(data['VACANT_CY'] / data['TOTHU_CY'] * 100, 2)
    data = data.drop(columns=['OWNER_CY', 'RENTER_CY', 'RENTER_CY', 'VACANT_CY'])
    data = data.rename(columns=variables)

    with open("./un_pw.json", "r") as file:
        aws_string = json.load(file)['aws_mysql']



    bls_unemployment_multiplier = pd.read_sql_query("""
                                                    select Geo_Type, Unemployment_multiplier
                                                    from ESRI_Unemployment_Multiplier 
                                                    where (Geo_ID = {} and Geo_Type =  'US.CBSA' )
                                                    """.format(msaid), create_engine(aws_string))

    multiplier = bls_unemployment_multiplier['Unemployment_multiplier'][0]

    data['Unemployment Rate'] = data['Unemployment Rate'].apply(lambda x: x * multiplier).apply(lambda x: math.floor(x * 10 ** 1) / 10 ** 1).apply(lambda x: str(x) + '%')

    data['Median Household Income'] = data['Median Household Income'].apply(lambda x: '${:,.0f}'.format(x))
    data['OwnerOccupancyRate'] = data['OwnerOccupancyRate'].apply(lambda x: str(x) + '%')
    data['RenterOccupancyRate'] = data['RenterOccupancyRate'].apply(lambda x: str(x) + '%')
    data['VacancyRate'] = data['VacancyRate'].apply(lambda x: str(x) + '%')
    data['2010-2020 Average Annual Population Growth'] = data['2010-2020 Average Annual Population Growth'].apply(
        lambda x: str(x) + '%')

    return data