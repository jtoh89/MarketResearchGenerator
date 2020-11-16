import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r




def get_zillow_data():
    final_df = pd.DataFrame()

    sql = sql_caller.SqlCaller()
    zillow_msa_lookup = sql.db_get_Zillow_MSAID_Lookup()

    path = os.path.dirname(os.path.abspath(__file__))

    for filename in os.listdir(path):
        if 'Metro_' not in filename:
            continue

        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)

            for col in ['SizeRank','StateName','RegionType']:
                if col in df.columns.values:
                    df = df.drop(columns=[col])

            df = pd.melt(df, id_vars=['RegionID','RegionName']).rename(columns={'variable':'Date'})
            df['RegionID'] = df['RegionID'].apply(lambda x: str(x))

            df['year'] = df['Date'].map(lambda x: x[:4])
            df['month'] = df['Date'].map(lambda x: x[5:7])
            df['Date'] = df['month'] + '/01/' + df['year'].astype(str)

            if 'zhvi' in filename:
                df = df.rename(columns={'value':'HomeValues'})
            elif 'ZORI' in filename:
                df = df.rename(columns={'value':'AverageRent'})
            elif 'price_cut' in filename:
                df = df.rename(columns={'value':'ShareofPriceCuts'})

            combine_df = pd.merge(df, zillow_msa_lookup, how='inner', left_on=['RegionID'], right_on=['Zillow_Id']).drop(columns=['year','month','RegionID','Zillow_Id'])

            # if len(combine_df) != len(df):
            #     print('!!! MISMATCH IN ROWS - {}!!!'.format(filename))

            if final_df.empty:
                final_df = combine_df
            else:
                final_df = pd.merge(final_df, combine_df, how='inner', left_on=['Geo_ID','Date'], right_on=['Geo_ID','Date'])

    final_df = final_df.drop(columns=['RegionName_y'])

    msaids = get_msaids()
    common = pd.merge(final_df, msaids, how='inner', left_on=['Geo_ID'], right_on=['Geo_ID']).drop(columns=['Geo_Name'])
    missing = final_df[(~final_df.Geo_ID.isin(common.Geo_ID))]

    if len(common) != len(msaids):
        print('!!! Mismatch in zillow msaids !!!')

    return common.rename(columns={'RegionName_x':'Geo_Name'})



def get_unemployment_data():
    ############################################
    ######## NATIONAL UNEMPLOYMENT
    ############################################

    """
        Get xls from: https://fred.stlouisfed.org/series/UNRATE
    """
    path = os.path.dirname(os.path.abspath(__file__))
    path = path + '/UNRATE.xls'

    us_df = pd.read_excel(path, converters={'observation_date': str}, skiprows=10)

    us_df['year'] = us_df['observation_date'].map(lambda x: int(x[:4]))
    us_df['month'] = us_df['observation_date'].map(lambda x: x[5:7])
    us_df['Date'] = us_df['month'] + '/01/' + us_df['year'].astype(str)
    us_df['Geo_ID'] = '999'

    us_df = us_df.rename(columns={'UNRATE': 'UnemploymentRate'})

    us_df = us_df[['Geo_ID', 'UnemploymentRate', 'Date']]
    us_df.UnemploymentRate = us_df.UnemploymentRate.round(2)

    ############################################
    ######## METRO UNEMPLOYMENT
    ############################################

    urls = {
        'metro': 'https://download.bls.gov/pub/time.series/la/la.data.60.Metro'
    }

    # DEFINE CURRENT MONTH AND YEAR TO PULL. SET US UNEMPLOYMENT RATE
    final_df = pd.DataFrame()
    delimeter = '\t'

    # Parse Data
    for k, v in urls.items():
        data = r.get(v)

        row_list = []

        count = 0
        for line in data.text.splitlines():
            if count is 0:
                headers = [x.strip() for x in line.split(delimeter)]
                count += 1
            else:
                row = [x.strip() for x in line.split(delimeter, maxsplit=len(headers) - 1)]

                # make sure we only look at MSAs
                if int(row[1]) < 2012 or row[0][-2:] != '03' or row[0][2] != 'U':
                    continue

                if len(row) != len(headers):
                    print('There is a mismatch in columns and values')
                    print(row)
                    row.append('n/a')
                    row_list.append(row)
                else:
                    row_list.append(row)
                count += 1

        df = pd.DataFrame(row_list, columns=headers)

        df['Geo_ID'] = df['series_id'].str[7:12]

        if final_df.empty:
            final_df = df
        else:
            final_df = final_df.append(df)

    final_df['Date'] = final_df['period'].apply(lambda x: x.replace('M', '')) + '/01/' + final_df['year'].astype(str)
    final_df = final_df.rename(columns={'value': 'UnemploymentRate'})

    final_df = final_df[['Geo_ID', 'UnemploymentRate', 'Date']]

    master_df = final_df.append(us_df)


    msaids = get_msaids()
    common = pd.merge(master_df, msaids, how='inner', left_on=['Geo_ID'], right_on=['Geo_ID'])
    missing = master_df[(~master_df.Geo_ID.isin(common.Geo_ID))]

    return common

def get_arcgis_data():
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

    Esri_to_NECTAID_conversion = {
        '12620': '70750', '12700': '70900', '12740': '71050', '13540': '71350', '13620': '71500', '14460': '71650',
        '14860': '71950', '15540': '72400', '18180': '72700', '19430': '19380', '25540': '73450',
        '28300': '73750', '29060': '73900', '30100': '74350', '30340': '74650', '31700': '74950', '35300': '75700',
        '35980': '76450', '36837': '36860', '38340': '76600', '38860': '76750', '39150': '39140',
        '39300': '77200', '40860': '77650', '44140': '78100', '45860': '78400', '47240': '78500', '49060': '11680',
        '49340': '79600', '01': '999'}

    for k,v in Esri_to_NECTAID_conversion.items():
        if k in list(data['Geo_ID']):
            data.loc[data['Geo_ID'] == k, 'Geo_ID'] = Esri_to_NECTAID_conversion[k]

    df = pd.melt(data, id_vars=['Geo_ID', 'Geo_Name']).rename(columns={'variable': 'Year','value':'Population'})

    df = df.rename(columns=variables)

    return df



def get_msaids():

    path = os.path.dirname(os.path.abspath(__file__))
    msa_ids = pd.read_csv(path+'/arcgis_msa_names.csv')

    msa_ids['Geo_Name'] = msa_ids['Geo_Name'].apply(lambda x: x.replace(' Metropolitan Statistical Area',''))
    msa_ids['Geo_ID'] = msa_ids['Geo_ID'].apply(lambda x: str(x))

    Esri_to_NECTAID_conversion = {
        '12620': '70750', '12700': '70900', '12740': '71050', '13540': '71350', '13620': '71500', '14460': '71650',
        '14860': '71950', '15540': '72400', '18180': '72700', '19430': '19380', '25540': '73450',
        '28300': '73750', '29060': '73900', '30100': '74350', '30340': '74650', '31700': '74950', '35300': '75700',
        '35980': '76450', '36837': '36860', '38340': '76600', '38860': '76750', '39150': '39140',
        '39300': '77200', '40860': '77650', '44140': '78100', '45860': '78400', '47240': '78500', '49060': '11680',
        '49340': '79600'}

    for k,v in Esri_to_NECTAID_conversion.items():
        if k in list(msa_ids['Geo_ID']):
            msa_ids.loc[msa_ids['Geo_ID'] == k, 'Geo_ID'] = Esri_to_NECTAID_conversion[k]

    return msa_ids

#
# def get_redfin_data(arcgis_msa_lookup):
#     path = os.path.dirname(os.path.abspath(__file__))
#
#     final_df = pd.DataFrame()
#     with open(os.path.join(path, 'redfin_data.csv')) as file:
#
#     #   If Read_CSV fails, make sure file is converted to actual csv, not text.
#         df = pd.read_csv(file, converters={'Median Sale Price':str,'Table Id':str})
#
#         df = df[['Table Id',
#                  'Region',
#                  'Period Begin',
#                  'Median Sale Price',
#                  'Inventory',
#                  'Price Drops'
#                  ]]
#
#         df = df.rename(columns={'Table Id': 'Geo_ID',
#                                 'Period Begin':'Date',
#                                 'Median Sale Price':'MedianSalePrice'})
#
#         try:
#             df['month'] = df['Date'].apply(lambda x:datetime.datetime.strptime(x,'%m/%d/%y').month)
#             df['year'] = df['Date'].apply(lambda x:datetime.datetime.strptime(x,'%m/%d/%y').year)
#         except:
#             df['month'] = df['Date'].apply(lambda x:datetime.datetime.strptime(x,'%m/%d/%Y').month)
#             df['year'] = df['Date'].apply(lambda x:datetime.datetime.strptime(x,'%m/%d/%Y').year)
#
#
#     #   Reassign MSA_ID: USA, Chicago, Dallas
#         df['Geo_ID'] = df['Geo_ID'].map(lambda x: x.replace('1400', '999').replace('16984', '16980').replace('19124', '19100'))
#         df['MedianSalePrice'] = df['MedianSalePrice'].map(lambda x: x.replace(',','').replace('$','').replace('K','000')).apply(pd.to_numeric)
#
#         if final_df.empty:
#             final_df = df
#         else:
#             final_df = final_df.append(df)
#
#     final_df['Date'] = final_df['month'].astype(str).apply(lambda x: x.zfill(2)) + '/01/' + final_df['year'].astype(str)
#
#     return final_df