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

            df = df[df['year'].astype(int) >= 2014]

            if 'zhvi' in filename:
                df = df.rename(columns={'value':'HomeValues'})
            elif 'ZORI' in filename:
                df = df.rename(columns={'value':'AverageRent'})

            combine_df = pd.merge(df, zillow_msa_lookup, how='inner', left_on=['RegionID'], right_on=['Zillow_Id']).drop(columns=['year','month','RegionID','Zillow_Id'])

            # if len(combine_df) != len(df):
            #     print('!!! MISMATCH IN ROWS - {}!!!'.format(filename))

            if final_df.empty:
                final_df = combine_df
            else:
                final_df = pd.merge(final_df, combine_df, how='left', left_on=['Geo_ID','Date'], right_on=['Geo_ID','Date'])

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
    us_df['Geo_ID'] = '99999'

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

    MSA_to_CBSA_conversion = {
        '70750': '12620', '70900': '12700', '71050': '12740', '71350': '13540', '71500': '13620', '71650': '14460',
        '71950': '14860', '72400': '15540', '72700': '18180',
        '19380': '19430', '73450': '25540', '73750': '28300', '73900': '29060', '74350': '30100', '74650': '30340',
        '74950': '31700', '75700': '35300', '76450': '35980',
        '36860': '36837', '76600': '38340', '76750': '38860', '39140': '39150', '77200': '39300', '77650': '40860',
        '78100': '44140', '78400': '45860', '78500': '47240',
        '11680': '49060', '79600': '49340'}

    for k, v in MSA_to_CBSA_conversion.items():
        if k in list(master_df['Geo_ID']):
            master_df.loc[master_df['Geo_ID'] == k, 'Geo_ID'] = v

    msaids = get_msaids()
    common = pd.merge(master_df, msaids, how='inner', left_on=['Geo_ID'], right_on=['Geo_ID'])
    missing = master_df[(~master_df.Geo_ID.isin(common.Geo_ID))]

    return common


def get_msaids():

    path = os.path.dirname(os.path.abspath(__file__))
    msa_ids = pd.read_csv(path+'/arcgis_msa_names.csv')

    msa_ids['Geo_Name'] = msa_ids['Geo_Name'].apply(lambda x: x.replace(' Metropolitan Statistical Area',''))
    msa_ids['Geo_ID'] = msa_ids['Geo_ID'].apply(lambda x: str(x))

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