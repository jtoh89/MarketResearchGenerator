import pandas as pd
import re
import os
import datetime
import numpy
import chardet
import math

#   Redfin Economic Data
def get_macrotrends(msaid, demographic_df):
    path = os.path.dirname(os.path.abspath(__file__))

#   Reassign MSA_ID: USA, Chicago, Dallas, Los Angeles, San Francisco
    msaid = msaid.replace('16980','16984').replace('19100','19124').replace('31080','31084').replace('41860','41884')

    with open(os.path.join(path, 'redfindata_KC.csv')) as file:
        usaid = '1400'

        #save file as csv if utf-8 erro
        df = pd.read_csv(file)
        final_df = pd.DataFrame()

        df = df[['Table Id','Region Type','Period Begin','parent_metro_region',
                 'Median Sale Price','Median Sale Price Mom','Median Sale Price Yoy',
                 'Median Dom','Median Dom Mom', 'Median Dom Yoy',
                 'months_of_supply','months_of_supply_mom','months_of_supply_yoy',
                 'New Listings','New Listings Mom','New Listings Yoy',
                 'Price Drops','Price Drops Mom','Price Drops Yoy']]

        df = df.rename(columns={'Table Id': 'MSA_ID', 'parent_metro_region':'RegionName','Period Begin':'Date'})
        df = df.loc[df['MSA_ID'].isin([msaid,usaid])]
        marketname = df[df['MSA_ID'] == int(msaid)]['RegionName'].iloc[0]
        # df['MSA_ID'] = df['MSA_ID'].apply(lambda x: str(x))
        df['Median Sale Price'] = df['Median Sale Price'].apply(lambda x: x.replace('$','').replace('K','000'))

        df['RegionName'] = df['RegionName'].fillna('United States')

        # df['MSA_ID'] = df['MSA_ID'].map(lambda x: x.replace('16984','16980').replace('19124','19100').replace('31084','31080').replace('41884','41860'))

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by=['Date'], ascending=False)

        #   Reassign MSA_ID: USA, Chicago, Dallas, Los Angeles, San Francisco
        # msaid = msaid.replace('16980', '16984').replace('19100', '19124').replace('31080', '31084').replace('41860','41884')
        us_df = df[df['MSA_ID'] == 1400]
        df = df[df['MSA_ID'] == int(msaid)]
        msa_df = df

        msa_df = msa_df.sort_values(by=['Date'], ascending=False)
        msa_df = msa_df.reset_index()
        msa_df = msa_df[msa_df['Date'].isin([msa_df['Date'][0], msa_df['Date'][1],msa_df['Date'][12]])]

        for i, row in msa_df.iterrows():
            if i ==0:
                if row['Price Drops Yoy'] > 0:
                    msa_df.at[i,'Pricedrops_YearChange'] = str(math.floor((row['Price Drops Yoy']*100) * 10 ** 2) / 10 ** 2) + '%'
                else:
                    msa_df.at[i,'Pricedrops_YearChange'] = str(math.floor((row['Price Drops Yoy']*100) * 10 ** 3) / 10 ** 3)[:-1] + '%'

                if row['Price Drops Mom'] > 0:
                    msa_df.at[i,'Pricedrops_MonthChange'] = str(math.floor((row['Price Drops Mom']*100) * 10 ** 2) / 10 ** 2) + '%'
                else:
                    msa_df.at[i,'Pricedrops_MonthChange'] = str(math.floor((row['Price Drops Mom']*100) * 10 ** 3) / 10 ** 3)[:-1] + '%'



                msa_df.at[i,'NewListings_YearChange'] = row['New Listings Yoy']
                msa_df.at[i,'NewListings_MonthChange'] = row['New Listings Mom']
                msa_df.at[i,'MedianPrice_YearChange'] = row['Median Sale Price Yoy']
                msa_df.at[i,'MedianPrice_MonthChange'] = row['Median Sale Price Mom']

                msa_df.at[i, 'DOM_YearChange'] = row['Median Dom Yoy']
                msa_df.at[i, 'DOM_MonthChange'] = row['Median Dom Mom']
                msa_df.at[i,'Mos_YearChange'] = row['months_of_supply_yoy']
                msa_df.at[i,'Mos_MonthChange'] = row['months_of_supply_mom']

        msa_df = msa_df.drop(columns=['months_of_supply_mom','months_of_supply_yoy','Price Drops Mom','Price Drops Yoy',
                                      'Median Sale Price Mom','Median Sale Price Yoy','New Listings Mom','New Listings Yoy',
                                      'Median Dom Mom','Median Dom Yoy'])

        us_df = us_df.rename(columns={'Median Sale Price':'us_mediansaleprice','months_of_supply':'us_mosupply'})\
            .drop(columns=['Region Type','MSA_ID','RegionName'])
        df = df.rename(columns={'Median Sale Price':'market_mediansaleprice','months_of_supply':'market_mosupply'})\
            .drop(columns=['Region Type','MSA_ID','RegionName'])

        df = df.merge(us_df, on=['Date', 'Date'])


        msa_df[['months_of_supply','New Listings']] = msa_df[['months_of_supply','New Listings']].apply(lambda x: round(x,2))
        msa_df['Price Drops'] = msa_df['Price Drops'].apply(lambda x: round(x * 100,2))
        msa_df['Price Drops'] = msa_df['Price Drops'].apply(lambda x: str(x) + '%')
        df[['market_mosupply', 'us_mosupply']] = df[['market_mosupply', 'us_mosupply']].apply(lambda x: round(x,2))



        with pd.ExcelWriter("testdata/datamacrodata.xlsx") as writer:
            demographic_df.to_excel(writer, sheet_name='demographics',index=False)
            msa_df.to_excel(writer, sheet_name='marketchanges',index=False)
            df[['Date','market_mediansaleprice','us_mediansaleprice']].rename(columns={'market_mediansaleprice':marketname,'us_mediansaleprice':'United States'}).to_excel(writer, sheet_name='mediansales',index=False)
            df[['Date', 'market_mosupply', 'us_mosupply']].rename(columns={'market_mosupply': marketname, 'us_mosupply': 'United States'}).to_excel(writer, sheet_name='monthsofsupply', index=False)



