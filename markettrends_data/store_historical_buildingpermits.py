import pandas as pd
import requests
from db_layer import sql_caller

final_df = pd.DataFrame()
aggregate_df = pd.DataFrame(columns=['Units_1-unit_rep','Units_2-units_rep','Units_3-4_units_rep','Units_5+_units_rep'])
years = [
    # These values use different MSAIDs 2006, 2007, 2008, 2009,2010, 2011, 2012, 2013,
         2014, 2015, 2016, 2017, 2018, 2019, 2020]

for year in years:
    skip = False
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']

    for month in months:
        url = 'https://www2.census.gov/econ/bps/Metro/ma{}{}c.txt'.format(str(year)[2:], month)

        request_tries = 0
        while request_tries < 3:
            try:
                data = requests.get(url)
                request_tries = 3

                if data.status_code != 200:
                    request_tries = 3
                    skip = True
                    print('Skipped request for: {}'.format(url))
                    continue

                print('Successful request for: {}'.format(url))

            except Exception as e:
                request_tries += 1
                print('Following error occured: {} for Year {}'.format(e,year))

                if request_tries == 2:
                    print('Could not retrieve building permits for Year {}'.format(year))
                    year += 1
                    skip = True
                    continue

        if not skip:
            counter = 1
            list = []

            data = data.text.replace('     ','')

            for line in data.splitlines():
                # line = line.decode('utf-8', errors='ignore')

                if len(line.strip()) < 1:
                    continue

                if counter <= 2:
                    list.append(line)
                    counter = counter + 1
                else:
                    list.append(line)

            # fix column names
            row1 = list[0].replace('/', '').replace('\n', '').replace('\r\n', '').split(',')
            row1.append('')
            row2 = list[1].replace('\n', '').split(',')
            column = [a + ' ' + b for b, a in zip(row1, row2)]

            # create dataframe and rename column names to match db columns

            df = pd.DataFrame((row.split(',') for row in list[2:]), columns=column)

            df = df.rename(columns=lambda x: x.strip())
            df = df.rename(columns={'Units 1-unit': 'Units_1', 'Units 2-units': 'Units_2', 'Units 3-4 units': 'Units_3to4',
                         'Units 5+ units': 'Units_5plus', 'Date Survey': 'Date_Survey'})

            drop_columns = ['Bldgs', 'Value']
            value_column_rename = ''

            # Rename ambiguous columns. Ex: Value -> 1Unit_Value
            for i, col in enumerate(df.columns.values):
                if 'Unit' in col and 'rep' not in col:
                    drop_columns.append(col)
                # elif col in ['MONCOV', 'Code_CSA', 'Date_Survey', 'Name_CBSA']:
                #     drop_columns.append(col)
                elif col == 'Value' and 'rep' not in value_column_rename:
                    continue
                else:
                    df.columns.values[i] = df.columns.values[i].replace(' ', '_')

                value_column_rename = col

            # drop useless columns
            df = df.drop(columns=drop_columns)

            df = df.rename(columns={'Code_CBSA':'Geo_ID'})
            df['Year'] = year
            df = df[~df.index.duplicated(keep='first')]

            df['Geo_ID'] = df['Geo_ID'].apply(lambda x: x.replace('31100','31080').replace('19380','19430').replace('42060','42200')
                                              .replace('39140','39150'))

            if final_df.empty:
                final_df = df
            else:
                final_df = final_df.append(df)


final_df['Date'] = final_df['Date_Survey'].apply(lambda x: str(x)[4:] + '/01/') + final_df['Year'].apply(lambda x: str(x))
final_df = final_df.drop(columns=['MONCOV', 'Code_CSA', 'Date_Survey','Year'])
final_df = final_df.rename(columns={'Units_1-unit_rep':'Permit_1unit',
                                    'Units_2-units_rep':'Permit_2unit',
                                    'Units_3-4_units_rep':'Permit_3to4unit',
                                    'Units_5+_units_rep':'Permit_5plus',
                                    'Name_CBSA':'Geo_Name'})


sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_MarketTrends_BuildingPermits(final_df)




