from data import arcgisdata
from data import buildingpermits
from data import redfinparser
import pandas as pd


year_to_use = 2020
# TEST = True
msaid = '19100'

demographicdata = pd.DataFrame()
demographicdata = arcgisdata.get_arcgisdata(msaid)
redfinparser.get_macrotrends(msaid=msaid,demographic_df=demographicdata)


bp = buildingpermits.BuildingPermits().get_building_permits(msaid)






