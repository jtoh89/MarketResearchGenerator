from blog_data import getarcgisdata
from blog_data import get_annual_buildingpermits
from blog_data import redfinparser
import pandas as pd


year_to_use = 2020
# TEST = True
msaid = '26420'

# demographicdata = pd.DataFrame()
# demographicdata = getarcgisdata.get_arcgisdata(msaid)
# redfinparser.get_redfin_marketdata(msaid=msaid, demographic_df=demographicdata)


bp = get_annual_buildingpermits.BuildingPermits().get_building_permits(msaid)






