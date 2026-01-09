import pandas as pd
from sqlalchemy import create_engine
import urllib

SERVER_NAME = 'DESKTOP-A95Q7PS'
DATABASE_NAME = 'CableTV_OLTP'

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

channel_groups = [
    {"GroupName": "Basic",    "MonthlyPrice": 100.00, "ParentGroupID": None},
    {"GroupName": "Light",    "MonthlyPrice": 160.00, "ParentGroupID": 1},
    {"GroupName": "Standard", "MonthlyPrice": 220.00, "ParentGroupID": 2},
    {"GroupName": "Premium",  "MonthlyPrice": 300.00, "ParentGroupID": 3},
    {"GroupName": "Ultimate", "MonthlyPrice": 420.00, "ParentGroupID": 4}
]

df_groups = pd.DataFrame(channel_groups)

df_groups.to_sql(
    name="ChannelGroups",
    con=engine,
    if_exists="append",
    index=False
)

print("ChannelGroups (5 basic packages) inserted")
