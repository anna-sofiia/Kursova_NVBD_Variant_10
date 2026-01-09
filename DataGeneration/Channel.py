import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine, text
import urllib

fake = Faker()

SERVER_NAME = 'DESKTOP-A95Q7PS'   # ← перевір, щоб співпадало
DATABASE_NAME = 'CableTV_OLTP'

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
print("Connected to DB")

groups_df = pd.read_sql(
    "SELECT GroupID, GroupName FROM ChannelGroups",
    engine
)

print(groups_df)
NUM_CHANNELS = 3000   # можеш змінити на 2000 або 5000

genres = [
    "News", "Sports", "Kids", "Movies", "Entertainment",
    "Music", "Documentary", "Lifestyle", "Business", "Culture"
]

# Реалістичний розподіл каналів по пакетах
package_weights = {
    "Basic": 0.30,
    "Light": 0.25,
    "Standard": 0.20,
    "Premium": 0.15,
    "Ultimate": 0.10
}
# мапа: назва пакета → GroupID
group_map = dict(zip(groups_df.GroupName, groups_df.GroupID))

channels_data = []

for _ in range(NUM_CHANNELS):
    base_package = random.choices(
        list(package_weights.keys()),
        weights=list(package_weights.values()),
        k=1
    )[0]

    channels_data.append({
        "ChannelName": f"{fake.word().title()} TV",
        "Genre": random.choice(genres),
        "GroupID": group_map[base_package]
    })

df_channels = pd.DataFrame(channels_data)
print(f" Generated {len(df_channels)} channels")
df_channels.to_sql(
    name="Channels",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=500
)

print("Channels inserted successfully")
