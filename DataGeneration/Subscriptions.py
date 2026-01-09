import pandas as pd
import random
from datetime import timedelta
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
print("Connected to DB")

# Зчитуємо ВСІХ абонентів
subscribers_df = pd.read_sql("""
    SELECT SubscriberID, RegistrationDate
    FROM Subscribers
""", engine)

# ВИБИРАЄМО тільки частину (65 000)
ACTIVE_SUBS_COUNT = 65000

active_subscribers = subscribers_df.sample(
    n=ACTIVE_SUBS_COUNT,
    random_state=42
)

# Зчитуємо пакети
groups_df = pd.read_sql("""
    SELECT GroupID, GroupName
    FROM ChannelGroups
""", engine)

print(f"Total subscribers: {len(subscribers_df)}")
print(f"Active subscriptions to generate: {len(active_subscribers)}")
print(groups_df)

package_weights = {
    "Basic": 0.30,
    "Light": 0.25,
    "Standard": 0.20,
    "Premium": 0.15,
    "Ultimate": 0.10
}

group_map = dict(zip(groups_df.GroupName, groups_df.GroupID))

# Генеруємо Subscriptions
subscriptions_data = []

for _, row in active_subscribers.iterrows():
    package = random.choices(
        list(package_weights.keys()),
        weights=list(package_weights.values()),
        k=1
    )[0]

    start_date = row["RegistrationDate"] + timedelta(days=random.randint(0, 30))

    subscriptions_data.append({
        "SubscriberID": row["SubscriberID"],
        "GroupID": group_map[package],
        "StartDate": start_date,
        "EndDate": None,
        "IsActive": 1
    })

df_subs = pd.DataFrame(subscriptions_data)
print(f"Generated subscriptions: {len(df_subs)}")

# Запис у БД
df_subs.to_sql(
    name="Subscriptions",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=500
)

print("Subscriptions inserted")
