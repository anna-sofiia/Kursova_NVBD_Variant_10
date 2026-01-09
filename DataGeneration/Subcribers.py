import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib

# ---------- DB CONNECTION ----------
SERVER_NAME = 'DESKTOP-A95Q7PS'
DATABASE_NAME = 'CableTV_OLTP'

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

print("Connected to database")
NUM_SUBSCRIBERS = 100_000
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2024, 12, 31)

fake = Faker('uk_UA')
subscribers_data = []

for i in range(NUM_SUBSCRIBERS):
    registration_date = fake.date_between(
        start_date=START_DATE,
        end_date=END_DATE
    )

    subscribers_data.append({
        "FullName": fake.name(),
        "Email": f"user{i}_{fake.email()}",
        "Address": fake.city(),
        "RegistrationDate": registration_date,
        "IsActive": random.random() < 0.85
    })

df_subscribers = pd.DataFrame(subscribers_data)
print(f"Generated {len(df_subscribers)} subscribers")
BATCH_SIZE = 5000

df_subscribers.to_sql(
    name="Subscribers",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=200
)

print("Subscribers successfully inserted")
