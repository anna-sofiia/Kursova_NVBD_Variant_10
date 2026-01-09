import pandas as pd
import random
from datetime import datetime, timedelta
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
subscribers = pd.read_sql(
    "SELECT SubscriberID FROM Subscribers",
    engine
)["SubscriberID"].tolist()

movies = pd.read_sql(
    "SELECT MovieID, RentalPrice FROM Movies",
    engine
)

movie_ids = movies["MovieID"].tolist()
movie_price_map = dict(zip(movies.MovieID, movies.RentalPrice))

print(f"Subscribers: {len(subscribers)}")
print(f"Movies: {len(movie_ids)}")
NUM_ORDERS = 1_000_000
BATCH_SIZE = 10_000

start_date = datetime.now() - timedelta(days=365 * 5)
end_date = datetime.now()
orders_batch = []

for i in range(1, NUM_ORDERS + 1):
    subscriber_id = random.choice(subscribers)
    movie_id = random.choice(movie_ids)

    order_date = start_date + (end_date - start_date) * random.random()
    amount = movie_price_map[movie_id]

    orders_batch.append({
        "SubscriberID": subscriber_id,
        "MovieID": movie_id,
        "OrderDate": order_date,
        "Amount": amount
    })

    if i % BATCH_SIZE == 0:
        pd.DataFrame(orders_batch).to_sql(
            name="Orders",
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted {i} orders")
        orders_batch = []

print("Orders generation completed")
