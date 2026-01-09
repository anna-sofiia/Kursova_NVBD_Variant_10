import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine
import urllib

fake = Faker()

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

NUM_MOVIES = 100_000
BATCH_SIZE = 5000

genres = [
    "Action", "Drama", "Comedy", "Thriller", "Horror",
    "Sci-Fi", "Fantasy", "Animation", "Romance", "Documentary"
]

movies_batch = []

for i in range(1, NUM_MOVIES + 1):
    movies_batch.append({
        "Title": fake.catch_phrase(),
        "Genre": random.choice(genres),
        "ReleaseYear": random.randint(1980, 2025),
        "RentalPrice": random.randint(40, 120)  # 🔹 ПРОСТО РАНДОМ
    })

    if i % BATCH_SIZE == 0:
        pd.DataFrame(movies_batch).to_sql(
            name="Movies",
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted {i} movies")
        movies_batch = []

print("Movies generation completed")
