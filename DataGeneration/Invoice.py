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
print(" Connected to DB")

subscriptions_df = pd.read_sql("""
    SELECT 
        s.SubscriptionID,
        s.SubscriberID,
        s.StartDate,
        g.MonthlyPrice
    FROM Subscriptions s
    JOIN ChannelGroups g ON s.GroupID = g.GroupID
    WHERE s.IsActive = 1
""", engine)

print(f"Active subscriptions: {len(subscriptions_df)}")
orders_df = pd.read_sql("""
    SELECT
        SubscriberID,
        OrderDate,
        Amount
    FROM Orders
""", engine)

orders_df["OrderMonth"] = orders_df["OrderDate"].dt.to_period("M")

orders_monthly = (
    orders_df
    .groupby(["SubscriberID", "OrderMonth"])["Amount"]
    .sum()
    .reset_index()
)
END_DATE = pd.Timestamp.today().to_period("M")
START_DATE = END_DATE - 59

all_months = pd.period_range(
    start=START_DATE,
    end=END_DATE,
    freq="M"
)
duration_choices = [
    (60, 0.30),
    (36, 0.30),
    (24, 0.20),
    (18, 0.20)
]
invoices_data = []

for _, sub in subscriptions_df.iterrows():
    active_months = random.choices(
        [d[0] for d in duration_choices],
        weights=[d[1] for d in duration_choices],
        k=1
    )[0]

    start_month = pd.to_datetime(sub["StartDate"]).to_period("M")
    valid_months = all_months[all_months >= start_month][:active_months]

    for month in valid_months:
        movie_sum = orders_monthly[
            (orders_monthly["SubscriberID"] == sub["SubscriberID"]) &
            (orders_monthly["OrderMonth"] == month)
        ]["Amount"].sum()

        total = sub["MonthlyPrice"] + movie_sum

        invoices_data.append({
            "SubscriberID": sub["SubscriberID"],
            "InvoiceDate": month.to_timestamp(),
            "TotalAmount": round(total, 2),
            "IsPaid": 0,
            "DueDate": (month + 1).to_timestamp()
        })
df_invoices = pd.DataFrame(invoices_data)
print(f"Generated invoices: {len(df_invoices)}")

df_invoices.to_sql(
    name="Invoices",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=1000
)

print("Invoices inserted")

