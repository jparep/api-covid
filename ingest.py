import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Step 1: Get data from public API
response = requests.get("https://disease.sh/v3/covid-19/countries")
data = response.json()

# Step 2: Normalize JSON into DataFrame
df = pd.json_normalize(data)

# Optional: Filter/rename columns
df = df[['country', 'cases', 'deaths', 'recovered', 'active', 'updated']]

# Step 3: Connect to Snowflake using env vars
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role=os.getenv('SNOWFLAKE_ROLE')
)

# Step 4: Create a cursor
cur = conn.cursor()

# Step 5: Insert data into Snowflake table
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO covid_stats (country, cases, deaths, recovered, active, updated)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row['country'], row['cases'], row['deaths'],
        row['recovered'], row['active'], row['updated']
    ))

# Cleanup
cur.close()
conn.close()

print("✅ Data successfully ingested into Snowflake.")
