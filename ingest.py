import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

try:
    # Step 1: Fetch data from the public COVID API
    api_url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(api_url)
    response.raise_for_status()  # Raises exception for non-2xx status codes
    data = response.json()

    # Step 2: Convert JSON to DataFrame and select necessary columns
    df = pd.json_normalize(data)
    df = df[['country', 'cases', 'deaths', 'recovered', 'active', 'updated']]
    df['updated'] = pd.to_datetime(df['updated'], unit='ms')

    # Prepare rows as list of tuples
    rows = [
        (
            row['country'],
            int(row['cases']),
            int(row['deaths']),
            int(row['recovered']),
            int(row['active']),
            row['updated'].to_pydatetime()
        )
        for _, row in df.iterrows()
    ]

    # Step 3: Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

    cursor = conn.cursor()

    # Step 4: Bulk insert into Snowflake
    insert_query = """
        INSERT INTO stats (country, cases, deaths, recovered, active, updated)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, rows)

    print(f"✅ {len(rows)} rows successfully ingested into Snowflake table covid.raw.stats at {datetime.utcnow()} UTC")

except requests.exceptions.RequestException as e:
    print(f"❌ API Error: {e}")
except snowflake.connector.errors.Error as e:
    print(f"❌ Snowflake Error: {e}")
except Exception as e:
    print(f"❌ General Error: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
