from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import requests
import os

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether,solana,tether-gold&vs_currencies=usd&include_24hr_change=true"
    response = requests.get(url)
    data = response.json()
    
    # Ubah format JSON ke DataFrame
    rows = []
    for coin, info in data.items():
        rows.append({
            "coin": coin,
            "price_usd": info["usd"],
            "change_24h": info["usd_24h_change"],
            "timestamp": pd.Timestamp.now()
        })
    return pd.DataFrame(rows)

def load_to_bigquery(df):
    
    client = bigquery.Client()
    table_id = "bigquery-pipeline-489008.crypto_data.daily_prices"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Menambah data baru di bawahnya
        schema=[
            bigquery.SchemaField("coin", "STRING"),
            bigquery.SchemaField("price_usd", "FLOAT"),
            bigquery.SchemaField("change_24h", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Tunggu sampai selesai
    print(f"Berhasil mengunggah {len(df)} baris ke {table_id}")

if __name__ == "__main__":
    df = fetch_crypto_data()
    load_to_bigquery(df)