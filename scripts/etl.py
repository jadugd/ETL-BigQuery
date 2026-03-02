from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import requests
import json
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
    # Mengambil kredensial dari Environment Variable (untuk keamanan di GitHub)
    info = json.loads(os.environ['GCP_SA_KEY'])
    credentials = service_account.Credentials.from_service_account_info(info)
    
    client = bigquery.Client(credentials=credentials, project=info['project_id'])
    table_id = f"{info['project_id']}.crypto_data.daily_prices"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Menambah data baru di bawahnya
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Tunggu sampai selesai
    print(f"Berhasil mengunggah {len(df)} baris ke {table_id}")

if __name__ == "__main__":
    df = fetch_crypto_data()
    print(df)